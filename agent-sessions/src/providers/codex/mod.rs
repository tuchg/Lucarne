use std::borrow::Cow;
use std::io::BufRead;

use serde::{Deserialize, Deserializer as SerdeDeserializer};
use serde_json::value::RawValue;
use tracing::trace;

use crate::util::{box_str, cow_to_box, opt_cow_to_box};
use crate::{Error, ParseSelection, Result};
use smol_str::SmolStr;

mod types;
pub(crate) use types::*;

#[cfg(feature = "discovery")]
mod discovery;
#[cfg(feature = "agent_session")]
mod event;

pub struct Codex;

impl Codex {
    pub(crate) fn probe_session_meta<R>(mut reader: R) -> Result<Option<SessionMeta>>
    where
        R: BufRead,
    {
        let mut line = Vec::new();

        loop {
            if !read_next_nonempty_line(&mut reader, &mut line)? {
                return Ok(None);
            }

            let envelope: ProbeEnvelope<'_> = serde_json::from_slice(&line)?;
            if envelope.kind != "session_meta" {
                continue;
            }

            let payload: SessionMetaPayload<'_> = serde_json::from_str(envelope.payload.get())?;
            return Ok(Some(SessionMeta {
                session_id: opt_cow_box_str(payload.id.or(payload.session_id)),
                cwd: opt_cow_box_str(payload.cwd),
                originator: opt_cow_box_str(payload.originator),
                model: opt_cow_box_str(payload.model),
                cli_version: opt_cow_box_str(payload.cli_version),
                timestamp: opt_cow_box_str(payload.timestamp),
            }));
        }
    }

    /// Probe the Codex rollout for both `session_meta` and a usable title.
    ///
    /// The title is taken from the first `response_item` whose payload is a
    /// `message` with `role == "user"` and whose first text block is not an
    /// instruction preamble (Codex injects an `AGENTS.md` block as the first
    /// user-role message before the real user prompt). Reading stops as soon
    /// as a title is found, so this remains cheap even for large rollouts.
    pub(crate) fn probe_session_meta_with_title<R>(
        mut reader: R,
    ) -> Result<Option<(SessionMeta, Option<SmolStr>)>>
    where
        R: BufRead,
    {
        let mut line = Vec::new();
        let mut session_meta: Option<SessionMeta> = None;
        let mut session_started_at: Option<SmolStr> = None;
        let mut title_candidate: Option<SmolStr> = None;
        let mut fallback_title: Option<SmolStr> = None;

        loop {
            if !read_next_nonempty_line(&mut reader, &mut line)? {
                return Ok(session_meta.map(|meta| (meta, title_candidate.or(fallback_title))));
            }
            // Once we have already located the session_meta, treat the rest of
            // the file as best-effort: malformed lines past that point must not
            // wipe out the entry (`history::tests::metadata_snapshot_pages_without_full_entry_parse`).
            let envelope: ProbeEnvelope<'_> = match serde_json::from_slice(&line) {
                Ok(envelope) => envelope,
                Err(err) => {
                    if session_meta.is_some() {
                        continue;
                    }
                    return Err(err.into());
                }
            };
            match envelope.kind {
                "session_meta" if session_meta.is_none() => {
                    let payload: SessionMetaPayload<'_> =
                        serde_json::from_str(envelope.payload.get())?;
                    let timestamp = payload
                        .timestamp
                        .or_else(|| envelope.timestamp.map(Cow::Borrowed));
                    session_started_at = timestamp
                        .as_ref()
                        .map(|value| SmolStr::from(value.as_ref()));
                    session_meta = Some(SessionMeta {
                        session_id: opt_cow_box_str(payload.id.or(payload.session_id)),
                        cwd: opt_cow_box_str(payload.cwd),
                        originator: opt_cow_box_str(payload.originator),
                        model: opt_cow_box_str(payload.model),
                        cli_version: opt_cow_box_str(payload.cli_version),
                        timestamp: opt_cow_box_str(timestamp),
                    });
                }
                "response_item" if session_meta.is_some() => {
                    let before_session_start = envelope
                        .timestamp
                        .zip(session_started_at.as_deref())
                        .is_some_and(|(timestamp, session_started_at)| {
                            timestamp < session_started_at
                        });
                    let payload: TitleProbeResponseItem<'_> =
                        match serde_json::from_str(envelope.payload.get()) {
                            Ok(payload) => payload,
                            Err(_) => continue,
                        };
                    if payload.item_type != "message" || payload.role != Some("user") {
                        continue;
                    }
                    let Some(text) = payload.content.iter().find_map(|block| match block.kind {
                        "input_text" | "output_text" | "summary_text" => block.text.as_deref(),
                        _ => None,
                    }) else {
                        continue;
                    };
                    let is_delegated_fork = is_codex_delegated_fork_user_text(text);
                    let title = first_line_snippet(text, 80);
                    if title.is_empty() {
                        continue;
                    }
                    if is_codex_instruction_preamble(text) {
                        if fallback_title.is_none() {
                            fallback_title = Some(title.into());
                        }
                        continue;
                    }
                    if is_delegated_fork {
                        return Ok(session_meta.map(|meta| (meta, Some(title.into()))));
                    }
                    if title_candidate.is_none() {
                        title_candidate = Some(title.into());
                    }
                    if !before_session_start {
                        return Ok(session_meta.map(|meta| (meta, title_candidate)));
                    }
                }
                _ => {}
            }
        }
    }

    pub fn probe_date_range_overlap<R>(
        mut reader: R,
        start_date: &str,
        end_date: &str,
    ) -> Result<bool>
    where
        R: BufRead,
    {
        let mut line = Vec::new();

        loop {
            if !read_next_nonempty_line(&mut reader, &mut line)? {
                return Ok(false);
            }

            let envelope: TimestampProbeEnvelope<'_> = serde_json::from_slice(&line)?;
            let Some(timestamp) = envelope.timestamp else {
                continue;
            };
            let Some(date) = timestamp.get(..10) else {
                continue;
            };

            if date >= start_date && date < end_date {
                return Ok(true);
            }
        }
    }
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

#[derive(Deserialize)]
struct TitleProbeResponseItem<'a> {
    #[serde(rename = "type")]
    item_type: &'a str,
    #[serde(default)]
    role: Option<&'a str>,
    #[serde(default, borrow)]
    content: Vec<TitleProbeContentBlock<'a>>,
}

#[derive(Deserialize)]
struct TitleProbeContentBlock<'a> {
    #[serde(rename = "type")]
    kind: &'a str,
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

fn is_codex_instruction_preamble(text: &str) -> bool {
    let trimmed = text.trim_start();
    let first = trimmed.lines().next().unwrap_or("").trim();
    first.starts_with("# AGENTS.md instructions")
        || trimmed.starts_with("<INSTRUCTIONS>")
        || trimmed.contains("\n<INSTRUCTIONS>")
        || trimmed.starts_with("<permissions instructions>")
        || trimmed.starts_with("<user_instructions>")
}

fn is_codex_delegated_fork_user_text(text: &str) -> bool {
    text.trim_start().starts_with(
        "Task: You are a delegated subagent running from a fork of the parent session.",
    )
}

impl Codex {
    pub(crate) fn name() -> &'static str {
        "codex"
    }
}

fn parse_codex_reader<R>(mut reader: R, selection: ParseSelection) -> Result<(Version, Body)>
where
    R: BufRead,
{
    if selection.is_meta_only() {
        return parse_codex_meta_only(reader);
    }

    let mut entries = Vec::new();
    let mut version = Version::V1;
    let mut line = Vec::new();

    while read_next_nonempty_line(&mut reader, &mut line)? {
        let envelope: ProbeEnvelope<'_> = serde_json::from_slice(&line)?;
        match envelope.kind {
            "session_meta" => {
                if selection.includes_meta() {
                    let payload: SessionMetaPayload<'_> =
                        serde_json::from_str(envelope.payload.get())?;
                    entries.push(Entry::SessionMeta(SessionMeta {
                        session_id: opt_cow_box_str(payload.id.or(payload.session_id)),
                        cwd: opt_cow_box_str(payload.cwd),
                        originator: opt_cow_box_str(payload.originator),
                        model: opt_cow_box_str(payload.model),
                        cli_version: opt_cow_box_str(payload.cli_version),
                        timestamp: opt_cow_box_str(payload.timestamp),
                    }));
                }
            }
            "turn_context" => {
                if selection.includes_state_records() {
                    let payload: TurnContextPayload<'_> =
                        serde_json::from_str(envelope.payload.get())?;
                    entries.push(Entry::TurnContext(TurnContext {
                        turn_id: opt_cow_box_str(payload.turn_id),
                        cwd: opt_cow_box_str(payload.cwd),
                        current_date: opt_cow_box_str(payload.current_date),
                        timezone: opt_cow_box_str(payload.timezone),
                        model: opt_cow_box_str(payload.model),
                    }));
                }
            }
            "response_item" => {
                let probe: ResponseItemKindProbe<'_> =
                    serde_json::from_str(envelope.payload.get())?;
                if matches!(probe.item_type, "message") && matches!(probe.role, Some("developer")) {
                    version = Version::V2;
                }
                if !selection.includes_codex_response_item(probe.item_type) {
                    continue;
                }
                let entry = parse_codex_response_item_entry(
                    probe.item_type,
                    envelope.payload,
                    envelope.timestamp,
                )?;
                entries.push(entry);
            }
            "event_msg" => {
                let probe: EventMsgKindProbe<'_> = serde_json::from_str(envelope.payload.get())?;
                if matches!(probe.kind, "task_started" | "task_complete") {
                    version = Version::V2;
                }
                if !selection.includes_codex_event_kind(probe.kind) {
                    continue;
                }
                let mut payload: EventMsgPayload<'_> =
                    serde_json::from_str(envelope.payload.get())?;
                payload.raw_json = Some(envelope.payload);
                let data = map_event_msg_data(&payload);
                entries.push(Entry::EventMsg(EventMsg {
                    kind: box_str(payload.kind),
                    turn_id: opt_box_str(payload.turn_id),
                    last_agent_message: opt_cow_box_str(payload.last_agent_message),
                    timestamp: opt_box_str(envelope.timestamp),
                    data,
                }));
            }
            "compacted" => {
                if selection.includes_state_records() {
                    let payload: CompactedPayload<'_> =
                        serde_json::from_str(envelope.payload.get())?;
                    entries.push(Entry::Compacted(Compacted {
                        message: opt_cow_box_str(payload.message),
                        timestamp: opt_box_str(envelope.timestamp),
                    }));
                }
            }
            other => {
                if selection.includes_raw_unknown() {
                    entries.push(Entry::Unknown(UnknownRecord {
                        kind: box_str(other),
                        raw_json: json_to_box(envelope.payload),
                        timestamp: opt_box_str(envelope.timestamp),
                    }));
                }
            }
        }
    }

    if entries.is_empty() && selection.is_full() {
        return Err(Error::Detection {
            agent: Codex::name(),
        });
    }

    trace!(
        target: "agent_sessions::parse",
        agent = Codex::name(),
        version = ?version,
        entries = entries.len(),
        "parsed Codex bundle"
    );
    Ok((
        version,
        Body {
            entries: entries.into_boxed_slice(),
        },
    ))
}

fn parse_codex_meta_only<R>(reader: R) -> Result<(Version, Body)>
where
    R: BufRead,
{
    let Some(meta) = Codex::probe_session_meta(reader)? else {
        return Err(Error::Detection {
            agent: Codex::name(),
        });
    };
    Ok((
        Version::V1,
        Body {
            entries: vec![Entry::SessionMeta(meta)].into_boxed_slice(),
        },
    ))
}

fn parse_codex_response_item_entry<'a>(
    item_type: &'a str,
    raw_payload: &'a RawValue,
    timestamp: Option<&'a str>,
) -> Result<Entry> {
    Ok(match item_type {
        "message" => {
            let payload: ResponseMessagePayload<'_> = serde_json::from_str(raw_payload.get())?;
            Entry::Message(Message {
                role: map_role(payload.role),
                model: opt_box_str(payload.model),
                phase: opt_box_str(payload.phase),
                blocks: payload
                    .content
                    .into_iter()
                    .map(map_codex_content_block)
                    .collect::<Vec<_>>()
                    .into_boxed_slice(),
                timestamp: opt_box_str(timestamp),
            })
        }
        "function_call" => {
            let payload: ResponseFunctionCallPayload<'_> = serde_json::from_str(raw_payload.get())?;
            Entry::FunctionCall(FunctionCall {
                id: opt_box_str(payload.id),
                call_id: opt_box_str(payload.call_id),
                name: box_str(payload.name.unwrap_or("unknown")),
                arguments_json: payload.arguments,
                timestamp: opt_box_str(timestamp),
            })
        }
        "function_call_output" => {
            let payload: ResponseFunctionCallOutputPayload<'_> =
                serde_json::from_str(raw_payload.get())?;
            let call_id = payload.call_id.ok_or(Error::InvalidStructure {
                agent: Codex::name(),
                details: "function_call_output is missing call_id",
            })?;
            Entry::FunctionCallOutput(FunctionCallOutput {
                call_id: box_str(call_id),
                output: payload.output.unwrap_or_else(|| box_str("")),
                timestamp: opt_box_str(timestamp),
            })
        }
        "custom_tool_call" => {
            let payload: ResponseCustomToolCallPayload<'_> =
                serde_json::from_str(raw_payload.get())?;
            Entry::CustomToolCall(CustomToolCall {
                call_id: opt_box_str(payload.call_id),
                name: box_str(payload.name.unwrap_or("unknown")),
                input: payload.input,
                status: opt_box_str(payload.status),
                timestamp: opt_box_str(timestamp),
            })
        }
        "custom_tool_call_output" => {
            let payload: ResponseCustomToolCallOutputPayload<'_> =
                serde_json::from_str(raw_payload.get())?;
            Entry::CustomToolCallOutput(CustomToolCallOutput {
                call_id: opt_box_str(payload.call_id),
                output: payload.output.unwrap_or_else(|| box_str("")),
                timestamp: opt_box_str(timestamp),
            })
        }
        "web_search_call" => {
            let payload: ResponseWebSearchCallPayload<'_> =
                serde_json::from_str(raw_payload.get())?;
            Entry::WebSearchCall(WebSearchCall {
                status: opt_box_str(payload.status),
                action_type: payload
                    .action
                    .as_ref()
                    .and_then(|action| opt_box_str(action.kind)),
                query: payload
                    .action
                    .as_ref()
                    .and_then(|action| opt_cow_box_str(action.query.clone())),
                queries: payload
                    .action
                    .map(|action| {
                        action
                            .queries
                            .into_iter()
                            .map(cow_to_box)
                            .collect::<Vec<_>>()
                            .into_boxed_slice()
                    })
                    .unwrap_or_else(|| Vec::new().into_boxed_slice()),
                timestamp: opt_box_str(timestamp),
            })
        }
        "ghost_snapshot" => {
            let payload: ResponseGhostSnapshotPayload<'_> =
                serde_json::from_str(raw_payload.get())?;
            Entry::GhostSnapshot(GhostSnapshot {
                commit_id: payload
                    .ghost_commit
                    .as_ref()
                    .and_then(|commit| opt_box_str(commit.id)),
                parent_id: payload
                    .ghost_commit
                    .as_ref()
                    .and_then(|commit| opt_box_str(commit.parent)),
                preexisting_untracked_files: payload
                    .ghost_commit
                    .as_ref()
                    .map(|commit| {
                        commit
                            .preexisting_untracked_files
                            .iter()
                            .cloned()
                            .map(cow_to_box)
                            .collect::<Vec<_>>()
                            .into_boxed_slice()
                    })
                    .unwrap_or_else(|| Vec::new().into_boxed_slice()),
                preexisting_untracked_dirs: payload
                    .ghost_commit
                    .as_ref()
                    .map(|commit| {
                        commit
                            .preexisting_untracked_dirs
                            .iter()
                            .cloned()
                            .map(cow_to_box)
                            .collect::<Vec<_>>()
                            .into_boxed_slice()
                    })
                    .unwrap_or_else(|| Vec::new().into_boxed_slice()),
                timestamp: opt_box_str(timestamp),
            })
        }
        "reasoning" => {
            let payload: ResponseReasoningPayload<'_> = serde_json::from_str(raw_payload.get())?;
            Entry::Reasoning(Reasoning {
                summary: payload
                    .summary
                    .into_iter()
                    .filter_map(|item| item.text.map(cow_to_box))
                    .collect::<Vec<_>>()
                    .into_boxed_slice(),
                timestamp: opt_box_str(timestamp),
            })
        }
        _ => Entry::Unknown(UnknownRecord {
            kind: box_str(item_type),
            raw_json: json_to_box(raw_payload),
            timestamp: opt_box_str(timestamp),
        }),
    })
}

impl ParseSelection {
    fn includes_codex_response_item(self, item_type: &str) -> bool {
        match item_type {
            "message" | "reasoning" => self.includes_messages(),
            "function_call"
            | "function_call_output"
            | "custom_tool_call"
            | "custom_tool_call_output"
            | "web_search_call" => self.includes_operations(),
            "ghost_snapshot" => self.includes_snapshots(),
            _ => self.includes_raw_unknown(),
        }
    }

    fn includes_codex_event_kind(self, kind: &str) -> bool {
        match kind {
            "agent_message"
            | "agent_reasoning"
            | "user_message"
            | "imageGeneration"
            | "image_generation"
            | "image_generation_result"
            | "image_generation_end" => self.includes_messages(),
            "token_count" => self.includes_usage(),
            "exec_command_end"
            | "patch_apply_end"
            | "web_search_end"
            | "mcp_tool_call_end"
            | "dynamic_tool_call_request"
            | "dynamic_tool_call_response"
            | "collab_agent_spawn_end"
            | "collab_close_end"
            | "collab_agent_interaction_end"
            | "collab_waiting_end" => self.includes_operations(),
            "task_started"
            | "task_complete"
            | "turn_aborted"
            | "context_compacted"
            | "thread_rolled_back"
            | "error"
            | "entered_review_mode"
            | "exited_review_mode" => self.includes_state_records(),
            _ => self.includes_raw_unknown(),
        }
    }
}

fn map_role(raw: Option<&str>) -> Role {
    match raw {
        Some("user") => Role::User,
        Some("assistant") => Role::Assistant,
        Some("system") => Role::System,
        Some("developer") => Role::Developer,
        Some(other) => Role::Other(box_str(other)),
        None => Role::Other(box_str("unknown")),
    }
}

fn map_codex_content_block(block: ResponseContent<'_>) -> ContentBlock {
    match block.kind {
        "input_text" | "output_text" | "summary_text" => ContentBlock::Text(TextBlock {
            text: block.text.map(cow_to_box).unwrap_or_else(|| box_str("")),
        }),
        "input_image" => ContentBlock::Image(ImageBlock {
            image_url: block
                .image_url
                .map(cow_to_box)
                .unwrap_or_else(|| box_str("")),
        }),
        other => ContentBlock::Raw(RawBlock {
            kind: box_str(other),
            raw_json: block.raw.map(json_to_box).unwrap_or_else(|| box_str("{}")),
        }),
    }
}

fn opt_cow_box_str(value: Option<Cow<'_, str>>) -> Option<SmolStr> {
    opt_cow_to_box(value)
}

fn opt_box_str(value: Option<&str>) -> Option<SmolStr> {
    value.map(box_str)
}

fn json_to_box(raw: &RawValue) -> SmolStr {
    box_str(raw.get())
}

fn opt_raw_json_box(raw: Option<&RawValue>) -> Option<SmolStr> {
    raw.map(json_to_box)
}

fn opt_raw_string_box(raw: Option<&RawValue>) -> Option<SmolStr> {
    raw.and_then(|raw| {
        serde_json::from_str::<Cow<'_, str>>(raw.get())
            .ok()
            .map(cow_to_box)
    })
}

fn opt_status_string(raw: Option<&RawValue>) -> Option<SmolStr> {
    raw.and_then(|raw| {
        match raw
            .get()
            .as_bytes()
            .iter()
            .copied()
            .find(|byte| !byte.is_ascii_whitespace())
        {
            Some(b'"') => serde_json::from_str::<Cow<'_, str>>(raw.get())
                .ok()
                .map(cow_to_box),
            _ => None,
        }
    })
}

fn map_event_msg_data(payload: &EventMsgPayload<'_>) -> EventMsgData {
    match payload.kind {
        "task_started" => EventMsgData::TaskStarted(TaskStartedEventMsg {
            model_context_window: payload.model_context_window,
            collaboration_mode_kind: opt_box_str(payload.collaboration_mode_kind),
        }),
        "task_complete" => EventMsgData::TaskComplete(TaskCompleteEventMsg {
            completed_at: payload.completed_at,
            duration_ms: payload.duration_ms,
            time_to_first_token_ms: payload.time_to_first_token_ms,
        }),
        "agent_message" => EventMsgData::AgentMessage(AgentMessageEventMsg {
            message: opt_cow_box_str(payload.message.clone()),
            phase: opt_box_str(payload.phase),
        }),
        "agent_reasoning" => EventMsgData::AgentReasoning(AgentReasoningEventMsg {
            text: opt_cow_box_str(payload.text.clone()),
        }),
        "imageGeneration"
        | "image_generation"
        | "image_generation_result"
        | "image_generation_end" => EventMsgData::ImageGeneration(ImageGenerationEventMsg {
            id: opt_box_str(payload.id.or(payload.call_id)),
            status: opt_raw_string_box(payload.status),
            revised_prompt: opt_cow_box_str(payload.revised_prompt.clone()),
            result_base64: opt_raw_string_box(payload.result),
        }),
        "user_message" => EventMsgData::UserMessage(UserMessageEventMsg {
            message: opt_cow_box_str(payload.message.clone()),
            images_json: opt_raw_json_box(payload.images),
            local_images_json: opt_raw_json_box(payload.local_images),
            text_elements_json: opt_raw_json_box(payload.text_elements),
        }),
        "token_count" => EventMsgData::TokenCount(TokenCountEventMsg {
            info_json: opt_raw_json_box(payload.info),
        }),
        "exec_command_end" => EventMsgData::ExecCommandEnd(ExecCommandEndEventMsg {
            call_id: opt_box_str(payload.call_id),
            command_json: opt_raw_json_box(payload.command),
            parsed_cmd_json: opt_raw_json_box(payload.parsed_cmd),
            stdout: opt_cow_box_str(payload.stdout.clone()),
            stderr: opt_cow_box_str(payload.stderr.clone()),
            aggregated_output: opt_cow_box_str(payload.aggregated_output.clone()),
            exit_code: payload.exit_code,
            duration_json: opt_raw_json_box(payload.duration),
            formatted_output: opt_cow_box_str(payload.formatted_output.clone()),
            status: opt_status_string(payload.status),
        }),
        "patch_apply_end" => EventMsgData::PatchApplyEnd(PatchApplyEndEventMsg {
            call_id: opt_box_str(payload.call_id),
            stdout: opt_cow_box_str(payload.stdout.clone()),
            stderr: opt_cow_box_str(payload.stderr.clone()),
            success: payload.success,
            changes_json: opt_raw_json_box(payload.changes),
            status: opt_status_string(payload.status),
        }),
        "turn_aborted" => EventMsgData::TurnAborted(TurnAbortedEventMsg {
            reason: opt_cow_box_str(payload.reason.clone()),
        }),
        "context_compacted" => EventMsgData::ContextCompacted,
        "web_search_end" => EventMsgData::WebSearchEnd(WebSearchEndEventMsg {
            call_id: opt_box_str(payload.call_id),
            query: opt_cow_box_str(payload.query.clone()),
            action_json: opt_raw_json_box(payload.action),
        }),
        "thread_rolled_back" => EventMsgData::ThreadRolledBack(ThreadRolledBackEventMsg {
            num_turns: payload.num_turns,
        }),
        "collab_waiting_end" => EventMsgData::CollabWaitingEnd(CollabWaitingEndEventMsg {
            call_id: opt_box_str(payload.call_id),
            agent_statuses_json: opt_raw_json_box(payload.agent_statuses),
            statuses_json: opt_raw_json_box(payload.statuses),
        }),
        "mcp_tool_call_end" => EventMsgData::McpToolCallEnd(McpToolCallEndEventMsg {
            call_id: opt_box_str(payload.call_id),
            invocation_json: opt_raw_json_box(payload.invocation),
            duration_json: opt_raw_json_box(payload.duration),
            result_json: opt_raw_json_box(payload.result),
        }),
        "dynamic_tool_call_request" => {
            EventMsgData::DynamicToolCallRequest(DynamicToolCallRequestEventMsg {
                call_id: opt_box_str(payload.call_id),
                tool: opt_box_str(payload.tool),
                arguments_json: opt_raw_json_box(payload.arguments),
            })
        }
        "dynamic_tool_call_response" => {
            EventMsgData::DynamicToolCallResponse(DynamicToolCallResponseEventMsg {
                call_id: opt_box_str(payload.call_id),
                tool: opt_box_str(payload.tool),
                arguments_json: opt_raw_json_box(payload.arguments),
                content_items_json: opt_raw_json_box(payload.content_items),
                success: payload.success,
                error_json: opt_raw_json_box(payload.error),
                duration_json: opt_raw_json_box(payload.duration),
            })
        }
        "collab_agent_spawn_end" => {
            EventMsgData::CollabAgentSpawnEnd(CollabAgentSpawnEndEventMsg {
                call_id: opt_box_str(payload.call_id),
                sender_thread_id: opt_box_str(payload.sender_thread_id),
                new_thread_id: opt_box_str(payload.new_thread_id),
                new_agent_nickname: opt_cow_box_str(payload.new_agent_nickname.clone()),
                new_agent_role: opt_cow_box_str(payload.new_agent_role.clone()),
                prompt: opt_cow_box_str(payload.prompt.clone()),
                model: opt_box_str(payload.model),
                reasoning_effort: opt_box_str(payload.reasoning_effort),
                status_json: opt_raw_json_box(payload.status),
            })
        }
        "collab_close_end" => EventMsgData::CollabCloseEnd(CollabCloseEndEventMsg {
            call_id: opt_box_str(payload.call_id),
            sender_thread_id: opt_box_str(payload.sender_thread_id),
            receiver_thread_id: opt_box_str(payload.receiver_thread_id),
            receiver_agent_nickname: opt_cow_box_str(payload.receiver_agent_nickname.clone()),
            receiver_agent_role: opt_cow_box_str(payload.receiver_agent_role.clone()),
            status_json: opt_raw_json_box(payload.status),
        }),
        "collab_agent_interaction_end" => {
            EventMsgData::CollabAgentInteractionEnd(CollabAgentInteractionEndEventMsg {
                call_id: opt_box_str(payload.call_id),
                sender_thread_id: opt_box_str(payload.sender_thread_id),
                receiver_thread_id: opt_box_str(payload.receiver_thread_id),
                receiver_agent_nickname: opt_cow_box_str(payload.receiver_agent_nickname.clone()),
                receiver_agent_role: opt_cow_box_str(payload.receiver_agent_role.clone()),
                prompt: opt_cow_box_str(payload.prompt.clone()),
                status_json: opt_raw_json_box(payload.status),
            })
        }
        "error" => EventMsgData::Error(ErrorEventMsg {
            message: opt_cow_box_str(payload.message.clone()),
            codex_error_info: opt_cow_box_str(payload.codex_error_info.clone()),
        }),
        "entered_review_mode" => EventMsgData::EnteredReviewMode(EnteredReviewModeEventMsg {
            target_json: opt_raw_json_box(payload.target),
            user_facing_hint: opt_cow_box_str(payload.user_facing_hint.clone()),
        }),
        "exited_review_mode" => EventMsgData::ExitedReviewMode(ExitedReviewModeEventMsg {
            review_output_json: opt_raw_json_box(payload.review_output),
        }),
        _ => EventMsgData::Unknown(UnknownEventMsg {
            raw_json: payload
                .raw_json
                .map(json_to_box)
                .unwrap_or_else(|| box_str("{}")),
        }),
    }
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

fn deserialize_null_default<'de, D, T>(deserializer: D) -> std::result::Result<T, D::Error>
where
    D: SerdeDeserializer<'de>,
    T: Deserialize<'de> + Default,
{
    let value = Option::<T>::deserialize(deserializer)?;
    Ok(value.unwrap_or_default())
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

#[derive(Deserialize)]
struct SessionMetaPayload<'a> {
    #[serde(default)]
    id: Option<Cow<'a, str>>,
    #[serde(default)]
    session_id: Option<Cow<'a, str>>,
    #[serde(default)]
    cwd: Option<Cow<'a, str>>,
    #[serde(default)]
    originator: Option<Cow<'a, str>>,
    #[serde(default)]
    model: Option<Cow<'a, str>>,
    #[serde(default)]
    cli_version: Option<Cow<'a, str>>,
    #[serde(default)]
    timestamp: Option<Cow<'a, str>>,
}

#[derive(Deserialize)]
struct TurnContextPayload<'a> {
    #[serde(default)]
    turn_id: Option<Cow<'a, str>>,
    #[serde(default)]
    cwd: Option<Cow<'a, str>>,
    #[serde(default)]
    current_date: Option<Cow<'a, str>>,
    #[serde(default)]
    timezone: Option<Cow<'a, str>>,
    #[serde(default)]
    model: Option<Cow<'a, str>>,
}

#[derive(Deserialize)]
struct ResponseItemKindProbe<'a> {
    #[serde(rename = "type")]
    item_type: &'a str,
    #[serde(default)]
    role: Option<&'a str>,
}

#[derive(Deserialize)]
struct ResponseMessagePayload<'a> {
    #[serde(default)]
    role: Option<&'a str>,
    #[serde(default)]
    model: Option<&'a str>,
    #[serde(default)]
    phase: Option<&'a str>,
    #[serde(default, deserialize_with = "deserialize_null_default")]
    content: Vec<ResponseContent<'a>>,
}

#[derive(Deserialize)]
struct ResponseFunctionCallPayload<'a> {
    #[serde(default)]
    id: Option<&'a str>,
    #[serde(default)]
    name: Option<&'a str>,
    #[serde(default)]
    call_id: Option<&'a str>,
    #[serde(default, deserialize_with = "deserialize_opt_string_or_json_box")]
    arguments: Option<SmolStr>,
}

#[derive(Deserialize)]
struct ResponseFunctionCallOutputPayload<'a> {
    #[serde(default)]
    call_id: Option<&'a str>,
    #[serde(default, deserialize_with = "deserialize_opt_string_or_json_box")]
    output: Option<SmolStr>,
}

#[derive(Deserialize)]
struct ResponseCustomToolCallPayload<'a> {
    #[serde(default)]
    name: Option<&'a str>,
    #[serde(default)]
    call_id: Option<&'a str>,
    #[serde(default)]
    status: Option<&'a str>,
    #[serde(default, deserialize_with = "deserialize_opt_string_or_json_box")]
    input: Option<SmolStr>,
}

#[derive(Deserialize)]
struct ResponseCustomToolCallOutputPayload<'a> {
    #[serde(default)]
    call_id: Option<&'a str>,
    #[serde(default, deserialize_with = "deserialize_opt_string_or_json_box")]
    output: Option<SmolStr>,
}

#[derive(Deserialize)]
struct ResponseWebSearchCallPayload<'a> {
    #[serde(default)]
    status: Option<&'a str>,
    #[serde(default)]
    action: Option<WebSearchAction<'a>>,
}

#[derive(Deserialize)]
struct ResponseGhostSnapshotPayload<'a> {
    #[serde(default, borrow)]
    ghost_commit: Option<GhostCommit<'a>>,
}

#[derive(Deserialize)]
struct ResponseReasoningPayload<'a> {
    #[serde(default, deserialize_with = "deserialize_null_default")]
    summary: Vec<ResponseSummary<'a>>,
}

#[derive(Deserialize)]
struct ResponseContent<'a> {
    #[serde(rename = "type")]
    kind: &'a str,
    #[serde(default)]
    text: Option<Cow<'a, str>>,
    #[serde(default)]
    image_url: Option<Cow<'a, str>>,
    #[serde(default, borrow)]
    raw: Option<&'a RawValue>,
}

#[derive(Deserialize)]
struct ResponseSummary<'a> {
    #[serde(default)]
    text: Option<Cow<'a, str>>,
}

#[derive(Deserialize)]
struct EventMsgKindProbe<'a> {
    #[serde(rename = "type")]
    kind: &'a str,
}

#[derive(Deserialize)]
struct EventMsgPayload<'a> {
    #[serde(rename = "type")]
    kind: &'a str,
    #[serde(default)]
    id: Option<&'a str>,
    #[serde(default)]
    #[serde(alias = "turnId")]
    turn_id: Option<&'a str>,
    #[serde(default)]
    last_agent_message: Option<Cow<'a, str>>,
    #[serde(default)]
    phase: Option<&'a str>,
    #[serde(default)]
    completed_at: Option<i64>,
    #[serde(default)]
    duration_ms: Option<u64>,
    #[serde(default)]
    time_to_first_token_ms: Option<u64>,
    #[serde(default)]
    model_context_window: Option<i64>,
    #[serde(default)]
    collaboration_mode_kind: Option<&'a str>,
    #[serde(default)]
    message: Option<Cow<'a, str>>,
    #[serde(default)]
    text: Option<Cow<'a, str>>,
    #[serde(default, alias = "revisedPrompt")]
    revised_prompt: Option<Cow<'a, str>>,
    #[serde(default, borrow)]
    images: Option<&'a RawValue>,
    #[serde(default, borrow)]
    local_images: Option<&'a RawValue>,
    #[serde(default, borrow)]
    text_elements: Option<&'a RawValue>,
    #[serde(default, borrow)]
    info: Option<&'a RawValue>,
    #[serde(default)]
    #[serde(alias = "callId")]
    call_id: Option<&'a str>,
    #[serde(default, borrow)]
    command: Option<&'a RawValue>,
    #[serde(default, borrow)]
    parsed_cmd: Option<&'a RawValue>,
    #[serde(default)]
    stdout: Option<Cow<'a, str>>,
    #[serde(default)]
    stderr: Option<Cow<'a, str>>,
    #[serde(default)]
    aggregated_output: Option<Cow<'a, str>>,
    #[serde(default)]
    exit_code: Option<i64>,
    #[serde(default, borrow)]
    duration: Option<&'a RawValue>,
    #[serde(default)]
    formatted_output: Option<Cow<'a, str>>,
    #[serde(default, borrow)]
    status: Option<&'a RawValue>,
    #[serde(default)]
    success: Option<bool>,
    #[serde(default, borrow)]
    changes: Option<&'a RawValue>,
    #[serde(default)]
    reason: Option<Cow<'a, str>>,
    #[serde(default)]
    query: Option<Cow<'a, str>>,
    #[serde(default, borrow)]
    action: Option<&'a RawValue>,
    #[serde(default)]
    num_turns: Option<u64>,
    #[serde(default)]
    sender_thread_id: Option<&'a str>,
    #[serde(default, borrow)]
    agent_statuses: Option<&'a RawValue>,
    #[serde(default, borrow)]
    statuses: Option<&'a RawValue>,
    #[serde(default, borrow)]
    invocation: Option<&'a RawValue>,
    #[serde(default, borrow)]
    result: Option<&'a RawValue>,
    #[serde(default)]
    tool: Option<&'a str>,
    #[serde(default, borrow)]
    arguments: Option<&'a RawValue>,
    #[serde(default, borrow)]
    content_items: Option<&'a RawValue>,
    #[serde(default, borrow)]
    error: Option<&'a RawValue>,
    #[serde(default)]
    new_thread_id: Option<&'a str>,
    #[serde(default)]
    new_agent_nickname: Option<Cow<'a, str>>,
    #[serde(default)]
    new_agent_role: Option<Cow<'a, str>>,
    #[serde(default)]
    prompt: Option<Cow<'a, str>>,
    #[serde(default)]
    model: Option<&'a str>,
    #[serde(default)]
    reasoning_effort: Option<&'a str>,
    #[serde(default)]
    receiver_thread_id: Option<&'a str>,
    #[serde(default)]
    receiver_agent_nickname: Option<Cow<'a, str>>,
    #[serde(default)]
    receiver_agent_role: Option<Cow<'a, str>>,
    #[serde(default)]
    codex_error_info: Option<Cow<'a, str>>,
    #[serde(default, borrow)]
    target: Option<&'a RawValue>,
    #[serde(default)]
    user_facing_hint: Option<Cow<'a, str>>,
    #[serde(default, borrow)]
    review_output: Option<&'a RawValue>,
    #[serde(skip)]
    raw_json: Option<&'a RawValue>,
}

#[derive(Deserialize)]
struct WebSearchAction<'a> {
    #[serde(rename = "type")]
    kind: Option<&'a str>,
    #[serde(default)]
    query: Option<Cow<'a, str>>,
    #[serde(default)]
    queries: Vec<Cow<'a, str>>,
}

#[derive(Deserialize)]
struct GhostCommit<'a> {
    #[serde(default)]
    id: Option<&'a str>,
    #[serde(default)]
    parent: Option<&'a str>,
    #[serde(default)]
    preexisting_untracked_files: Vec<Cow<'a, str>>,
    #[serde(default)]
    preexisting_untracked_dirs: Vec<Cow<'a, str>>,
}

#[derive(Deserialize)]
struct CompactedPayload<'a> {
    #[serde(default)]
    message: Option<Cow<'a, str>>,
}

#[derive(Deserialize)]
struct ProbeEnvelope<'a> {
    #[serde(default)]
    timestamp: Option<&'a str>,
    #[serde(rename = "type")]
    kind: &'a str,
    #[serde(borrow)]
    payload: &'a RawValue,
}

#[derive(Deserialize)]
struct TimestampProbeEnvelope<'a> {
    #[serde(default)]
    timestamp: Option<&'a str>,
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

    #[cfg(feature = "agent_session")]
    #[test]
    fn direct_agent_session_reader_parses_current_fixture() {
        let bytes =
            include_bytes!("../../../tests/fixtures/codex/codex_current_sample.jsonl").as_slice();
        let metadata = crate::InputMetadata::new().name("codex-session.jsonl");
        let selection = crate::ParseSelection::full();
        let direct = super::event::parse_direct_agent_session_reader_selected(
            Cursor::new(bytes),
            metadata,
            selection,
        )
        .unwrap();

        assert_eq!(direct.agent.as_str(), "codex");
        assert!(!direct.events.is_empty());
    }

    #[cfg(feature = "agent_session")]
    #[test]
    fn direct_agent_session_reader_applies_message_selection() {
        let bytes =
            include_bytes!("../../../tests/fixtures/codex/codex_current_sample.jsonl").as_slice();
        let metadata = crate::InputMetadata::new().name("codex-session.jsonl");
        let selection = crate::ParseSelection::empty().with_meta().with_messages();
        let direct = super::event::parse_direct_agent_session_reader_selected(
            Cursor::new(bytes),
            metadata,
            selection,
        )
        .unwrap();

        assert!(direct.events.iter().all(|event| {
            matches!(
                event.body,
                crate::agent_session::Body::Prompt(_) | crate::agent_session::Body::Response(_)
            )
        }));
    }

    #[test]
    fn reader_meta_only_stops_before_later_malformed_line() {
        let bytes = concat!(
            r#"{"timestamp":"2026-04-16T00:00:00.000Z","type":"session_meta","payload":{"session_id":"sess-meta","cwd":"/tmp/project","originator":"codex-cli","model":"gpt-5.4"}}"#,
            "\n",
            "not-json",
        );

        let (version, body) =
            super::parse_codex_reader(Cursor::new(bytes), crate::ParseSelection::meta_only())
                .unwrap();

        assert_eq!(version, super::Version::V1);
        let [super::Entry::SessionMeta(meta)] = body.entries.as_ref() else {
            panic!("meta-only reader parse should return one session_meta entry");
        };
        assert_eq!(meta.session_id.as_deref(), Some("sess-meta"));
        assert_eq!(meta.cwd.as_deref(), Some("/tmp/project"));
    }

    #[test]
    fn probe_session_meta_accepts_escaped_windows_cwd() {
        let bytes = r#"{"timestamp":"2026-04-16T00:00:00.000Z","type":"session_meta","payload":{"session_id":"sess-windows","cwd":"C:\\Users\\alice\\project","originator":"codex-cli","model":"gpt-5.4"}}
"#;

        let meta = super::Codex::probe_session_meta(Cursor::new(bytes))
            .unwrap()
            .expect("session meta");

        assert_eq!(meta.session_id.as_deref(), Some("sess-windows"));
        assert_eq!(meta.cwd.as_deref(), Some(r"C:\Users\alice\project"));
    }

    #[test]
    fn reader_accepts_escaped_windows_paths_in_ghost_snapshot() {
        let bytes = concat!(
            r#"{"timestamp":"2026-04-16T00:00:01.000Z","type":"response_item","payload":{"type":"ghost_snapshot","ghost_commit":{"id":"abc123","preexisting_untracked_files":["C:\\Users\\alice\\project\\file.txt"],"preexisting_untracked_dirs":["D:\\work\\scratch"]}}}"#,
            "\n",
        );

        let (_version, body) =
            super::parse_codex_reader(Cursor::new(bytes), crate::ParseSelection::full()).unwrap();

        let [super::Entry::GhostSnapshot(snapshot)] = body.entries.as_ref() else {
            panic!("expected ghost snapshot entry");
        };
        assert_eq!(
            snapshot.preexisting_untracked_files.as_ref(),
            &[SmolStr::from(r"C:\Users\alice\project\file.txt")]
        );
        assert_eq!(
            snapshot.preexisting_untracked_dirs.as_ref(),
            &[SmolStr::from(r"D:\work\scratch")]
        );
    }

    #[cfg(feature = "agent_session")]
    #[test]
    fn probe_session_meta_with_title_uses_post_session_user_text() {
        use std::io::Cursor;

        let bytes = concat!(
            "{\"timestamp\":\"2026-05-11T00:49:47.936Z\",\"type\":\"session_meta\",\"payload\":{\"id\":\"codex-fork\",\"timestamp\":\"2026-05-11T00:49:47.936Z\",\"cwd\":\"/work/fork\"}}\n",
            "{\"timestamp\":\"2026-05-10T11:59:25.404Z\",\"type\":\"response_item\",\"payload\":{\"type\":\"message\",\"role\":\"user\",\"content\":[{\"type\":\"input_text\",\"text\":\"parent title should not leak\"}]}}\n",
            "{\"timestamp\":\"2026-05-11T00:49:51.117Z\",\"type\":\"response_item\",\"payload\":{\"type\":\"message\",\"role\":\"user\",\"content\":[{\"type\":\"input_text\",\"text\":\"Task: You are a delegated subagent running from a fork of the parent session. Treat the inherited conversation as reference-only context, not a live thread to continue.\\n\\nTask:\\nImplement the Codex parser title.\"}]}}\n",
        );

        let (_meta, title) =
            super::Codex::probe_session_meta_with_title(Cursor::new(bytes.as_bytes()))
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
    fn probe_session_meta_with_title_keeps_first_user_without_fork_marker() {
        use std::io::Cursor;

        let bytes = concat!(
            "{\"timestamp\":\"2026-05-11T00:49:47.936Z\",\"type\":\"session_meta\",\"payload\":{\"id\":\"codex-plain\",\"timestamp\":\"2026-05-11T00:49:47.936Z\",\"cwd\":\"/work/plain\"}}\n",
            "{\"timestamp\":\"2026-05-10T11:59:25.404Z\",\"type\":\"response_item\",\"payload\":{\"type\":\"message\",\"role\":\"user\",\"content\":[{\"type\":\"input_text\",\"text\":\"plain first user title\"}]}}\n",
            "{\"timestamp\":\"2026-05-11T00:49:51.117Z\",\"type\":\"response_item\",\"payload\":{\"type\":\"message\",\"role\":\"user\",\"content\":[{\"type\":\"input_text\",\"text\":\"plain Codex session title\"}]}}\n",
        );

        let (_meta, title) =
            super::Codex::probe_session_meta_with_title(Cursor::new(bytes.as_bytes()))
                .unwrap()
                .expect("expected session meta");

        assert_eq!(title.as_deref(), Some("plain first user title"));
    }

    #[cfg(feature = "agent_session")]
    #[test]
    fn probe_session_meta_with_title_scans_past_reasoning_lines_for_late_title() {
        use std::io::Cursor;

        let mut lines = vec![
            "{\"timestamp\":\"2026-05-11T00:49:47.936Z\",\"type\":\"session_meta\",\"payload\":{\"id\":\"codex-late-title\",\"timestamp\":\"2026-05-11T00:49:47.936Z\",\"cwd\":\"/work/late-title\"}}".to_string(),
        ];
        lines.extend((0..140).map(|idx| {
            format!(
                "{{\"timestamp\":\"2026-05-11T00:{idx:02}:00.000Z\",\"type\":\"response_item\",\"payload\":{{\"type\":\"reasoning\",\"summary\":[]}}}}"
            )
        }));
        lines.push("{\"timestamp\":\"2026-05-11T00:59:00.000Z\",\"type\":\"response_item\",\"payload\":{\"type\":\"message\",\"role\":\"user\",\"content\":[{\"type\":\"input_text\",\"text\":\"late Codex title\"}]}}".into());
        lines.push("not-json-after-title".into());

        let (meta, title) =
            super::Codex::probe_session_meta_with_title(Cursor::new(lines.join("\n")))
                .unwrap()
                .expect("expected session meta");

        assert_eq!(meta.session_id.as_deref(), Some("codex-late-title"));
        assert_eq!(title.as_deref(), Some("late Codex title"));
    }

    #[cfg(feature = "agent_session")]
    #[test]
    fn probe_session_meta_with_title_scans_past_noisy_post_meta_lines() {
        use std::io::Cursor;

        let mut lines = vec![
            "{\"timestamp\":\"2026-05-11T00:49:47.936Z\",\"type\":\"session_meta\",\"payload\":{\"id\":\"codex-post-meta-noise\",\"timestamp\":\"2026-05-11T00:49:47.936Z\",\"cwd\":\"/work/post-meta-noise\"}}".to_string(),
        ];
        lines.extend((0..70).map(|idx| {
            format!(
                "{{\"timestamp\":\"2026-05-11T00:{idx:02}:00.000Z\",\"type\":\"event_msg\",\"payload\":{{\"type\":\"token_count\",\"info\":{{}}}}}}"
            )
        }));
        lines.extend((0..70).map(|idx| format!("not-json-{idx}")));
        lines.push("{\"timestamp\":\"2026-05-11T00:59:00.000Z\",\"type\":\"response_item\",\"payload\":{\"type\":\"message\",\"role\":\"user\",\"content\":[{\"type\":\"input_text\",\"text\":\"late title after noisy post-meta rows\"}]}}".into());

        let (meta, title) =
            super::Codex::probe_session_meta_with_title(Cursor::new(lines.join("\n")))
                .unwrap()
                .expect("expected session meta");

        assert_eq!(meta.session_id.as_deref(), Some("codex-post-meta-noise"));
        assert_eq!(
            title.as_deref(),
            Some("late title after noisy post-meta rows")
        );
    }

    #[cfg(feature = "agent_session")]
    #[test]
    fn probe_session_meta_with_title_scans_to_late_session_meta() {
        use std::io::Cursor;

        let mut lines = Vec::new();
        lines.extend((0..300).map(|idx| {
            format!(
                "{{\"timestamp\":\"2026-05-11T00:{idx:02}:00.000Z\",\"type\":\"event_msg\",\"payload\":{{\"type\":\"token_count\",\"info\":{{}}}}}}"
            )
        }));
        lines.push(
            "{\"timestamp\":\"2026-05-11T00:49:47.936Z\",\"type\":\"session_meta\",\"payload\":{\"id\":\"codex-late-meta\",\"timestamp\":\"2026-05-11T00:49:47.936Z\",\"cwd\":\"/work/late-meta\"}}".to_string(),
        );
        lines.push("{\"timestamp\":\"2026-05-11T00:59:00.000Z\",\"type\":\"response_item\",\"payload\":{\"type\":\"message\",\"role\":\"user\",\"content\":[{\"type\":\"input_text\",\"text\":\"late meta title\"}]}}".into());
        lines.push("not-json-after-title".into());

        let (meta, title) =
            super::Codex::probe_session_meta_with_title(Cursor::new(lines.join("\n")))
                .expect("poison after title must not be read")
                .expect("expected session meta");

        assert_eq!(meta.session_id.as_deref(), Some("codex-late-meta"));
        assert_eq!(title.as_deref(), Some("late meta title"));
    }
}
