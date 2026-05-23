use std::borrow::Cow;
use std::collections::HashMap;
use std::io::BufRead;

use crate::util::{box_str, cow_to_box, opt_cow_to_box};
use crate::{Error, InputMetadata, ParseSelection, Result};
use serde::{Deserialize, Deserializer as SerdeDeserializer, Serialize};
use serde_json::Deserializer;
use serde_json::value::RawValue;
use smol_str::SmolStr;

mod types;
pub(crate) use types::*;

#[cfg(feature = "discovery")]
mod discovery;
#[cfg(feature = "agent_session")]
mod event;

const CLI_EVENTS_FILE: &str = "events.jsonl";
const WORKSPACE_FILE: &str = "workspace.yaml";
pub struct Copilot;

impl Copilot {
    pub(crate) fn name() -> &'static str {
        "copilot"
    }
}

fn parse_copilot_body_reader<R>(
    mut reader: R,
    metadata: InputMetadata<'_>,
    selection: ParseSelection,
) -> Result<(Version, Body)>
where
    R: BufRead,
{
    if metadata_looks_like_cli_events(metadata) {
        let body = if selection.is_meta_only() {
            parse_cli_events_meta_reader(reader, None)?
        } else {
            parse_cli_events_reader(reader, None, selection)?
        };
        return Ok((Version::CliEventsV1, Body::CliEvents(body)));
    }

    let mut text = String::new();
    reader.read_to_string(&mut text)?;
    if looks_like_chat_session_bytes(text.as_bytes()) {
        let body = if selection.is_meta_only() {
            parse_chat_session_meta_str(&text)?
        } else {
            parse_chat_session_str(&text, selection)?
        };
        return Ok((Version::ChatSessionV1, Body::ChatSession(body)));
    }

    let body = if selection.is_meta_only() {
        parse_cli_events_meta_reader(std::io::Cursor::new(text.as_bytes()), None)?
    } else {
        parse_cli_events_reader(std::io::Cursor::new(text.as_bytes()), None, selection)?
    };
    Ok((Version::CliEventsV1, Body::CliEvents(body)))
}

fn metadata_looks_like_cli_events(metadata: InputMetadata<'_>) -> bool {
    metadata
        .name
        .is_some_and(|name| name.ends_with(CLI_EVENTS_FILE))
        || metadata
            .media_type
            .is_some_and(|media_type| media_type == "application/jsonl")
}

fn looks_like_chat_session_bytes(bytes: &[u8]) -> bool {
    if !bytes.iter().any(|byte| !byte.is_ascii_whitespace()) {
        return false;
    }

    let mut stream = Deserializer::from_slice(bytes).into_iter::<RawChatSessionProbe<'_>>();
    let Some(first) = stream.next() else {
        return false;
    };
    let Ok(first) = first else {
        return false;
    };

    first.requests.is_some() && stream.next().is_none()
}

fn opt_box_str(value: Option<&str>) -> Option<SmolStr> {
    value.map(box_str)
}

fn parse_chat_session_str(text: &str, selection: ParseSelection) -> Result<ChatSessionBody> {
    let raw: RawChatSession<'_> = serde_json::from_str(text)?;
    let requests = if selection.includes_messages() {
        raw.requests
            .into_iter()
            .map(|request| ChatRequest {
                request_id: cow_to_box(request.request_id),
                prompt: request.message.and_then(|message| {
                    if let Some(text) = message.text.filter(|text| !text.is_empty()) {
                        Some(cow_to_box(text))
                    } else {
                        message
                            .parts
                            .into_iter()
                            .find_map(|part| part.text.map(cow_to_box))
                    }
                }),
                timestamp: request.timestamp,
                model_id: request.model_id.map(cow_to_box),
                response: request
                    .response
                    .into_iter()
                    .map(|part| ChatResponsePart {
                        text: extract_response_text(part.value),
                    })
                    .collect::<Vec<_>>()
                    .into_boxed_slice(),
            })
            .collect::<Vec<_>>()
            .into_boxed_slice()
    } else {
        Vec::new().into_boxed_slice()
    };

    Ok(ChatSessionBody {
        session_id: raw.session_id.map(cow_to_box),
        workspace_id: raw.workspace_id.map(cow_to_box),
        model: raw
            .selected_model
            .and_then(|model| model.identifier.map(box_str)),
        mode: raw.mode.and_then(|mode| mode.kind.or(mode.id)).map(box_str),
        requests,
    })
}

fn parse_chat_session_meta_str(text: &str) -> Result<ChatSessionBody> {
    let raw: RawChatSessionMeta<'_> = serde_json::from_str(text)?;
    Ok(ChatSessionBody {
        session_id: raw.session_id.map(cow_to_box),
        workspace_id: raw.workspace_id.map(cow_to_box),
        model: raw
            .selected_model
            .and_then(|model| model.identifier.map(box_str)),
        mode: raw.mode.and_then(|mode| mode.kind.or(mode.id)).map(box_str),
        requests: Vec::new().into_boxed_slice(),
    })
}

fn parse_cli_events_reader<R>(
    mut reader: R,
    workspace: Option<WorkspaceMetadata>,
    selection: ParseSelection,
) -> Result<CliEventsBody>
where
    R: BufRead,
{
    let mut records = Vec::new();
    let mut line = Vec::new();

    while read_next_nonempty_line(&mut reader, &mut line)? {
        let event: RawCliEvent<'_> = serde_json::from_slice(&line)?;
        match event.kind {
            "user.message" | "assistant.message" => {
                let tool_requests = if selection.includes_operations() {
                    event
                        .data
                        .tool_requests
                        .iter()
                        .map(|request| parse_raw_tool_request(request))
                        .collect::<Result<Vec<_>>>()?
                } else {
                    Vec::new()
                };
                records.push(CliRecord::Message(CliMessage {
                    message_id: opt_cow_to_box(event.data.message_id),
                    parent_tool_call_id: opt_cow_to_box(event.data.parent_tool_call_id),
                    role: if event.kind == "user.message" {
                        Role::User
                    } else {
                        Role::Assistant
                    },
                    content: event
                        .data
                        .content
                        .map(cow_to_box)
                        .unwrap_or_else(|| box_str("")),
                    output_tokens: event.data.output_tokens,
                    tool_names: tool_requests
                        .iter()
                        .filter_map(|request| opt_cow_to_box(request.name.clone()))
                        .collect::<Vec<_>>()
                        .into_boxed_slice(),
                    tool_requests: tool_requests
                        .into_iter()
                        .map(|request| ToolRequest {
                            name: opt_cow_to_box(request.name),
                            tool_call_id: opt_cow_to_box(request.tool_call_id),
                            command: request
                                .arguments
                                .and_then(|arguments| opt_cow_to_box(arguments.command)),
                        })
                        .collect::<Vec<_>>()
                        .into_boxed_slice(),
                    timestamp: opt_box_str(event.timestamp),
                }))
            }
            "tool.execution_start" => {
                if selection.includes_operations() {
                    let tool_arguments = event
                        .data
                        .tool_arguments
                        .map(parse_raw_tool_arguments)
                        .transpose()?;
                    records.push(CliRecord::ToolExecution(ToolExecution::Start {
                        tool_name: opt_cow_to_box(event.data.tool_name),
                        tool_call_id: opt_cow_to_box(event.data.tool_call_id),
                        command: tool_arguments
                            .and_then(|arguments| opt_cow_to_box(arguments.command)),
                        model: opt_cow_to_box(event.data.model),
                        timestamp: opt_box_str(event.timestamp),
                    }))
                }
            }
            "tool.execution_complete" => {
                if selection.includes_operations() {
                    records.push(CliRecord::ToolExecution(ToolExecution::Complete {
                        tool_name: opt_cow_to_box(event.data.tool_name),
                        tool_call_id: opt_cow_to_box(event.data.tool_call_id),
                        success: event.data.success,
                        error_message: event
                            .data
                            .error
                            .as_ref()
                            .and_then(|error| opt_box_str(error.message())),
                        error_code: event
                            .data
                            .error
                            .as_ref()
                            .and_then(|error| opt_box_str(error.code())),
                        model: opt_cow_to_box(event.data.model),
                        timestamp: opt_box_str(event.timestamp),
                    }))
                }
            }
            "session.task_complete" => records.push(CliRecord::TaskComplete(TaskComplete {
                summary: opt_cow_to_box(event.data.summary),
                timestamp: opt_box_str(event.timestamp),
            })),
            "assistant.turn_start" => records.push(CliRecord::TurnBoundary(TurnBoundary::Start {
                turn_id: opt_cow_to_box(event.data.turn_id),
                interaction_id: opt_cow_to_box(event.data.interaction_id),
                timestamp: opt_box_str(event.timestamp),
            })),
            "assistant.turn_end" => records.push(CliRecord::TurnBoundary(TurnBoundary::End {
                turn_id: opt_cow_to_box(event.data.turn_id),
                interaction_id: opt_cow_to_box(event.data.interaction_id),
                timestamp: opt_box_str(event.timestamp),
            })),
            "system.notification" => {
                match event
                    .data
                    .notification_kind
                    .as_ref()
                    .and_then(|kind| kind.kind_type.as_deref())
                {
                    Some("shell_completed") => {
                        records.push(CliRecord::SystemNotification(
                            SystemNotification::ShellCompleted {
                                content: opt_cow_to_box(event.data.content),
                                shell_id: event
                                    .data
                                    .notification_kind
                                    .as_ref()
                                    .and_then(|kind| opt_cow_to_box(kind.shell_id.clone())),
                                exit_code: event
                                    .data
                                    .notification_kind
                                    .as_ref()
                                    .and_then(|kind| kind.exit_code),
                                description: event
                                    .data
                                    .notification_kind
                                    .as_ref()
                                    .and_then(|kind| opt_cow_to_box(kind.description.clone())),
                                timestamp: opt_box_str(event.timestamp),
                            },
                        ));
                    }
                    _ => records.push(CliRecord::SystemNotification(SystemNotification::Other {
                        content: opt_cow_to_box(event.data.content),
                        notification_type: event
                            .data
                            .notification_kind
                            .as_ref()
                            .and_then(|kind| opt_cow_to_box(kind.kind_type.clone())),
                        timestamp: opt_box_str(event.timestamp),
                    })),
                }
            }
            "session.start" => records.push(CliRecord::SessionEvent(SessionEvent::Start {
                session_id: opt_cow_to_box(event.data.session_id),
                selected_model: opt_cow_to_box(event.data.selected_model),
                timestamp: opt_box_str(event.timestamp),
            })),
            "session.resume" => records.push(CliRecord::SessionEvent(SessionEvent::Resume {
                selected_model: opt_cow_to_box(event.data.selected_model),
                timestamp: opt_box_str(event.timestamp),
            })),
            "session.shutdown" => records.push(CliRecord::SessionEvent(SessionEvent::Shutdown {
                current_model: opt_cow_to_box(event.data.current_model),
                model_usages: event
                    .data
                    .model_metrics
                    .unwrap_or_default()
                    .into_iter()
                    .filter_map(|(model, metric)| {
                        let usage = metric.usage?;
                        Some(ShutdownModelUsage {
                            model: model.into(),
                            input_tokens: usage.input_tokens.unwrap_or(0),
                            output_tokens: usage.output_tokens.unwrap_or(0),
                            cache_read_tokens: usage.cache_read_tokens.unwrap_or(0),
                            cache_write_tokens: usage.cache_write_tokens.unwrap_or(0),
                            reasoning_tokens: usage.reasoning_tokens.unwrap_or(0),
                        })
                    })
                    .collect::<Vec<_>>()
                    .into_boxed_slice(),
                timestamp: opt_box_str(event.timestamp),
            })),
            "session.mode_changed" => {
                records.push(CliRecord::SessionEvent(SessionEvent::ModeChanged {
                    previous_mode: opt_cow_to_box(event.data.previous_mode),
                    new_mode: opt_cow_to_box(event.data.new_mode),
                    timestamp: opt_box_str(event.timestamp),
                }))
            }
            "session.model_change" => {
                records.push(CliRecord::SessionEvent(SessionEvent::ModelChange {
                    previous_model: opt_cow_to_box(event.data.previous_model),
                    new_model: opt_cow_to_box(event.data.new_model),
                    timestamp: opt_box_str(event.timestamp),
                }))
            }
            "session.plan_changed" => {
                records.push(CliRecord::SessionEvent(SessionEvent::PlanChanged {
                    operation: opt_cow_to_box(event.data.operation),
                    timestamp: opt_box_str(event.timestamp),
                }))
            }
            "session.compaction_start" => {
                records.push(CliRecord::SessionEvent(SessionEvent::CompactionStart {
                    timestamp: opt_box_str(event.timestamp),
                }))
            }
            "session.compaction_complete" => {
                records.push(CliRecord::SessionEvent(SessionEvent::CompactionComplete {
                    success: event.data.success,
                    summary_content: event.data.summary_content.map(|text| text.into()),
                    error_message: event
                        .data
                        .error
                        .as_ref()
                        .and_then(|error| opt_box_str(error.message())),
                    error_code: event
                        .data
                        .error
                        .as_ref()
                        .and_then(|error| opt_box_str(error.code())),
                    timestamp: opt_box_str(event.timestamp),
                }))
            }
            "session.truncation" => {
                records.push(CliRecord::SessionEvent(SessionEvent::Truncation {
                    timestamp: opt_box_str(event.timestamp),
                }))
            }
            "session.error" => records.push(CliRecord::SessionEvent(SessionEvent::Error {
                error_type: opt_cow_to_box(event.data.error_type),
                message: event.data.message.map(cow_to_box),
                timestamp: opt_box_str(event.timestamp),
            })),
            "subagent.started" => records.push(CliRecord::SubagentEvent(SubagentEvent::Started {
                tool_call_id: opt_cow_to_box(event.data.tool_call_id),
                agent_name: opt_cow_to_box(event.data.agent_name),
                agent_display_name: opt_cow_to_box(event.data.agent_display_name),
                agent_description: event.data.agent_description.map(|value| value.into()),
                timestamp: opt_box_str(event.timestamp),
            })),
            "subagent.completed" => {
                records.push(CliRecord::SubagentEvent(SubagentEvent::Completed {
                    tool_call_id: opt_cow_to_box(event.data.tool_call_id),
                    agent_name: opt_cow_to_box(event.data.agent_name),
                    agent_display_name: opt_cow_to_box(event.data.agent_display_name),
                    timestamp: opt_box_str(event.timestamp),
                }))
            }
            "subagent.failed" => records.push(CliRecord::SubagentEvent(SubagentEvent::Failed {
                tool_call_id: opt_cow_to_box(event.data.tool_call_id),
                agent_name: opt_cow_to_box(event.data.agent_name),
                agent_display_name: opt_cow_to_box(event.data.agent_display_name),
                error: event
                    .data
                    .error
                    .as_ref()
                    .and_then(|error| opt_box_str(error.message())),
                timestamp: opt_box_str(event.timestamp),
            })),
            "subagent.deselected" => {
                records.push(CliRecord::SubagentEvent(SubagentEvent::Deselected {
                    timestamp: opt_box_str(event.timestamp),
                }))
            }
            "abort" => records.push(CliRecord::Abort(AbortRecord {
                reason: event.data.reason.map(cow_to_box),
                timestamp: opt_box_str(event.timestamp),
            })),
            other => records.push(CliRecord::Unknown(UnknownRecord {
                kind: box_str(other),
                raw_json: serde_json::to_string(&event)?.into(),
                timestamp: opt_box_str(event.timestamp),
            })),
        }
    }

    if !selection.is_full() {
        records = records
            .into_iter()
            .filter_map(|record| select_cli_record(record, selection))
            .collect();
    }

    if records.is_empty() && selection.is_full() {
        return Err(Error::Detection {
            agent: Copilot::name(),
        });
    }

    Ok(CliEventsBody {
        workspace,
        records: records.into_boxed_slice(),
    })
}

fn select_cli_record(mut record: CliRecord, selection: ParseSelection) -> Option<CliRecord> {
    match &mut record {
        CliRecord::Message(message) => {
            if !(selection.includes_messages()
                || selection.includes_operations()
                || selection.includes_usage())
            {
                return None;
            }
            if !selection.includes_messages() {
                message.content = "".into();
            }
            if !selection.includes_operations() {
                message.tool_names = Vec::new().into_boxed_slice();
                message.tool_requests = Vec::new().into_boxed_slice();
            }
            if !selection.includes_usage() {
                message.output_tokens = None;
            }
            Some(record)
        }
        CliRecord::ToolExecution(_) => selection.includes_operations().then_some(record),
        CliRecord::TaskComplete(_)
        | CliRecord::TurnBoundary(_)
        | CliRecord::SubagentEvent(_)
        | CliRecord::Abort(_) => selection.includes_state_records().then_some(record),
        CliRecord::SystemNotification(SystemNotification::ShellCompleted { .. }) => {
            selection.includes_operations().then_some(record)
        }
        CliRecord::SystemNotification(SystemNotification::Other { .. }) => {
            selection.includes_state_records().then_some(record)
        }
        CliRecord::SessionEvent(SessionEvent::Shutdown { .. }) => {
            (selection.includes_state_records() || selection.includes_usage()).then_some(record)
        }
        CliRecord::SessionEvent(_) => selection.includes_state_records().then_some(record),
        CliRecord::Unknown(_) => selection.includes_raw_unknown().then_some(record),
    }
}

fn parse_cli_events_meta_reader<R>(
    mut reader: R,
    workspace: Option<WorkspaceMetadata>,
) -> Result<CliEventsBody>
where
    R: BufRead,
{
    let mut line = Vec::new();
    while read_next_nonempty_line(&mut reader, &mut line)? {
        let event: RawCliEvent<'_> = serde_json::from_slice(&line)?;
        if event.kind == "session.start" {
            return Ok(CliEventsBody {
                workspace,
                records: vec![CliRecord::SessionEvent(SessionEvent::Start {
                    session_id: opt_cow_to_box(event.data.session_id),
                    selected_model: opt_cow_to_box(event.data.selected_model),
                    timestamp: opt_box_str(event.timestamp),
                })]
                .into_boxed_slice(),
            });
        }
    }

    Ok(CliEventsBody {
        workspace,
        records: Vec::new().into_boxed_slice(),
    })
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

fn extract_response_text(raw: Option<&RawValue>) -> Option<SmolStr> {
    let raw = raw?;
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
        Some(b'{') => {
            #[derive(Deserialize)]
            struct ValueWrapper<'a> {
                #[serde(default, borrow)]
                value: Option<Cow<'a, str>>,
            }
            serde_json::from_str::<ValueWrapper<'_>>(raw.get())
                .ok()
                .and_then(|wrapper| wrapper.value.map(cow_to_box))
        }
        _ => None,
    }
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct RawChatSession<'a> {
    #[serde(default)]
    session_id: Option<Cow<'a, str>>,
    #[serde(default)]
    workspace_id: Option<Cow<'a, str>>,
    #[serde(default, borrow)]
    requests: Vec<RawChatRequest<'a>>,
    #[serde(default, borrow)]
    mode: Option<RawMode<'a>>,
    #[serde(default, borrow)]
    selected_model: Option<RawSelectedModel<'a>>,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct RawChatSessionMeta<'a> {
    #[serde(default)]
    session_id: Option<Cow<'a, str>>,
    #[serde(default)]
    workspace_id: Option<Cow<'a, str>>,
    #[serde(default, borrow)]
    mode: Option<RawMode<'a>>,
    #[serde(default, borrow)]
    selected_model: Option<RawSelectedModel<'a>>,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct RawChatSessionProbe<'a> {
    #[serde(default, borrow)]
    requests: Option<&'a serde_json::value::RawValue>,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct RawChatRequest<'a> {
    request_id: Cow<'a, str>,
    #[serde(default, borrow)]
    message: Option<RawChatMessage<'a>>,
    #[serde(default, borrow)]
    response: Vec<RawResponsePart<'a>>,
    #[serde(default)]
    timestamp: Option<i64>,
    #[serde(default)]
    model_id: Option<Cow<'a, str>>,
}

#[derive(Deserialize)]
struct RawChatMessage<'a> {
    #[serde(default)]
    text: Option<Cow<'a, str>>,
    #[serde(default, borrow)]
    parts: Vec<RawMessagePart<'a>>,
}

#[derive(Deserialize)]
struct RawMessagePart<'a> {
    #[serde(default)]
    text: Option<Cow<'a, str>>,
}

#[derive(Deserialize)]
struct RawResponsePart<'a> {
    #[serde(default, borrow)]
    value: Option<&'a RawValue>,
}

#[derive(Deserialize)]
struct RawMode<'a> {
    #[serde(default)]
    id: Option<&'a str>,
    #[serde(default)]
    kind: Option<&'a str>,
}

#[derive(Deserialize)]
struct RawSelectedModel<'a> {
    #[serde(default)]
    identifier: Option<&'a str>,
}

#[derive(Deserialize, Serialize)]
struct RawCliEvent<'a> {
    #[serde(rename = "type")]
    kind: &'a str,
    #[serde(default)]
    timestamp: Option<&'a str>,
    #[serde(default, borrow)]
    data: RawCliEventData<'a>,
}

#[derive(Deserialize, Serialize, Default)]
#[serde(rename_all = "camelCase")]
struct RawCliEventData<'a> {
    #[serde(default)]
    content: Option<Cow<'a, str>>,
    #[serde(default)]
    message_id: Option<Cow<'a, str>>,
    #[serde(default)]
    summary: Option<Cow<'a, str>>,
    #[serde(default)]
    message: Option<Cow<'a, str>>,
    #[serde(default)]
    reason: Option<Cow<'a, str>>,
    #[serde(default)]
    tool_name: Option<Cow<'a, str>>,
    #[serde(default)]
    tool_call_id: Option<Cow<'a, str>>,
    #[serde(default)]
    parent_tool_call_id: Option<Cow<'a, str>>,
    #[serde(default)]
    turn_id: Option<Cow<'a, str>>,
    #[serde(default)]
    interaction_id: Option<Cow<'a, str>>,
    #[serde(default)]
    session_id: Option<Cow<'a, str>>,
    #[serde(default)]
    selected_model: Option<Cow<'a, str>>,
    #[serde(default)]
    current_model: Option<Cow<'a, str>>,
    #[serde(default)]
    model_metrics: Option<HashMap<String, RawCliModelMetric>>,
    #[serde(default)]
    previous_mode: Option<Cow<'a, str>>,
    #[serde(default)]
    new_mode: Option<Cow<'a, str>>,
    #[serde(default)]
    previous_model: Option<Cow<'a, str>>,
    #[serde(default)]
    new_model: Option<Cow<'a, str>>,
    #[serde(default)]
    operation: Option<Cow<'a, str>>,
    #[serde(default)]
    success: Option<bool>,
    #[serde(default)]
    model: Option<Cow<'a, str>>,
    #[serde(default)]
    output_tokens: Option<u64>,
    #[serde(default)]
    summary_content: Option<String>,
    #[serde(default)]
    error_type: Option<Cow<'a, str>>,
    #[serde(default, borrow, deserialize_with = "deserialize_opt_cli_error")]
    error: Option<RawCliError<'a>>,
    #[serde(default)]
    agent_name: Option<Cow<'a, str>>,
    #[serde(default)]
    agent_display_name: Option<Cow<'a, str>>,
    #[serde(default)]
    agent_description: Option<String>,
    #[serde(default, rename = "kind")]
    notification_kind: Option<RawNotificationKind<'a>>,
    #[serde(default, borrow)]
    tool_requests: Vec<&'a RawValue>,
    #[serde(default, borrow, rename = "arguments")]
    tool_arguments: Option<&'a RawValue>,
}

#[derive(Deserialize, Serialize)]
struct RawToolRequest<'a> {
    #[serde(default)]
    name: Option<Cow<'a, str>>,
    #[serde(default, rename = "toolCallId")]
    tool_call_id: Option<Cow<'a, str>>,
    #[serde(default, borrow)]
    arguments: Option<RawToolArguments<'a>>,
}

fn parse_raw_tool_request<'a>(raw: &'a RawValue) -> Result<RawToolRequest<'a>> {
    Ok(serde_json::from_str(raw.get())?)
}

fn parse_raw_tool_arguments<'a>(raw: &'a RawValue) -> Result<RawToolArguments<'a>> {
    Ok(serde_json::from_str(raw.get())?)
}

#[derive(Serialize)]
struct RawToolArguments<'a> {
    #[serde(default)]
    command: Option<Cow<'a, str>>,
}

impl<'de: 'a, 'a> Deserialize<'de> for RawToolArguments<'a> {
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: SerdeDeserializer<'de>,
    {
        #[derive(Deserialize)]
        #[serde(untagged)]
        enum Repr<'a> {
            Object {
                #[serde(default, borrow)]
                command: Option<Cow<'a, str>>,
            },
            String(Cow<'a, str>),
        }

        Ok(match Repr::deserialize(deserializer)? {
            Repr::Object { command } => Self { command },
            Repr::String(command) => Self {
                command: Some(command),
            },
        })
    }
}

#[derive(Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
struct RawNotificationKind<'a> {
    #[serde(rename = "type")]
    #[serde(default)]
    kind_type: Option<Cow<'a, str>>,
    #[serde(default)]
    shell_id: Option<Cow<'a, str>>,
    #[serde(default)]
    exit_code: Option<i64>,
    #[serde(default)]
    description: Option<Cow<'a, str>>,
}

#[derive(Deserialize, Serialize, Default)]
#[serde(rename_all = "camelCase")]
struct RawCliModelMetric {
    #[serde(default)]
    usage: Option<RawCliUsageMetric>,
}

#[derive(Deserialize, Serialize, Default)]
#[serde(rename_all = "camelCase")]
struct RawCliUsageMetric {
    #[serde(default)]
    input_tokens: Option<u64>,
    #[serde(default)]
    output_tokens: Option<u64>,
    #[serde(default)]
    cache_read_tokens: Option<u64>,
    #[serde(default)]
    cache_write_tokens: Option<u64>,
    #[serde(default)]
    reasoning_tokens: Option<u64>,
}

#[derive(Serialize)]
struct RawCliError<'a> {
    #[serde(default)]
    message: Option<Cow<'a, str>>,
    #[serde(default)]
    code: Option<Cow<'a, str>>,
}

impl RawCliError<'_> {
    fn message(&self) -> Option<&str> {
        self.message.as_deref()
    }

    fn code(&self) -> Option<&str> {
        self.code.as_deref()
    }
}

#[derive(Deserialize)]
struct RawCliErrorObject<'a> {
    #[serde(default)]
    message: Option<Cow<'a, str>>,
    #[serde(default)]
    code: Option<Cow<'a, str>>,
}

fn deserialize_opt_cli_error<'de, D>(
    deserializer: D,
) -> std::result::Result<Option<RawCliError<'de>>, D::Error>
where
    D: SerdeDeserializer<'de>,
{
    let raw = Option::<&RawValue>::deserialize(deserializer)?;
    raw.map(parse_cli_error)
        .transpose()
        .map_err(serde::de::Error::custom)
}

fn parse_cli_error<'a>(raw: &'a RawValue) -> serde_json::Result<RawCliError<'a>> {
    match raw
        .get()
        .as_bytes()
        .iter()
        .copied()
        .find(|byte| !byte.is_ascii_whitespace())
    {
        Some(b'"') => Ok(RawCliError {
            message: Some(serde_json::from_str::<Cow<'a, str>>(raw.get())?),
            code: None,
        }),
        Some(b'{') => {
            let object: RawCliErrorObject<'a> = serde_json::from_str(raw.get())?;
            Ok(RawCliError {
                message: object.message,
                code: object.code,
            })
        }
        _ => Err(serde_json::Error::io(std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            "copilot error must be a string or object",
        ))),
    }
}

#[cfg(test)]
mod tests {
    use std::io::Cursor;

    #[test]
    fn cli_events_reader_parses_current_fixture() {
        let events = include_bytes!("../../../tests/fixtures/copilot/events.jsonl").as_slice();
        let from_reader = super::parse_cli_events_reader(
            Cursor::new(events),
            None,
            crate::ParseSelection::full(),
        )
        .unwrap();

        assert!(!from_reader.records.is_empty());
    }

    #[test]
    fn cli_events_meta_reader_stops_before_later_malformed_line() {
        let bytes = concat!(
            r#"{"type":"session.start","timestamp":"2026-04-16T10:00:01Z","data":{"sessionId":"sess-copilot-meta","selectedModel":"gpt-5.4"}}"#,
            "\n",
            "not-json",
        );

        let body = super::parse_cli_events_meta_reader(Cursor::new(bytes), None).unwrap();
        let [
            super::CliRecord::SessionEvent(super::SessionEvent::Start {
                session_id,
                selected_model,
                ..
            }),
        ] = body.records.as_ref()
        else {
            panic!("meta-only reader parse should return one session.start record");
        };
        assert_eq!(session_id.as_deref(), Some("sess-copilot-meta"));
        assert_eq!(selected_model.as_deref(), Some("gpt-5.4"));
    }

    #[test]
    fn chat_session_reader_api_parses_current_fixture() {
        let bytes =
            include_bytes!("../../../tests/fixtures/copilot/chat-session-sample.json").as_slice();
        let (_version, body) = super::parse_copilot_body_reader(
            Cursor::new(bytes),
            crate::InputMetadata::default(),
            crate::ParseSelection::full(),
        )
        .unwrap();

        let super::Body::ChatSession(from_reader) = body else {
            panic!("expected copilot chat-session body");
        };
        assert!(!from_reader.requests.is_empty());
    }

    #[test]
    fn chat_session_reader_accepts_escaped_windows_workspace_and_prompt() {
        let bytes = concat!(
            r#"{"sessionId":"sess-chat","workspaceId":"C:\\Users\\alice\\project","requests":[{"requestId":"req-1","message":{"text":"open C:\\Users\\alice\\project\nthen inspect"},"response":[]}]}"#,
            "\n",
        );

        let (_version, body) = super::parse_copilot_body_reader(
            Cursor::new(bytes.as_bytes()),
            crate::InputMetadata::default(),
            crate::ParseSelection::full(),
        )
        .unwrap();

        let super::Body::ChatSession(body) = body else {
            panic!("expected chat session body");
        };
        assert_eq!(
            body.workspace_id.as_deref(),
            Some(r#"C:\Users\alice\project"#)
        );
        let [request] = body.requests.as_ref() else {
            panic!("expected one request");
        };
        assert_eq!(
            request.prompt.as_deref(),
            Some("open C:\\Users\\alice\\project\nthen inspect")
        );
    }

    #[cfg(feature = "agent_session")]
    #[test]
    fn direct_agent_session_reader_parses_chat_fixture() {
        let bytes =
            include_bytes!("../../../tests/fixtures/copilot/chat-session-sample.json").as_slice();
        let metadata = crate::InputMetadata::default();
        let selection = crate::ParseSelection::full();
        let direct = super::event::parse_direct_agent_session_reader_selected(
            Cursor::new(bytes),
            metadata,
            selection,
        )
        .unwrap();

        assert_eq!(direct.agent.as_str(), "copilot");
        assert!(!direct.events.is_empty());
    }

    #[cfg(feature = "agent_session")]
    #[test]
    fn direct_agent_session_reader_parses_cli_events_fixture() {
        let bytes = include_bytes!("../../../tests/fixtures/copilot/events.jsonl").as_slice();
        let metadata = crate::InputMetadata::new().name("events.jsonl");
        let selection = crate::ParseSelection::full();
        let direct = super::event::parse_direct_agent_session_reader_selected(
            Cursor::new(bytes),
            metadata,
            selection,
        )
        .unwrap();

        assert_eq!(direct.agent.as_str(), "copilot");
        assert!(!direct.events.is_empty());
    }

    #[cfg(feature = "agent_session")]
    #[test]
    fn direct_agent_session_reader_filters_cli_message_selection() {
        let bytes = include_bytes!("../../../tests/fixtures/copilot/events.jsonl").as_slice();
        let metadata = crate::InputMetadata::new().name("events.jsonl");
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

    #[cfg(feature = "agent_session")]
    #[test]
    fn direct_agent_session_reader_uses_cli_carriers_for_message_selection() {
        let bytes = [
            r#"{"type":"session.start","timestamp":"2026-04-16T10:00:00Z","data":{"sessionId":"sess-carrier","selectedModel":"gpt-5.4"}}"#,
            r#"{"type":"assistant.turn_start","timestamp":"2026-04-16T10:00:01Z","data":{"turnId":"turn-1","interactionId":"interaction-1"}}"#,
            r#"{"type":"assistant.message","timestamp":"2026-04-16T10:00:02Z","data":{"messageId":"msg-1","content":"done","outputTokens":7}}"#,
        ]
        .join("\n");
        let selection = crate::ParseSelection::empty().with_messages().with_usage();
        let direct = super::event::parse_direct_agent_session_reader_selected(
            Cursor::new(bytes.as_bytes()),
            crate::InputMetadata::new().name("events.jsonl"),
            selection,
        )
        .unwrap();

        assert_eq!(direct.meta.session_id, None);
        let [response, usage] = direct.events.as_ref() else {
            panic!("message selection should only expose response and usage events");
        };
        assert_eq!(response.turn_id.as_deref(), Some("turn-1"));
        let crate::agent_session::Body::Response(response_body) = &response.body else {
            panic!("expected assistant response");
        };
        assert_eq!(response_body.model.as_deref(), Some("gpt-5.4"));

        assert_eq!(usage.turn_id.as_deref(), Some("turn-1"));
        let crate::agent_session::Body::Usage(usage_body) = &usage.body else {
            panic!("expected assistant usage");
        };
        assert_eq!(usage_body.model.as_deref(), Some("gpt-5.4"));
        assert_eq!(usage_body.output_tokens, 7);
    }
}
