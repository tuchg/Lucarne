use smol_str::SmolStr;
use std::borrow::Cow;
use std::io::BufRead;

use serde::Deserialize;
use serde_json::value::RawValue;
use tracing::trace;

use crate::util::{box_str, cow_to_box};
use crate::{Error, ParseSelection, Result};

mod types;
pub(crate) use types::*;

#[cfg(feature = "discovery")]
mod discovery;
#[cfg(feature = "agent_session")]
mod event;

pub struct Gemini;

impl Gemini {
    pub(crate) fn name() -> &'static str {
        "gemini"
    }
}

#[cfg(feature = "watch")]
fn parse_gemini_reader<R>(
    reader: R,
    cwd: Option<SmolStr>,
    selection: ParseSelection,
) -> Result<Body>
where
    R: BufRead,
{
    parse_gemini_body_reader(reader, cwd, selection)
}

pub(super) fn parse_gemini_body_reader<R>(
    reader: R,
    cwd: Option<SmolStr>,
    selection: ParseSelection,
) -> Result<Body>
where
    R: BufRead,
{
    if selection.is_meta_only() {
        let raw: RawSessionMeta = serde_json::from_reader(reader)?;
        return Ok(Body {
            session_id: raw.session_id.into(),
            cwd,
            start_time: opt_box_str(raw.start_time),
            last_updated: opt_box_str(raw.last_updated),
            kind: opt_box_str(raw.kind),
            summary: opt_box_str(raw.summary),
            directories_json: raw.directories.as_deref().map(json_to_box),
            entries: Vec::new().into_boxed_slice(),
        });
    }

    let raw: RawSession = serde_json::from_reader(reader)?;
    let mut entries = Vec::with_capacity(raw.messages.len());

    for raw_message in raw.messages {
        let message: RawMessage = serde_json::from_str(raw_message.get())?;
        match message.kind.as_deref() {
            Some("user") => {
                if selection.includes_messages() {
                    entries.push(Entry::User(UserMessage {
                        id: opt_box_str(message.id),
                        timestamp: opt_box_str(message.timestamp),
                        content: map_user_content(message.content.as_deref())?.into_boxed_slice(),
                    }));
                }
            }
            Some("gemini") => {
                if selection.includes_messages()
                    || selection.includes_usage()
                    || selection.includes_operations()
                {
                    entries.push(Entry::Gemini(GeminiMessage {
                        id: opt_box_str(message.id),
                        timestamp: opt_box_str(message.timestamp),
                        content: selection
                            .includes_messages()
                            .then(|| message.content.as_deref().and_then(raw_string_to_box))
                            .flatten(),
                        model: opt_box_str(message.model),
                        thoughts: if selection.includes_messages() {
                            message
                                .thoughts
                                .into_iter()
                                .map(|thought| Thought {
                                    description: thought.description.unwrap_or_default().into(),
                                })
                                .collect::<Vec<_>>()
                                .into_boxed_slice()
                        } else {
                            Vec::new().into_boxed_slice()
                        },
                        tokens: selection
                            .includes_usage()
                            .then(|| {
                                message.tokens.map(|tokens| TokenUsage {
                                    input: tokens.input,
                                    output: tokens.output,
                                    cached: tokens.cached,
                                    thoughts: tokens.thoughts,
                                    tool: tokens.tool,
                                    total: tokens.total,
                                })
                            })
                            .flatten(),
                        tool_calls: if selection.includes_operations() {
                            message
                                .tool_calls
                                .into_iter()
                                .map(map_tool_call)
                                .collect::<Result<Vec<_>>>()?
                                .into_boxed_slice()
                        } else {
                            Vec::new().into_boxed_slice()
                        },
                    }));
                }
            }
            Some("info") => {
                if selection.includes_state() {
                    entries.push(Entry::Info(InfoMessage {
                        id: opt_box_str(message.id),
                        timestamp: opt_box_str(message.timestamp),
                        content: message
                            .content
                            .as_deref()
                            .and_then(raw_string_to_box)
                            .unwrap_or_else(|| "".into()),
                    }));
                }
            }
            Some(other) => {
                if selection.includes_raw_unknown() {
                    entries.push(Entry::Unknown(UnknownEntry {
                        kind: box_str(other),
                        raw_json: box_str(raw_message.get()),
                        timestamp: opt_box_str(message.timestamp),
                    }));
                }
            }
            None => {
                if selection.includes_raw_unknown() {
                    entries.push(Entry::Unknown(UnknownEntry {
                        kind: "unknown".into(),
                        raw_json: box_str(raw_message.get()),
                        timestamp: opt_box_str(message.timestamp),
                    }));
                }
            }
        }
    }

    if entries.is_empty() && selection.is_full() {
        return Err(Error::Detection {
            agent: Gemini::name(),
        });
    }

    trace!(
        target: "agent_sessions::parse",
        agent = Gemini::name(),
        version = ?Version::V1,
            entries = entries.len(),
            "parsed Gemini bundle"
    );
    Ok(Body {
        session_id: raw.session_id.into(),
        cwd,
        start_time: opt_box_str(raw.start_time),
        last_updated: opt_box_str(raw.last_updated),
        kind: opt_box_str(raw.kind),
        summary: opt_box_str(raw.summary),
        directories_json: raw.directories.as_deref().map(json_to_box),
        entries: entries.into_boxed_slice(),
    })
}

fn map_user_content(raw: Option<&RawValue>) -> Result<Vec<UserContentPart>> {
    let Some(raw) = raw else {
        return Ok(Vec::new());
    };
    match raw
        .get()
        .as_bytes()
        .iter()
        .copied()
        .find(|byte| !byte.is_ascii_whitespace())
    {
        Some(b'"') => {
            let text = serde_json::from_str::<Cow<'_, str>>(raw.get())?;
            return Ok(vec![UserContentPart::Text(TextPart {
                text: cow_to_box(text),
            })]);
        }
        Some(b'[') => {}
        _ => return Ok(Vec::new()),
    }

    let parts: Vec<&RawValue> = serde_json::from_str(raw.get())?;
    let mut mapped = Vec::with_capacity(parts.len());
    for part_raw in parts {
        let part: RawUserContent<'_> = serde_json::from_str(part_raw.get())?;
        if let Some(text) = part.text {
            mapped.push(UserContentPart::Text(TextPart {
                text: cow_to_box(text),
            }));
        } else {
            mapped.push(UserContentPart::Raw(RawPart {
                raw_json: box_str(part_raw.get()),
            }));
        }
    }
    Ok(mapped)
}

fn map_tool_call(raw: RawToolCall) -> Result<ToolCall> {
    Ok(ToolCall {
        id: opt_box_str(raw.id),
        name: raw.name.unwrap_or_else(|| "unknown".to_string()).into(),
        args_json: raw.args_json,
        status: opt_box_str(raw.status),
        timestamp: opt_box_str(raw.timestamp),
        result_display_json: raw.result_display_json,
        responses: raw
            .results
            .into_iter()
            .map(|raw| map_tool_response(raw.as_ref()))
            .collect::<Result<Vec<_>>>()?
            .into_boxed_slice(),
    })
}

fn map_tool_response(raw: &RawValue) -> Result<ToolResponse> {
    let response: RawToolResponse = serde_json::from_str(raw.get())?;
    let Some(function_response) = response.function_response else {
        return Ok(ToolResponse::Raw(RawPart {
            raw_json: box_str(raw.get()),
        }));
    };

    let Some(payload) = function_response.response else {
        return Ok(ToolResponse::Raw(RawPart {
            raw_json: box_str(raw.get()),
        }));
    };

    let payload: RawFunctionPayload = serde_json::from_str(payload.get())?;
    if let Some(output) = payload.output {
        return Ok(ToolResponse::Output(OutputResponse {
            id: opt_box_str(function_response.id),
            name: opt_box_str(function_response.name),
            output: output.into(),
        }));
    }
    if let Some(error) = payload.error {
        return Ok(ToolResponse::Error(ErrorResponse {
            id: opt_box_str(function_response.id),
            name: opt_box_str(function_response.name),
            error: error.into(),
        }));
    }

    Ok(ToolResponse::Raw(RawPart {
        raw_json: box_str(raw.get()),
    }))
}

fn json_to_box(raw: &RawValue) -> SmolStr {
    box_str(raw.get())
}

fn opt_box_str<S>(value: Option<S>) -> Option<SmolStr>
where
    S: AsRef<str>,
{
    value.map(|value| box_str(value.as_ref()))
}

fn raw_string_to_box(raw: &RawValue) -> Option<SmolStr> {
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
}

#[derive(Deserialize)]
struct RawSession {
    #[serde(rename = "sessionId")]
    session_id: String,
    #[serde(default, rename = "startTime")]
    start_time: Option<String>,
    #[serde(default, rename = "lastUpdated")]
    last_updated: Option<String>,
    #[serde(default)]
    kind: Option<String>,
    #[serde(default)]
    summary: Option<String>,
    #[serde(default)]
    directories: Option<Box<RawValue>>,
    #[serde(default)]
    messages: Vec<Box<RawValue>>,
}

#[derive(Deserialize)]
struct RawSessionMeta {
    #[serde(rename = "sessionId")]
    session_id: String,
    #[serde(default, rename = "startTime")]
    start_time: Option<String>,
    #[serde(default, rename = "lastUpdated")]
    last_updated: Option<String>,
    #[serde(default)]
    kind: Option<String>,
    #[serde(default)]
    summary: Option<String>,
    #[serde(default)]
    directories: Option<Box<RawValue>>,
}

#[derive(Deserialize)]
struct RawMessage {
    #[serde(default)]
    id: Option<String>,
    #[serde(default)]
    timestamp: Option<String>,
    #[serde(default, rename = "type")]
    kind: Option<String>,
    #[serde(default)]
    content: Option<Box<RawValue>>,
    #[serde(default)]
    model: Option<String>,
    #[serde(default)]
    thoughts: Vec<RawThought>,
    #[serde(default)]
    tokens: Option<RawTokens>,
    #[serde(default, rename = "toolCalls")]
    tool_calls: Vec<RawToolCall>,
}

#[derive(Deserialize)]
struct RawUserContent<'a> {
    #[serde(default, borrow)]
    text: Option<Cow<'a, str>>,
}

#[derive(Deserialize)]
struct RawThought {
    #[serde(default)]
    description: Option<String>,
}

#[derive(Deserialize)]
struct RawTokens {
    #[serde(default)]
    input: u64,
    #[serde(default)]
    output: u64,
    #[serde(default)]
    cached: u64,
    #[serde(default)]
    thoughts: u64,
    #[serde(default)]
    tool: u64,
    #[serde(default)]
    total: u64,
}

#[derive(Deserialize)]
struct RawToolCall {
    #[serde(default)]
    id: Option<String>,
    #[serde(default)]
    name: Option<String>,
    #[serde(
        default,
        rename = "args",
        deserialize_with = "deserialize_opt_json_box"
    )]
    args_json: Option<SmolStr>,
    #[serde(default)]
    status: Option<String>,
    #[serde(default)]
    timestamp: Option<String>,
    #[serde(
        default,
        rename = "resultDisplay",
        deserialize_with = "deserialize_opt_json_box"
    )]
    result_display_json: Option<SmolStr>,
    #[serde(default, rename = "result")]
    results: Vec<Box<RawValue>>,
}

#[derive(Deserialize)]
struct RawToolResponse {
    #[serde(default, rename = "functionResponse")]
    function_response: Option<RawFunctionResponse>,
}

#[derive(Deserialize)]
struct RawFunctionResponse {
    #[serde(default)]
    id: Option<String>,
    #[serde(default)]
    name: Option<String>,
    #[serde(default)]
    response: Option<Box<RawValue>>,
}

#[derive(Deserialize)]
struct RawFunctionPayload {
    #[serde(default)]
    output: Option<String>,
    #[serde(default)]
    error: Option<String>,
}

fn deserialize_opt_json_box<'de, D>(
    deserializer: D,
) -> std::result::Result<Option<SmolStr>, D::Error>
where
    D: serde::Deserializer<'de>,
{
    let raw = Option::<Box<RawValue>>::deserialize(deserializer)?;
    Ok(raw.as_deref().map(json_to_box))
}

#[cfg(test)]
mod tests {
    use std::io::Cursor;

    #[test]
    fn reader_accepts_escaped_windows_message_text() {
        let bytes = concat!(
            r#"{"sessionId":"gem-win","messages":[{"type":"user","id":"u1","timestamp":"2026-05-01T11:09:46.305Z","content":"open C:\\Users\\alice\\project\nthen inspect"}]}"#,
            "\n",
        );

        let body = super::parse_gemini_body_reader(
            Cursor::new(bytes.as_bytes()),
            Some(r#"C:\Users\alice\project"#.into()),
            crate::ParseSelection::full(),
        )
        .unwrap();

        let [super::Entry::User(message)] = body.entries.as_ref() else {
            panic!("expected one user entry");
        };
        let [super::UserContentPart::Text(text)] = message.content.as_ref() else {
            panic!("expected one text part");
        };
        assert_eq!(
            text.text.as_str(),
            "open C:\\Users\\alice\\project\nthen inspect"
        );
    }

    #[test]
    fn direct_agent_session_reader_parses_current_fixture() {
        let bytes = include_bytes!("../../../tests/fixtures/gemini/session-sample.json").as_slice();
        let metadata = crate::InputMetadata::new().name("gemini-session.json");
        let selection = crate::ParseSelection::full();
        let direct = super::event::parse_direct_agent_session_reader_selected(
            Cursor::new(bytes),
            metadata,
            selection,
        )
        .unwrap();

        assert_eq!(direct.agent.as_str(), "gemini");
        assert!(!direct.events.is_empty());
    }
}
