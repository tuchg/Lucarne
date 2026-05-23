use smol_str::SmolStr;
use std::io::BufRead;

use serde::Deserialize;
use serde_json::value::RawValue;
use tracing::trace;

use crate::{Error, ParseSelection, Result};

mod types;
pub(crate) use types::*;

#[cfg(feature = "discovery")]
mod discovery;
#[cfg(feature = "agent_session")]
mod event;

pub struct Pi;

// ── Raw JSON types (private) ──────────────────────────────────────

/// A single JSONL line envelope.
#[derive(Debug, Deserialize)]

struct RawLine {
    #[serde(rename = "type")]
    entry_type: String,
    #[serde(default)]
    id: Option<String>,
    #[serde(default)]
    timestamp: Option<String>,

    // session header
    #[serde(default)]
    cwd: Option<String>,
    #[serde(rename = "parentSession", default)]
    parent_session: Option<String>,

    // message
    #[serde(default)]
    message: Option<RawMessage>,

    // model_change
    #[serde(default)]
    provider: Option<String>,
    #[serde(rename = "modelId", default)]
    model_id: Option<String>,

    // thinking_level_change
    #[serde(rename = "thinkingLevel", default)]
    thinking_level: Option<String>,

    // compaction
    #[serde(default)]
    summary: Option<String>,
    #[serde(rename = "firstKeptEntryId", default)]
    first_kept_entry_id: Option<String>,
    #[serde(rename = "tokensBefore", default)]
    tokens_before: Option<u64>,

    // branch_summary
    #[serde(rename = "fromId", default)]
    from_id: Option<String>,

    // custom / custom_message
    #[serde(rename = "customType", default)]
    custom_type: Option<String>,
    #[serde(default)]
    data: Option<serde_json::Value>,
    #[serde(default)]
    content: Option<serde_json::Value>,
    #[serde(default)]
    display: Option<bool>,

    // label
    #[serde(rename = "targetId", default)]
    target_id: Option<String>,
    #[serde(default)]
    label: Option<String>,

    // session_info
    #[serde(default)]
    name: Option<String>,
}

#[derive(Debug, Deserialize)]
struct RawMessage {
    #[serde(default)]
    role: String,
    #[serde(default)]
    model: Option<String>,
    #[serde(default)]
    content: Option<serde_json::Value>,
    #[serde(default)]
    usage: Option<Box<RawValue>>,
    #[serde(rename = "stopReason", default)]
    stop_reason: Option<String>,
    #[serde(rename = "errorMessage", default)]
    error_message: Option<String>,
    #[serde(rename = "toolName", default)]
    tool_name: Option<String>,
    #[serde(rename = "isError", default)]
    is_error: Option<bool>,
}

#[derive(Debug, Deserialize)]
struct RawUsage {
    #[serde(default)]
    input: u64,
    #[serde(default)]
    output: u64,
    #[serde(rename = "cacheRead", default)]
    cache_read: u64,
    #[serde(rename = "cacheWrite", default)]
    cache_write: u64,
    #[serde(rename = "totalTokens", default)]
    total_tokens: u64,
}

impl Pi {
    pub(crate) fn name() -> &'static str {
        "pi"
    }
}

#[cfg(any(test, feature = "watch"))]
fn parse_pi_reader<R>(reader: R, selection: ParseSelection) -> Result<Body>
where
    R: BufRead,
{
    parse_pi_body_reader(reader, selection)
}

pub(super) fn parse_pi_body_reader<R>(reader: R, selection: ParseSelection) -> Result<Body>
where
    R: BufRead,
{
    let mut session_id = None;
    let mut entries = Vec::new();
    parse_pi_reader_into(reader, selection, &mut session_id, &mut entries)?;
    finish_pi_body(session_id, entries)
}

fn parse_pi_reader_into<R>(
    mut reader: R,
    selection: ParseSelection,
    session_id: &mut Option<SmolStr>,
    entries: &mut Vec<Entry>,
) -> Result<()>
where
    R: BufRead,
{
    let mut line = Vec::new();
    let mut line_num = 0usize;

    loop {
        line.clear();
        let bytes_read = reader
            .read_until(b'\n', &mut line)
            .map_err(|err| Error::Message(err.to_string().into()))?;
        if bytes_read == 0 {
            return Ok(());
        }
        line_num += 1;
        if line.iter().all(|byte| byte.is_ascii_whitespace()) {
            continue;
        }

        let line = std::str::from_utf8(&line)
            .map_err(|_| Error::Message("pi session file is not valid UTF-8".into()))?
            .trim();

        let raw: RawLine = serde_json::from_str(line)
            .map_err(|err| Error::Message(format!("pi line {line_num}: {err}").into()))?;

        // Session header → insert as first entry.
        if raw.entry_type == "session" {
            let sid: SmolStr = raw.id.unwrap_or_default().into();
            let c: Option<SmolStr> = Some(raw.cwd.unwrap_or_default().into());
            let ts: Option<SmolStr> = Some(raw.timestamp.unwrap_or_default().into());
            *session_id = Some(sid.clone());
            if selection.includes_meta() {
                entries.push(Entry::SessionInfo {
                    session_id: sid,
                    cwd: c,
                    timestamp: ts,
                    name: raw.name.map(Into::into),
                });
            }
            if selection.is_meta_only() {
                return Ok(());
            }
            continue;
        }

        let entry = match raw.entry_type.as_str() {
            "message" => {
                let msg = raw.message.as_ref();
                let role = msg.map(|m| m.role.as_str()).unwrap_or("");
                match role {
                    "user" if selection.includes_messages() => {
                        let content = msg.and_then(|m| m.content.as_ref());
                        let text = content.and_then(extract_msg_text).unwrap_or_default();
                        let blocks = content.map(parse_blocks).unwrap_or_default();
                        Some(Entry::UserMessage {
                            text: text.into(),
                            blocks,
                            timestamp: raw.timestamp.as_deref().map(Into::into),
                        })
                    }
                    "assistant" if selection.includes_messages() => {
                        let content = msg.and_then(|m| m.content.as_ref());
                        let text = content.and_then(extract_msg_text).unwrap_or_default();
                        let blocks = content.map(parse_blocks).unwrap_or_default();
                        let model = msg.and_then(|m| m.model.as_deref()).map(Into::into);
                        let stop_reason =
                            msg.and_then(|m| m.stop_reason.as_deref()).map(Into::into);
                        let error_message =
                            msg.and_then(|m| m.error_message.as_deref()).map(Into::into);
                        let (
                            input_tokens,
                            output_tokens,
                            cache_read_tokens,
                            cache_write_tokens,
                            total_tokens,
                        ) = if selection.includes_usage() {
                            extract_usage(msg.and_then(|m| m.usage.as_deref()))?
                        } else {
                            (0, 0, 0, 0, 0)
                        };
                        Some(Entry::AssistantMessage {
                            text: text.into(),
                            blocks,
                            timestamp: raw.timestamp.as_deref().map(Into::into),
                            model,
                            stop_reason,
                            error_message,
                            input_tokens,
                            output_tokens,
                            cache_read_tokens,
                            cache_write_tokens,
                            total_tokens,
                        })
                    }
                    "toolResult" if selection.includes_operations() => {
                        let text = msg
                            .and_then(|m| m.content.as_ref())
                            .and_then(extract_msg_text)
                            .unwrap_or_default();
                        let tool_name = msg
                            .and_then(|m| m.tool_name.as_deref())
                            .unwrap_or("unknown");
                        let is_error = msg.and_then(|m| m.is_error).unwrap_or(false);
                        Some(Entry::ToolResult {
                            tool_name: tool_name.into(),
                            text: text.into(),
                            is_error,
                        })
                    }
                    _ => None,
                }
            }
            "model_change" if selection.includes_state() => {
                let model_id = raw.model_id.unwrap_or_default();
                Some(Entry::ModelChange {
                    provider: raw.provider.map(Into::into),
                    model_id: model_id.into(),
                })
            }
            "thinking_level_change" if selection.includes_state() => {
                let level = raw.thinking_level.unwrap_or_default();
                Some(Entry::ThinkingLevelChange {
                    level: level.into(),
                })
            }
            "compaction" if selection.includes_state() => {
                let summary = raw.summary.unwrap_or_default();
                Some(Entry::Compaction {
                    summary: summary.into(),
                    first_kept_entry_id: raw.first_kept_entry_id.map(Into::into),
                    tokens_before: raw.tokens_before,
                })
            }
            "branch_summary" if selection.includes_state() => {
                let summary = raw.summary.unwrap_or_default();
                Some(Entry::BranchSummary {
                    summary: summary.into(),
                    from_id: raw.from_id.map(Into::into),
                })
            }
            "custom" if selection.includes_state() => {
                let custom_type = raw.custom_type.unwrap_or_default();
                let data_json = raw.data.map(|v| v.to_string().into());
                Some(Entry::Custom {
                    custom_type: custom_type.into(),
                    data_json,
                })
            }
            "custom_message" if selection.includes_messages() => {
                let custom_type = raw.custom_type.unwrap_or_default();
                let text = raw.content.as_ref().and_then(extract_msg_text);
                let display = raw.display.unwrap_or(true);
                Some(Entry::CustomMessage {
                    custom_type: custom_type.into(),
                    text: text.map(Into::into),
                    display,
                })
            }
            "label" if selection.includes_state() => Some(Entry::Label {
                target_id: raw.target_id.map(Into::into),
                label: raw.label.map(Into::into),
            }),
            "session_info" if selection.includes_meta() => {
                let name = raw.name.unwrap_or_default();
                // session_info is a later rename — carry forward session header id if known.
                Some(Entry::SessionInfo {
                    session_id: session_id.clone().unwrap_or_else(|| "".into()),
                    cwd: None,
                    timestamp: None,
                    name: Some(name.into()),
                })
            }
            _ => None,
        };

        if let Some(entry) = entry {
            entries.push(entry);
        }
    }
}

fn finish_pi_body(session_id: Option<SmolStr>, entries: Vec<Entry>) -> Result<Body> {
    trace!(
        target: "agent_sessions::parse",
        agent = Pi::name(),
        session_id = session_id.as_deref().unwrap_or("<none>"),
        entries = entries.len(),
        "parsed pi session"
    );

    Ok(Body {
        entries: entries.into_boxed_slice(),
    })
}

// ── Metadata probe ────────────────────────────────────────────────

#[cfg(feature = "agent_session")]
impl Pi {
    pub fn probe_session_meta<R>(mut reader: R) -> Result<Option<crate::agent_session::SessionMeta>>
    where
        R: BufRead,
    {
        let mut line = Vec::new();
        let mut meta: Option<crate::agent_session::SessionMeta> = None;
        let mut session_timestamp: Option<SmolStr> = None;
        let mut has_parent_session = false;

        loop {
            line.clear();
            let bytes_read = reader
                .read_until(b'\n', &mut line)
                .map_err(|err| Error::Message(err.to_string().into()))?;
            if bytes_read == 0 {
                return Ok(meta);
            }
            if line.iter().all(|b| b.is_ascii_whitespace()) {
                continue;
            }

            let raw: RawLine = match serde_json::from_slice(&line) {
                Ok(raw) => raw,
                Err(err) => {
                    if meta.is_some() {
                        continue;
                    }
                    return Err(err.into());
                }
            };

            if raw.entry_type == "session" && meta.is_none() {
                let created_at = raw.timestamp.map(|s| s.into());
                session_timestamp = created_at.clone();
                has_parent_session = raw.parent_session.is_some();
                meta = Some(crate::agent_session::SessionMeta {
                    session_id: raw.id.map(|s| s.into()),
                    cwd: raw.cwd.map(|s| s.into()),
                    title: raw.name.map(|s| s.into()),
                    created_at: crate::agent_session::smol_opt(created_at),
                    source_kind: Some("pi-v1".into()),
                    ..Default::default()
                });
                continue;
            }

            let Some(meta) = meta.as_mut() else {
                return Ok(None);
            };
            if meta.title.is_some() {
                return Ok(Some(meta.clone()));
            }
            if has_parent_session
                && raw
                    .timestamp
                    .as_deref()
                    .zip(session_timestamp.as_deref())
                    .is_some_and(|(timestamp, session_timestamp)| timestamp < session_timestamp)
            {
                continue;
            }
            if raw.entry_type != "message" {
                continue;
            }
            let Some(message) = raw.message.as_ref() else {
                continue;
            };
            if message.role != "user" {
                continue;
            }
            let Some(text) = message.content.as_ref().and_then(extract_msg_text) else {
                continue;
            };
            let title = first_line_snippet(&text, 80);
            if title.is_empty() || is_pi_instruction_preamble(&text) {
                continue;
            }
            meta.title = Some(title.into());
            return Ok(Some(meta.clone()));
        }
    }
}

// ── Helpers ────────────────────────────────────────────────────────

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

fn is_pi_instruction_preamble(text: &str) -> bool {
    let trimmed = text.trim_start();
    let first = trimmed.lines().next().unwrap_or("").trim();
    first.starts_with("# AGENTS.md instructions")
        || trimmed.starts_with("<INSTRUCTIONS>")
        || trimmed.contains("\n<INSTRUCTIONS>")
        || trimmed.starts_with("<permissions instructions>")
        || trimmed.starts_with("<environment_context>")
}

/// Extract human-readable text from a Pi message content field.
fn extract_msg_text(content: &serde_json::Value) -> Option<String> {
    match content {
        serde_json::Value::String(s) => {
            let s = s.trim();
            if s.is_empty() {
                None
            } else {
                Some(s.to_string())
            }
        }
        serde_json::Value::Array(blocks) => {
            let parts: Vec<&str> = blocks
                .iter()
                .filter_map(|b| {
                    let typ = b.get("type")?.as_str()?;
                    match typ {
                        "text" => b.get("text")?.as_str(),
                        _ => None,
                    }
                })
                .filter(|s| !s.is_empty())
                .collect();
            if parts.is_empty() {
                None
            } else {
                Some(parts.join(" "))
            }
        }
        _ => None,
    }
}

/// Extract token counts from usage. Prefers explicit token-count fields
/// over the shorter `input`/`output` aliases which may be missing.
fn extract_usage(raw: Option<&RawValue>) -> Result<(u64, u64, u64, u64, u64)> {
    let Some(raw) = raw else {
        return Ok((0, 0, 0, 0, 0));
    };
    let u: RawUsage = serde_json::from_str(raw.get())?;
    Ok((
        u.input,
        u.output,
        u.cache_read,
        u.cache_write,
        u.total_tokens,
    ))
}

/// Parse Pi content blocks (the `content` array inside a message) into
/// provider-specific `Block` variants.
fn parse_blocks(content: &serde_json::Value) -> Box<[Block]> {
    let Some(arr) = content.as_array() else {
        return Box::new([]);
    };
    arr.iter()
        .filter_map(|b| {
            let typ = b.get("type")?.as_str()?;
            match typ {
                "text" => Some(Block::Text {
                    text: b.get("text")?.as_str().unwrap_or("").into(),
                }),
                "thinking" => Some(Block::Thinking {
                    text: b.get("thinking")?.as_str().unwrap_or("").into(),
                }),
                "toolCall" | "toolUse" => {
                    let id = b.get("id").and_then(|v| v.as_str()).map(Into::into);
                    let name: SmolStr = b
                        .get("name")
                        .and_then(|v| v.as_str())
                        .unwrap_or("unknown")
                        .into();
                    let input_json = b.get("input").map(|v| v.to_string().into());
                    Some(Block::ToolCall {
                        id,
                        name,
                        input_json,
                    })
                }
                "tool_result" => {
                    let tool_use_id = b
                        .get("tool_use_id")
                        .and_then(|v| v.as_str())
                        .map(Into::into);
                    let text = extract_block_text(b.get("content"));
                    let is_error = b.get("is_error").and_then(|v| v.as_bool()).unwrap_or(false);
                    Some(Block::ToolResult {
                        tool_use_id,
                        text: text.into(),
                        is_error,
                    })
                }
                "image" => Some(Block::Image {
                    data: b.get("data").and_then(|v| v.as_str()).map(Into::into),
                    mime_type: b.get("mimeType").and_then(|v| v.as_str()).map(Into::into),
                }),
                other => Some(Block::Other {
                    block_type: other.into(),
                }),
            }
        })
        .collect::<Vec<_>>()
        .into_boxed_slice()
}

fn extract_block_text(content: Option<&serde_json::Value>) -> String {
    match content {
        Some(serde_json::Value::String(s)) => s.clone(),
        Some(serde_json::Value::Array(blocks)) => {
            let parts: Vec<&str> = blocks
                .iter()
                .filter_map(|b| b.get("text")?.as_str())
                .filter(|s| !s.is_empty())
                .collect();
            parts.join(" ")
        }
        _ => String::new(),
    }
}

#[cfg(all(test, feature = "agent_session"))]
mod tests {
    use std::io::Cursor;

    #[test]
    fn direct_agent_session_reader_parses_inline_jsonl() {
        let bytes = concat!(
            r#"{"type":"session","version":3,"id":"pi-direct","timestamp":"2026-05-03T00:00:00.000Z","cwd":"/tmp/project"}"#,
            "\n",
            r#"{"type":"message","id":"u1","timestamp":"2026-05-03T00:00:01.000Z","message":{"role":"user","content":[{"type":"text","text":"ping"}]}}"#,
            "\n",
            r#"{"type":"message","id":"a1","parentId":"u1","timestamp":"2026-05-03T00:00:15.000Z","message":{"role":"assistant","model":"deepseek-v4-pro","stopReason":"stop","content":[{"type":"text","text":"pong"}],"usage":{"input":10,"output":20,"totalTokens":30}}}"#,
            "\n",
        );
        let metadata = crate::InputMetadata::new().name("pi-session.jsonl");
        let selection = crate::ParseSelection::full();
        let direct = super::event::parse_direct_agent_session_reader_selected(
            Cursor::new(bytes),
            metadata,
            selection,
        )
        .unwrap();

        assert_eq!(direct.agent.as_str(), "pi");
        assert_eq!(direct.meta.session_id.as_deref(), Some("pi-direct"));
        assert!(!direct.events.is_empty());
    }

    #[test]
    fn reader_meta_only_stops_before_later_malformed_line() {
        let bytes = concat!(
            r#"{"type":"session","version":3,"id":"pi-meta","timestamp":"2026-05-03T00:00:00.000Z","cwd":"/tmp/project"}"#,
            "\n",
            "not-json",
        );

        let body =
            super::parse_pi_reader(Cursor::new(bytes), crate::ParseSelection::meta_only()).unwrap();

        let [
            super::Entry::SessionInfo {
                session_id, cwd, ..
            },
        ] = body.entries.as_ref()
        else {
            panic!("meta-only reader parse should return one session_info entry");
        };
        assert_eq!(session_id.as_str(), "pi-meta");
        assert_eq!(cwd.as_deref(), Some("/tmp/project"));
    }

    #[test]
    fn reader_accepts_escaped_windows_cwd_and_message_text() {
        let bytes = concat!(
            r#"{"type":"session","version":3,"id":"pi-win","timestamp":"2026-05-03T00:00:00.000Z","cwd":"C:\\Users\\alice\\project"}"#,
            "\n",
            r#"{"type":"message","id":"u1","timestamp":"2026-05-03T00:00:01.000Z","message":{"role":"user","content":[{"type":"text","text":"open C:\\Users\\alice\\project\nthen inspect"}]}}"#,
            "\n",
        );

        let body =
            super::parse_pi_reader(Cursor::new(bytes), crate::ParseSelection::full()).unwrap();

        let [
            super::Entry::SessionInfo { cwd, .. },
            super::Entry::UserMessage { text, .. },
        ] = body.entries.as_ref()
        else {
            panic!("expected session info and user message");
        };
        assert_eq!(cwd.as_deref(), Some(r#"C:\Users\alice\project"#));
        assert_eq!(
            text.as_str(),
            "open C:\\Users\\alice\\project\nthen inspect"
        );
    }

    #[test]
    fn probe_session_meta_uses_first_post_parent_session_user_text_as_title() {
        let mut lines = vec![
            r#"{"type":"session","version":3,"id":"child","timestamp":"2026-05-11T00:49:47.936Z","cwd":"/tmp/project","parentSession":"/tmp/parent.jsonl"}"#,
            r#"{"type":"message","id":"old","timestamp":"2026-05-10T11:59:25.404Z","message":{"role":"user","content":[{"type":"text","text":"review下项目架构 看看除了agent session还有哪里需要统一抽象的"}]}}"#,
        ];
        lines.extend(
            std::iter::repeat_n(
                r#"{"type":"message","id":"old-assistant","timestamp":"2026-05-10T12:00:00.000Z","message":{"role":"assistant","content":[{"type":"text","text":"inherited"}]}}"#,
                3,
            ),
        );
        lines.push(
            r#"{"type":"message","id":"new","timestamp":"2026-05-11T00:49:51.117Z","message":{"role":"user","content":[{"type":"text","text":"Task: You are a delegated subagent running from a fork of the parent session. Treat the inherited conversation as reference-only context, not a live thread to continue.\n\nTask:\nRead the design doc and implement the Claude parser."}]}}"#,
        );
        let bytes = lines.join("\n");

        let meta = super::Pi::probe_session_meta(Cursor::new(bytes.as_bytes()))
            .unwrap()
            .expect("expected session meta");

        assert_eq!(
            meta.title.as_deref(),
            Some(
                "Task: You are a delegated subagent running from a fork of the parent session. Tr…"
            )
        );
    }

    #[test]
    fn probe_session_meta_scans_full_inherited_parent_lines_for_title() {
        let mut lines = vec![
            r#"{"type":"session","version":3,"id":"child","timestamp":"2026-05-11T00:49:47.936Z","cwd":"/tmp/project","parentSession":"/tmp/parent.jsonl"}"#.to_string(),
        ];
        lines.extend((0..300).map(|idx| {
            format!(
                r#"{{"type":"message","id":"old-{idx}","timestamp":"2026-05-10T12:00:00.000Z","message":{{"role":"assistant","content":[{{"type":"text","text":"inherited"}}]}}}}"#
            )
        }));
        lines.push(
            r#"{"type":"message","id":"new","timestamp":"2026-05-11T00:49:51.117Z","message":{"role":"user","content":[{"type":"text","text":"Task: You are a delegated subagent running from a fork of the parent session. Treat the inherited conversation as reference-only context, not a live thread to continue."}]}}"#.to_string(),
        );
        lines.push("not-json-after-title".into());

        let meta = super::Pi::probe_session_meta(Cursor::new(lines.join("\n")))
            .expect("poison after title must not be read")
            .expect("expected session meta");

        assert_eq!(meta.session_id.as_deref(), Some("child"));
        assert_eq!(
            meta.title.as_deref(),
            Some(
                "Task: You are a delegated subagent running from a fork of the parent session. Tr…"
            )
        );
    }

    #[test]
    fn probe_session_meta_uses_first_user_without_parent_session() {
        let bytes = [
            r#"{"type":"session","version":3,"id":"plain","timestamp":"2026-05-11T00:49:47.936Z","cwd":"/tmp/project"}"#,
            r#"{"type":"message","id":"old","timestamp":"2026-05-10T11:59:25.404Z","message":{"role":"user","content":[{"type":"text","text":"plain first user title"}]}}"#,
            r#"{"type":"message","id":"new","timestamp":"2026-05-11T00:49:51.117Z","message":{"role":"user","content":[{"type":"text","text":"plain session title"}]}}"#,
        ]
        .join("\n");

        let meta = super::Pi::probe_session_meta(Cursor::new(bytes.as_bytes()))
            .unwrap()
            .expect("expected session meta");

        assert_eq!(meta.title.as_deref(), Some("plain first user title"));
    }
}
