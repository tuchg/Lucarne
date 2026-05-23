use std::borrow::Cow;
use std::io::BufRead;

use serde::Deserialize;
use serde_json::value::RawValue;
use tracing::trace;

use crate::util::{box_str, cow_to_box};
use crate::{Error, ParseSelection, Result};
use smol_str::SmolStr;

mod types;
pub(crate) use types::*;

#[cfg(feature = "discovery")]
mod discovery;
#[cfg(feature = "agent_session")]
mod event;

pub struct Cursor;

impl Cursor {
    pub(crate) fn name() -> &'static str {
        "cursor"
    }
}

fn cursor_session_id_from_name(name: Option<&str>) -> Option<SmolStr> {
    name.and_then(|name| {
        std::path::Path::new(name)
            .file_stem()
            .and_then(|stem| stem.to_str())
            .map(box_str)
    })
}

#[cfg(any(test, feature = "watch"))]
fn parse_cursor_reader<R>(
    mut reader: R,
    session_id: Option<SmolStr>,
    selection: ParseSelection,
) -> Result<Body>
where
    R: BufRead,
{
    parse_cursor_body_reader(&mut reader, session_id, selection)
}

pub(super) fn parse_cursor_body_reader<R>(
    mut reader: R,
    session_id: Option<SmolStr>,
    selection: ParseSelection,
) -> Result<Body>
where
    R: BufRead,
{
    if selection.is_meta_only() || !selection.includes_messages() {
        return Ok(Body {
            session_id,
            entries: Vec::new().into_boxed_slice(),
        });
    }

    let mut entries = Vec::new();
    let mut line = Vec::new();

    while read_next_nonempty_line(&mut reader, &mut line)? {
        let raw: RawEntry<'_> = serde_json::from_slice(&line)?;
        let role = match raw.role {
            Some("user") => Role::User,
            Some("assistant") => Role::Assistant,
            Some(other) => Role::Other(box_str(other)),
            None => continue,
        };
        let message = match raw.message {
            Some(message) => message,
            None => continue,
        };
        let blocks = map_cursor_content(message.content)?;
        if blocks.is_empty() {
            continue;
        }
        entries.push(Entry {
            role,
            timestamp: raw.timestamp.as_deref().map(box_str),
            blocks,
        });
    }

    if entries.is_empty() && selection.is_full() {
        return Err(Error::Detection {
            agent: Cursor::name(),
        });
    }

    trace!(
        target: "agent_sessions::parse",
        agent = Cursor::name(),
        version = ?Version::V1,
        entries = entries.len(),
        "parsed Cursor bundle"
    );
    Ok(Body {
        session_id,
        entries: entries.into_boxed_slice(),
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

fn map_cursor_content(content: &RawValue) -> Result<Box<[ContentBlock]>> {
    match content
        .get()
        .as_bytes()
        .iter()
        .copied()
        .find(|byte| !byte.is_ascii_whitespace())
    {
        Some(b'"') => {
            let text = serde_json::from_str::<Cow<'_, str>>(content.get())?;
            return Ok(vec![ContentBlock::Text(TextBlock {
                text: cow_to_box(text),
            })]
            .into_boxed_slice());
        }
        Some(b'[') => {}
        _ => return Ok(Vec::new().into_boxed_slice()),
    }

    let blocks: Vec<CursorBlock<'_>> = serde_json::from_str(content.get())?;
    Ok(blocks
        .into_iter()
        .map(|block| match block.kind {
            "text" => ContentBlock::Text(TextBlock {
                text: cow_to_box(block.text.unwrap_or(Cow::Borrowed(""))),
            }),
            "tool_use" => ContentBlock::ToolUse(ToolUseBlock {
                id: block.id.map(cow_to_box),
                name: cow_to_box(block.name.unwrap_or(Cow::Borrowed("unknown"))),
                input_json: block.input,
            }),
            "tool_result" => ContentBlock::ToolResult(ToolResultBlock {
                tool_use_id: block.tool_use_id.map(cow_to_box),
                content: block
                    .content
                    .map(result_content)
                    .unwrap_or_else(|| box_str("")),
                is_error: block.is_error.unwrap_or(false),
            }),
            other => ContentBlock::Raw(RawBlock {
                kind: box_str(other),
                raw_json: block.raw.unwrap_or_else(|| box_str("{}")),
            }),
        })
        .collect::<Vec<_>>()
        .into_boxed_slice())
}

#[derive(Deserialize)]
struct RawEntry<'a> {
    #[serde(default)]
    role: Option<&'a str>,
    #[serde(default, borrow)]
    timestamp: Option<Cow<'a, str>>,
    #[serde(default)]
    message: Option<CursorMessage<'a>>,
}

#[derive(Deserialize)]
struct CursorMessage<'a> {
    #[serde(borrow)]
    content: &'a RawValue,
}

#[derive(Deserialize)]
struct CursorBlock<'a> {
    #[serde(rename = "type")]
    kind: &'a str,
    #[serde(default, borrow)]
    id: Option<Cow<'a, str>>,
    #[serde(default, borrow)]
    tool_use_id: Option<Cow<'a, str>>,
    #[serde(default, borrow)]
    text: Option<Cow<'a, str>>,
    #[serde(default, borrow)]
    name: Option<Cow<'a, str>>,
    #[serde(default, deserialize_with = "deserialize_opt_raw_json_box")]
    input: Option<SmolStr>,
    #[serde(default, deserialize_with = "deserialize_opt_raw_json_box")]
    content: Option<SmolStr>,
    #[serde(default)]
    is_error: Option<bool>,
    #[serde(default, deserialize_with = "deserialize_opt_raw_json_box")]
    raw: Option<SmolStr>,
}

fn result_content(raw: SmolStr) -> SmolStr {
    serde_json::from_str::<Cow<'_, str>>(&raw)
        .map(cow_to_box)
        .unwrap_or(raw)
}

fn deserialize_opt_raw_json_box<'de, D>(
    deserializer: D,
) -> std::result::Result<Option<SmolStr>, D::Error>
where
    D: serde::Deserializer<'de>,
{
    let raw = Option::<&RawValue>::deserialize(deserializer)?;
    Ok(raw.map(|raw| box_str(raw.get())))
}

#[cfg(test)]
mod tests {
    use std::io::Cursor as IoCursor;

    #[test]
    fn reader_meta_only_stops_before_later_malformed_line() {
        let bytes = "not-json";

        let body = super::parse_cursor_reader(
            IoCursor::new(bytes),
            Some("cursor-meta".into()),
            crate::ParseSelection::meta_only(),
        )
        .unwrap();

        assert_eq!(body.session_id.as_deref(), Some("cursor-meta"));
        assert!(body.entries.is_empty());
    }

    #[test]
    fn reader_accepts_escaped_windows_message_text() {
        let bytes = concat!(
            r#"{"role":"user","timestamp":"2026-05-01T11:09:46.305Z","message":{"content":"open C:\\Users\\alice\\project\nthen inspect"}}"#,
            "\n",
        );

        let body = super::parse_cursor_reader(
            IoCursor::new(bytes.as_bytes()),
            Some("cursor-win".into()),
            crate::ParseSelection::full(),
        )
        .unwrap();

        let [entry] = body.entries.as_ref() else {
            panic!("expected one cursor entry");
        };
        let [super::ContentBlock::Text(text)] = entry.blocks.as_ref() else {
            panic!("expected one text block");
        };
        assert_eq!(
            text.text.as_str(),
            "open C:\\Users\\alice\\project\nthen inspect"
        );
    }

    #[cfg(feature = "agent_session")]
    #[test]
    fn direct_agent_session_reader_parses_current_fixture() {
        let bytes = include_bytes!("../../../tests/fixtures/cursor/transcript.jsonl").as_slice();
        let metadata = crate::InputMetadata::new().name("cursor-session.jsonl");
        let selection = crate::ParseSelection::full();
        let direct = super::event::parse_direct_agent_session_reader_selected(
            IoCursor::new(bytes),
            metadata,
            selection,
        )
        .unwrap();

        assert_eq!(direct.agent.as_str(), "cursor");
        assert!(!direct.events.is_empty());
    }
}
