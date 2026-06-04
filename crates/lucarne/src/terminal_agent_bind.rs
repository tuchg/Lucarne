//! Terminal-adjacent agent binding.
//!
//! This module binds a terminal pane cwd to the agent session running inside it
//! and reads that provider-owned transcript as chat messages. It deliberately
//! lives in `lucarne` core, not a sidecar crate: the binding is product context
//! between terminal entrypoints and Lucarne provider/session state.
//!
//! Provider-specific discovery, metadata and parsing rules still stay inside
//! `agent-sessions` descriptors. This module only orchestrates through those
//! typed provider contracts and persists rmux-related binding observations in
//! [`crate::control_plane::ControlPlaneSqliteStore`] as cold control-plane rows.

use std::path::{Path, PathBuf};

use agent_sessions::agent_session::{Actor, Body, ContentBlock, Session, SessionMeta};
use agent_sessions::reader::SessionReader;
use agent_sessions::{agent_providers, AgentProviderDescriptor, ParseSelection};
use serde::{Deserialize, Serialize};
use tracing::{debug, warn};

use crate::control_plane::{ControlPlaneSqliteStore, ControlPlaneStoreError, ProviderSessionId};

const TERMINAL_AGENT_BINDING_KIND: &str = "terminal_agent_binding";
const INITIAL_TRANSCRIPT_WINDOW_BYTES: u64 = 256 * 1024;
const INCREMENTAL_TRANSCRIPT_READ_BYTES: u64 = 256 * 1024;

/// The agent session a pane is bound to.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct BoundAgent {
    /// Provider id, owned by the provider descriptor.
    pub kind: String,
    /// The provider's own session id.
    pub session_id: String,
    /// Path to the provider-owned transcript file.
    pub transcript: PathBuf,
}

/// One parsed conversation turn.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Msg {
    pub role: String,
    pub text: String,
}

/// Cold record that a provider session was observed from a terminal pane.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct TerminalAgentBindingRecord {
    pub provider_session_id: ProviderSessionId,
    pub kind: String,
    pub session_id: String,
    pub cwd: Option<String>,
    pub rmux_session: Option<String>,
    pub title: Option<String>,
    pub summary: Option<String>,
    pub transcript: PathBuf,
    pub first_seen: i64,
    pub last_seen: i64,
}

/// Most-recently-seen rmux-related agent session row.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct HistRow {
    pub kind: String,
    pub session_id: String,
    pub cwd: Option<String>,
    pub rmux_session: Option<String>,
    pub title: Option<String>,
    pub summary: Option<String>,
    pub last_seen: i64,
}

/// Resolve the agent session bound to a pane with the given cwd, if any.
pub fn bind(cwd: &str) -> Option<BoundAgent> {
    let mut best: Option<(i64, BoundAgent)> = None;
    for provider in agent_providers() {
        let _ = provider.discover_sources_into(&mut |source| {
            let Ok(meta) = provider.parse_source_meta(&source) else {
                return;
            };
            if !meta_cwd_matches(&meta, cwd) {
                return;
            }
            let modified = source.last_modified_unix();
            if best.as_ref().is_some_and(|(best_m, _)| modified <= *best_m) {
                return;
            }
            let session_id = meta
                .session_id
                .as_deref()
                .map(str::to_owned)
                .or_else(|| {
                    source
                        .path()
                        .file_stem()
                        .map(|stem| stem.to_string_lossy().into_owned())
                })
                .unwrap_or_default();
            best = Some((
                modified,
                BoundAgent {
                    kind: provider.id().to_string(),
                    session_id,
                    transcript: source.path().to_path_buf(),
                },
            ));
        });
    }
    let bound = best.map(|(_, agent)| agent);
    match &bound {
        Some(agent) => debug!(
            target: "lucarne::terminal_agent_bind",
            cwd,
            provider = %agent.kind,
            provider_session_id = %agent.session_id,
            transcript = %agent.transcript.display(),
            "bound terminal cwd to provider transcript"
        ),
        None => debug!(
            target: "lucarne::terminal_agent_bind",
            cwd,
            "no provider transcript matched terminal cwd"
        ),
    }
    bound
}

fn meta_cwd_matches(meta: &SessionMeta, cwd: &str) -> bool {
    meta.cwd.as_deref() == Some(cwd)
}

/// Read transcript messages appended since byte `from`.
///
/// Only complete newline-terminated lines are consumed; a partial trailing line
/// is left for the next read. Provider schema parsing is delegated to the
/// descriptor that owns the transcript.
///
/// `from == 0` is treated as an initial/history read and starts from a bounded
/// tail window, not from byte zero. Terminal transcript files are provider-owned
/// append-only logs and can grow indefinitely; terminal-adjacent views should
/// bootstrap from a recent cursor window instead of scanning the full file.
pub fn read_messages(path: &Path, from: u64) -> (Vec<Msg>, u64) {
    let Some((bytes, consumed)) = read_complete_lines_from(path, from) else {
        warn!(
            target: "lucarne::terminal_agent_bind",
            transcript = %path.display(),
            offset = from,
            "failed to read terminal-bound transcript"
        );
        return (Vec::new(), from);
    };
    if bytes.is_empty() {
        return (Vec::new(), consumed);
    }
    let Some(provider) = provider_for(path) else {
        return (Vec::new(), consumed);
    };
    match provider.parse_agent_session_bytes(bytes, ParseSelection::empty().with_messages()) {
        Ok(session) => {
            let messages = messages_from_session(provider, &session);
            debug!(
                target: "lucarne::terminal_agent_bind",
                transcript = %path.display(),
                from,
                consumed,
                messages = messages.len(),
                "read terminal-bound transcript messages"
            );
            (messages, consumed)
        }
        Err(err) => {
            warn!(
                target: "lucarne::terminal_agent_bind",
                transcript = %path.display(),
                from,
                consumed,
                error = %err,
                "failed to parse terminal-bound transcript messages"
            );
            (Vec::new(), consumed)
        }
    }
}

fn read_complete_lines_from(path: &Path, from: u64) -> Option<(Vec<u8>, u64)> {
    use std::io::{Read, Seek, SeekFrom};

    let mut file = std::fs::File::open(path).ok()?;
    let len = file.metadata().map(|m| m.len()).unwrap_or(0);
    if len <= from && from != 0 {
        return Some((Vec::new(), from));
    }
    let start = read_start_for_cursor(&mut file, len, from)?;
    if len <= start {
        return Some((Vec::new(), start));
    }
    file.seek(SeekFrom::Start(start)).ok()?;
    let mut buf = Vec::new();
    let limit = if from == 0 {
        INITIAL_TRANSCRIPT_WINDOW_BYTES
    } else {
        INCREMENTAL_TRANSCRIPT_READ_BYTES
    };
    file.take(limit).read_to_end(&mut buf).ok()?;

    let mut consumed_start = start;
    if from == 0 && start > 0 {
        let Some(first_newline) = buf.iter().position(|byte| *byte == b'\n') else {
            return Some((Vec::new(), start));
        };
        buf.drain(..=first_newline);
        consumed_start += first_newline as u64 + 1;
    }

    let consumed_len = match buf.iter().rposition(|byte| *byte == b'\n') {
        Some(last_newline) => last_newline + 1,
        None => return Some((Vec::new(), consumed_start)),
    };
    buf.truncate(consumed_len);
    Some((buf, consumed_start + consumed_len as u64))
}

fn read_start_for_cursor(file: &mut std::fs::File, len: u64, from: u64) -> Option<u64> {
    if from > 0 {
        return Some(from.min(len));
    }
    if len <= INITIAL_TRANSCRIPT_WINDOW_BYTES {
        return Some(0);
    }
    let start = len.saturating_sub(INITIAL_TRANSCRIPT_WINDOW_BYTES);
    std::io::Seek::seek(file, std::io::SeekFrom::Start(start)).ok()?;
    Some(start)
}

fn messages_from_session(provider: AgentProviderDescriptor, session: &Session) -> Vec<Msg> {
    let mut msgs = Vec::new();
    for event in session.events.iter() {
        let msg = match (&event.actor, &event.body) {
            (Actor::User, Body::Prompt(prompt)) => {
                let text = text_of(prompt.text.as_deref(), &prompt.blocks);
                if text.is_empty() || !provider.is_transcript_user_text_visible(&text) {
                    continue;
                }
                Msg {
                    role: "user".to_string(),
                    text,
                }
            }
            (Actor::Assistant, Body::Response(response)) => {
                let text = text_of(response.text.as_deref(), &response.blocks);
                if text.is_empty() {
                    continue;
                }
                Msg {
                    role: "assistant".to_string(),
                    text,
                }
            }
            _ => continue,
        };
        msgs.push(msg);
    }
    msgs
}

fn text_of(inline: Option<&str>, blocks: &[ContentBlock]) -> String {
    if let Some(text) = inline.map(str::trim).filter(|text| !text.is_empty()) {
        return text.to_string();
    }
    agent_sessions::agent_session::text_from_blocks(blocks)
        .map(|text| text.trim().to_string())
        .unwrap_or_default()
}

fn provider_for(path: &Path) -> Option<AgentProviderDescriptor> {
    agent_providers()
        .into_iter()
        .find(|provider| provider.parse_file_meta(path.to_path_buf()).is_ok())
}

/// Short title for a transcript: provider title or first visible user message.
pub fn first_user_message(path: &Path) -> Option<String> {
    let provider = provider_for(path)?;
    if let Ok(meta) = provider.parse_file_meta(path.to_path_buf()) {
        if let Some(title) = meta
            .title
            .as_deref()
            .map(str::trim)
            .filter(|t| !t.is_empty())
        {
            return Some(title.chars().take(60).collect());
        }
    }
    let bytes = read_tail_window(path)?;
    let session = provider
        .parse_agent_session_bytes(bytes, ParseSelection::empty().with_messages())
        .ok()?;
    session
        .events
        .iter()
        .find_map(|event| match (&event.actor, &event.body) {
            (Actor::User, Body::Prompt(prompt)) => {
                let text = text_of(prompt.text.as_deref(), &prompt.blocks);
                if text.is_empty() || !provider.is_transcript_user_text_visible(&text) {
                    return None;
                }
                let first = text.lines().find(|l| !l.trim().is_empty()).unwrap_or("");
                let s: String = first.chars().take(60).collect();
                (!s.trim().is_empty()).then_some(s)
            }
            _ => None,
        })
}

fn read_tail_window(path: &Path) -> Option<Vec<u8>> {
    const MAX_HEAD_BYTES: u64 = 256 * 1024;
    let reader = SessionReader::open(path).ok()?;
    let mut lines = reader.reverse_lines_limited(MAX_HEAD_BYTES).ok()?;
    let mut collected = Vec::new();
    while let Some(line) = lines.next_line().ok()? {
        collected.push(line);
    }
    if collected.is_empty() {
        return None;
    }
    let mut bytes = Vec::new();
    for line in collected.iter().rev() {
        bytes.extend_from_slice(line);
        bytes.push(b'\n');
    }
    Some(bytes)
}

fn now() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_secs() as i64)
        .unwrap_or(0)
}

fn provider_session_id(kind: &str, session_id: &str) -> ProviderSessionId {
    ProviderSessionId::new(format!("{kind}:{session_id}"))
}

/// Record that `session_id` was seen bound to an rmux pane.
pub fn record(
    store: &ControlPlaneSqliteStore,
    kind: &str,
    session_id: &str,
    cwd: &str,
    rmux_session: &str,
    title: &str,
    transcript: &Path,
) -> Result<(), ControlPlaneStoreError> {
    let provider_session_id = provider_session_id(kind, session_id);
    let existing = terminal_agent_binding(store, &provider_session_id)?;
    let ts = now();
    let record = TerminalAgentBindingRecord {
        provider_session_id: provider_session_id.clone(),
        kind: kind.to_string(),
        session_id: session_id.to_string(),
        cwd: Some(cwd.to_string()),
        rmux_session: Some(rmux_session.to_string()),
        title: Some(title.to_string()),
        summary: first_user_message(transcript),
        transcript: transcript.to_path_buf(),
        first_seen: existing.as_ref().map(|r| r.first_seen).unwrap_or(ts),
        last_seen: ts,
    };
    let result = store.upsert_entity_state(
        TERMINAL_AGENT_BINDING_KIND,
        provider_session_id.as_str(),
        None,
        &record,
    );
    match &result {
        Ok(()) => debug!(
            target: "lucarne::terminal_agent_bind",
            provider = kind,
            provider_session_id = provider_session_id.as_str(),
            rmux_session,
            cwd,
            transcript = %transcript.display(),
            "recorded terminal-agent binding"
        ),
        Err(err) => warn!(
            target: "lucarne::terminal_agent_bind",
            provider = kind,
            provider_session_id = provider_session_id.as_str(),
            rmux_session,
            cwd,
            transcript = %transcript.display(),
            error = %err,
            "failed to record terminal-agent binding"
        ),
    }
    result
}

/// Most-recently-seen terminal-related agent sessions.
pub fn history(
    store: &ControlPlaneSqliteStore,
    limit: usize,
) -> Result<Vec<HistRow>, ControlPlaneStoreError> {
    let mut rows = terminal_agent_bindings(store)?
        .into_iter()
        .map(|r| HistRow {
            kind: r.kind,
            session_id: r.session_id,
            cwd: r.cwd,
            rmux_session: r.rmux_session,
            title: r.title,
            summary: r.summary,
            last_seen: r.last_seen,
        })
        .collect::<Vec<_>>();
    rows.sort_by(|a, b| b.last_seen.cmp(&a.last_seen));
    rows.truncate(limit);
    Ok(rows)
}

pub fn transcript_path(
    store: &ControlPlaneSqliteStore,
    session_id: &str,
) -> Result<Option<PathBuf>, ControlPlaneStoreError> {
    let records = terminal_agent_bindings(store)?;
    Ok(records
        .into_iter()
        .find(|record| record.session_id == session_id)
        .map(|record| record.transcript))
}

pub fn terminal_agent_binding(
    store: &ControlPlaneSqliteStore,
    provider_session_id: &ProviderSessionId,
) -> Result<Option<TerminalAgentBindingRecord>, ControlPlaneStoreError> {
    store.entity_state(TERMINAL_AGENT_BINDING_KIND, provider_session_id.as_str())
}

pub fn terminal_agent_bindings(
    store: &ControlPlaneSqliteStore,
) -> Result<Vec<TerminalAgentBindingRecord>, ControlPlaneStoreError> {
    store.entities_by_kind(TERMINAL_AGENT_BINDING_KIND)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Write;

    fn claude_session_line(cwd: &str, session_id: &str) -> String {
        format!(
            r#"{{"type":"user","sessionId":"{session_id}","cwd":"{cwd}","timestamp":"2026-05-30T00:00:00Z","message":{{"role":"user","content":[{{"type":"text","text":"hello there"}}]}}}}"#
        )
    }

    fn claude_assistant_line() -> String {
        r#"{"type":"assistant","message":{"role":"assistant","content":[{"type":"text","text":"hi back"}],"stop_reason":"end_turn"}}"#.to_string()
    }

    #[test]
    fn read_messages_projects_user_and_assistant_via_provider() {
        let mut f = tempfile::Builder::new()
            .suffix(".jsonl")
            .tempfile()
            .unwrap();
        writeln!(f, "{}", claude_session_line("/tmp/x", "sess-1")).unwrap();
        writeln!(f, "{}", claude_assistant_line()).unwrap();
        f.flush().unwrap();

        let (msgs, off) = read_messages(f.path(), 0);
        assert!(off > 0, "consumed offset advances past complete lines");
        assert_eq!(msgs.len(), 2, "one user + one assistant bubble");
        assert_eq!(msgs[0].role, "user");
        assert_eq!(msgs[0].text, "hello there");
        assert_eq!(msgs[1].role, "assistant");
        assert_eq!(msgs[1].text, "hi back");
    }

    #[test]
    fn read_messages_leaves_partial_trailing_line_unconsumed() {
        let mut f = tempfile::Builder::new()
            .suffix(".jsonl")
            .tempfile()
            .unwrap();
        let l1 = format!("{}\n", claude_session_line("/tmp/x", "sess-1"));
        f.write_all(l1.as_bytes()).unwrap();
        f.flush().unwrap();
        let (msgs, off) = read_messages(f.path(), 0);
        assert_eq!(msgs.len(), 1);
        assert_eq!(off, l1.len() as u64);

        let l2 = format!("{}\n", claude_assistant_line());
        let partial = r#"{"type":"user","message":{"rol"#;
        f.write_all(l2.as_bytes()).unwrap();
        f.write_all(partial.as_bytes()).unwrap();
        f.flush().unwrap();
        let (msgs2, off2) = read_messages(f.path(), off);
        assert_eq!(msgs2.len(), 1);
        assert_eq!(msgs2[0].text, "hi back");
        assert_eq!(off2, off + l2.len() as u64);
    }

    #[test]
    fn initial_read_uses_bounded_tail_window() {
        let mut f = tempfile::Builder::new()
            .suffix(".jsonl")
            .tempfile()
            .unwrap();
        writeln!(f, "{}", claude_session_line("/tmp/x", "sess-old")).unwrap();
        let filler = "x".repeat((INITIAL_TRANSCRIPT_WINDOW_BYTES as usize) + 1024);
        writeln!(f, "{filler}").unwrap();
        writeln!(f, "{}", claude_session_line("/tmp/x", "sess-new")).unwrap();
        f.flush().unwrap();

        let (msgs, off) = read_messages(f.path(), 0);
        assert!(off > INITIAL_TRANSCRIPT_WINDOW_BYTES);
        assert_eq!(msgs.len(), 1);
        assert_eq!(
            msgs[0].text, "hello there",
            "initial load should parse the recent complete transcript line"
        );
    }

    #[test]
    fn first_user_message_uses_provider_parsed_title_or_first_user_text() {
        let mut f = tempfile::Builder::new()
            .suffix(".jsonl")
            .tempfile()
            .unwrap();
        writeln!(f, "{}", claude_session_line("/tmp/x", "sess-1")).unwrap();
        writeln!(f, "{}", claude_assistant_line()).unwrap();
        f.flush().unwrap();
        let summary = first_user_message(f.path()).expect("a summary line");
        assert!(summary.contains("hello there"), "summary was: {summary}");
    }

    #[test]
    fn bind_matches_pane_cwd_to_provider_parsed_session() {
        let temp = tempfile::tempdir().unwrap();
        let projects = temp.path().join("projects").join("-tmp-proj");
        std::fs::create_dir_all(&projects).unwrap();
        let cwd = "/tmp/proj-bind-test";
        std::fs::write(
            projects.join("sess-bind.jsonl"),
            format!(
                "{}\n{}\n",
                claude_session_line(cwd, "sess-bind"),
                claude_assistant_line()
            ),
        )
        .unwrap();

        let prev = std::env::var_os("CLAUDE_CONFIG_DIR");
        unsafe {
            std::env::set_var("CLAUDE_CONFIG_DIR", temp.path());
        }
        let bound = bind(cwd);
        match prev {
            Some(v) => unsafe { std::env::set_var("CLAUDE_CONFIG_DIR", v) },
            None => unsafe { std::env::remove_var("CLAUDE_CONFIG_DIR") },
        }

        let bound = bound.expect("bind resolves the session for the matching cwd");
        assert_eq!(bound.kind, "claude");
        assert_eq!(bound.session_id, "sess-bind");
        assert!(bound.transcript.ends_with("sess-bind.jsonl"));
    }

    #[test]
    fn terminal_agent_binding_uses_control_plane_store() {
        let store = ControlPlaneSqliteStore::open_in_memory().expect("store");
        let mut f = tempfile::Builder::new()
            .suffix(".jsonl")
            .tempfile()
            .unwrap();
        writeln!(f, "{}", claude_session_line("/tmp/x", "sess-store")).unwrap();
        f.flush().unwrap();

        record(
            &store,
            "claude",
            "sess-store",
            "/tmp/x",
            "rmux-a",
            "terminal",
            f.path(),
        )
        .expect("record binding");

        let history = history(&store, 10).expect("history");
        assert_eq!(history.len(), 1);
        assert_eq!(history[0].session_id, "sess-store");
        assert_eq!(history[0].rmux_session.as_deref(), Some("rmux-a"));
        assert_eq!(
            transcript_path(&store, "sess-store")
                .expect("transcript lookup")
                .as_deref(),
            Some(f.path())
        );
    }
}
