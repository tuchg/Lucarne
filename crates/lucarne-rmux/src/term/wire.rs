//! wire — the tagged Server/Client frames exchanged with web mirror clients.

use serde::{Deserialize, Serialize};

use crate::term::grid::{Cursor, GridDelta, PaneGrid};
use crate::term::input::TermInput;
use crate::term::registry::{SessionDescriptor, SessionId};

/// Server → Client frames. Tagged by `type` in `snake_case`.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum ServerFrame {
    /// The monitored session list (sent on connect & after create/close).
    SessionList {
        sessions: Vec<SessionDescriptor>,
    },
    /// Full grid — on subscribe, on resize, or as a delta-gap resync fallback.
    Snapshot {
        session: SessionId,
        grid: PaneGrid,
        cursor: Cursor,
    },
    /// Incremental update — the hot path (dirty-row runs only).
    SnapshotDelta {
        session: SessionId,
        base_rev: u64,
        rev: u64,
        delta: GridDelta,
        cursor: Cursor,
    },
    /// A session was created in response to `ClientFrame::CreateSession`.
    SessionCreated {
        session: SessionId,
    },
    /// A session was closed (via `CloseSession` or it exited).
    SessionClosed {
        session: SessionId,
    },
    Error {
        code: u16,
        msg: String,
    },
    Pong {
        t: u64,
    },
}

/// Client → Server frames. Tagged by `type` in `snake_case`.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum ClientFrame {
    Subscribe {
        session: SessionId,
    },
    Detach {
        session: SessionId,
    },
    Input {
        session: SessionId,
        event: TermInput,
    },
    Resync {
        session: SessionId,
        have_rev: u64,
    },
    CreateSession {
        title: Option<String>,
    },
    CloseSession {
        session: SessionId,
    },
    Ping {
        t: u64,
    },
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::term::grid::{Cursor, PaneGrid};

    #[test]
    fn server_snapshot_round_trips_tagged() {
        let f = ServerFrame::Snapshot {
            session: "s:0:0".into(),
            grid: PaneGrid {
                cols: 1,
                rows: 1,
                cells: vec![],
                rev: 3,
            },
            cursor: Cursor {
                row: 0,
                col: 0,
                visible: true,
                style_raw: 0,
            },
        };
        let json = serde_json::to_string(&f).unwrap();
        assert!(json.contains("\"type\":\"snapshot\""));
        let back: ServerFrame = serde_json::from_str(&json).unwrap();
        assert_eq!(back, f);
    }

    #[test]
    fn client_input_round_trips_tagged() {
        let f = ClientFrame::Input {
            session: "s:0:0".into(),
            event: TermInput::Text {
                text: "ls\n".into(),
            },
        };
        let back: ClientFrame = serde_json::from_str(&serde_json::to_string(&f).unwrap()).unwrap();
        assert_eq!(back, f);
    }
}
