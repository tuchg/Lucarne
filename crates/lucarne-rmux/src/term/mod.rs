//! Terminal domain types used by the rmux-backed terminal subsystem.
//!
//! These types used to live in the small `lucarne-term` crate. They now live
//! with the rmux terminal capability so the fork has one terminal package:
//! domain vocabulary, archive helpers, SDK adapter, and monitor.

pub mod diff;
pub mod grid;
pub mod input;
pub mod registry;
pub mod wire;

pub use diff::{diff, DiffResult, Differ};
pub use grid::{Cell, CellSpan, Color, Cursor, Dims, GridDelta, PaneGrid, RowDelta, Style};
pub use input::{control_key_token, key_token, ControlKey, KeyMods, TermInput};
pub use registry::{
    primary_pane_session_id, split_primary_pane_session_id, validate_primary_pane_session_id,
    Origin, PaneSelectionError, SessionDescriptor, SessionId, SessionRegistry,
};
pub use wire::{ClientFrame, ServerFrame};
