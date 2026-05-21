//! Canonical event schema — every adapter translates vendor output into
//! these types. This is the single source of truth for "what an agent
//! said/did" in lucarne.
//!
//! Events are ordered by (Epoch, Seq). Epoch changes whenever the
//! backing process restarts; consumers detect an epoch change and
//! rebuild timeline state. Within one epoch, Seq is monotonic and
//! dense.
//!
//! The module is split along sub-domain boundaries for cohesion, but
//! every public name is re-exported at `crate::event::*` so callers
//! (and existing `use lucarne::event::{...}` imports) stay unchanged.

mod command;
mod core;
mod permission;
mod timeline;
mod tool;
mod usage;

pub use command::{CommandResultData, CommandResultPayload};
pub use core::{
    now_rfc3339, Attachment, Event, Kind, Payload, SessionClosed, SessionStarted, TurnCompleted,
    TurnFailed, TurnStarted,
};
pub use permission::{
    Decision, PermissionAnswer, PermissionQuestion, PermissionQuestionOption, PermissionRequest,
    PermissionResolved, PermissionResponse, Risk,
};
pub use timeline::{
    new_timeline_assistant, new_timeline_reasoning, new_timeline_tool_call,
    new_timeline_tool_result, new_timeline_user, AssistantMessage, CompactionBlock, PlanBlock,
    Reasoning, Timeline, TimelineItem, TimelineType, Todo, TodoBlock, ToolCallItem, ToolResult,
    ToolResultItem, UserMessage,
};
pub use tool::{
    edit_tool, read_tool, shell, tool_call, unknown_tool, write_tool, SubAgentCall, ToolCall,
};
pub use usage::{LogLine, ResumeHandle, Usage, UsageDelta};

// ——— shared skip_serializing_if helpers ———
//
// `serde(skip_serializing_if = "path::to::fn")` takes any path, so we
// expose these at the event-module level and reference them as
// `super::is_zero_u64` inside each submodule.

pub(crate) fn is_zero_u64(x: &u64) -> bool {
    *x == 0
}
pub(crate) fn is_zero_i32(x: &i32) -> bool {
    *x == 0
}
pub(crate) fn is_zero_i64(x: &i64) -> bool {
    *x == 0
}
pub(crate) fn is_zero_f64(x: &f64) -> bool {
    *x == 0.0
}
pub(crate) fn is_false(x: &bool) -> bool {
    !*x
}
