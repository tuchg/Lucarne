//! Core event envelope: `Event`, `Kind`, `Payload`, and the lifecycle
//! payloads (session started/closed, turn started/completed/failed).

use serde::{Deserialize, Serialize};
use smol_str::SmolStr;
use std::time::{SystemTime, UNIX_EPOCH};

use super::command::CommandResultPayload;
use super::permission::{PermissionRequest, PermissionResolved};
use super::timeline::Timeline;
use super::usage::{LogLine, ResumeHandle, Usage, UsageDelta};

/// The atomic unit of the output stream. `seq` / `epoch` are stamped by
/// the journal / runtime; dialects leave them empty.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Event {
    #[serde(default, skip_serializing_if = "super::is_zero_u64")]
    pub seq: u64,
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub epoch: String,
    pub ts: String,
    #[serde(flatten)]
    pub payload: Payload,
}

impl Event {
    pub fn new(payload: Payload) -> Self {
        Self {
            seq: 0,
            epoch: String::new(),
            ts: String::new(),
            payload,
        }
    }
    pub fn kind(&self) -> Kind {
        self.payload.kind()
    }
}

/// `Kind` is the discriminator for event payloads.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Kind {
    SessionStarted,
    TurnStarted,
    TurnCompleted,
    TurnFailed,
    SessionClosed,
    Timeline,
    PermissionRequest,
    PermissionResolved,
    Log,
    UsageDelta,
    CommandResult,
    Attachment,
}

/// Payload variants. Matches the Go `Event.Kind + Payload` pair via
/// `#[serde(tag = "kind", content = "payload")]` so JSON shape on the
/// wire is `{"kind":"log","payload":{...}}`.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "kind", content = "payload", rename_all = "snake_case")]
pub enum Payload {
    SessionStarted(SessionStarted),
    TurnStarted(TurnStarted),
    TurnCompleted(TurnCompleted),
    TurnFailed(TurnFailed),
    SessionClosed(SessionClosed),
    Timeline(Timeline),
    PermissionRequest(PermissionRequest),
    PermissionResolved(PermissionResolved),
    Log(LogLine),
    UsageDelta(UsageDelta),
    CommandResult(CommandResultPayload),
    Attachment(Attachment),
}

impl Payload {
    pub fn kind(&self) -> Kind {
        match self {
            Self::SessionStarted(_) => Kind::SessionStarted,
            Self::TurnStarted(_) => Kind::TurnStarted,
            Self::TurnCompleted(_) => Kind::TurnCompleted,
            Self::TurnFailed(_) => Kind::TurnFailed,
            Self::SessionClosed(_) => Kind::SessionClosed,
            Self::Timeline(_) => Kind::Timeline,
            Self::PermissionRequest(_) => Kind::PermissionRequest,
            Self::PermissionResolved(_) => Kind::PermissionResolved,
            Self::Log(_) => Kind::Log,
            Self::UsageDelta(_) => Kind::UsageDelta,
            Self::CommandResult(_) => Kind::CommandResult,
            Self::Attachment(_) => Kind::Attachment,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Attachment {
    pub id: SmolStr,
    pub filename: SmolStr,
    pub media_type: SmolStr,
    pub data_base64: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub caption: Option<SmolStr>,
}

// ——— Lifecycle ———

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct SessionStarted {
    pub session_id: String,
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub model: String,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct TurnStarted {
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub turn_id: String,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct TurnCompleted {
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub turn_id: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub usage: Option<Usage>,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct TurnFailed {
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub turn_id: String,
    pub error: String,
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub code: String,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct SessionClosed {
    pub reason: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub resume: Option<ResumeHandle>,
}

// ——— Time helper ———

pub fn now_rfc3339() -> String {
    // Minimal RFC3339Nano formatter without extra deps. Produces
    // `2006-01-02T15:04:05.xxxxxxxxxZ`.
    let d = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default();
    let secs = d.as_secs() as i64;
    let nanos = d.subsec_nanos();
    let (y, mo, da, h, mi, se) = civil_from_days(secs);
    format!(
        "{:04}-{:02}-{:02}T{:02}:{:02}:{:02}.{:09}Z",
        y, mo, da, h, mi, se, nanos
    )
}

/// Break a unix-seconds value into civil (Y,M,D,h,m,s) using the
/// Howard Hinnant algorithm — avoids pulling in chrono.
fn civil_from_days(secs: i64) -> (i32, u32, u32, u32, u32, u32) {
    let days = secs.div_euclid(86_400);
    let time = secs.rem_euclid(86_400) as u32;
    let z = days + 719_468;
    let era = if z >= 0 { z } else { z - 146_096 } / 146_097;
    let doe = (z - era * 146_097) as u64; // [0, 146096]
    let yoe = (doe - doe / 1460 + doe / 36_524 - doe / 146_096) / 365; // [0, 399]
    let y = yoe as i64 + era * 400;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100); // [0, 365]
    let mp = (5 * doy + 2) / 153; // [0, 11]
    let d = doy - (153 * mp + 2) / 5 + 1; // [1, 31]
    let m = if mp < 10 { mp + 3 } else { mp - 9 }; // [1, 12]
    let year = if m <= 2 { y + 1 } else { y };
    let h = time / 3600;
    let min = (time % 3600) / 60;
    let s = time % 60;
    (year as i32, m as u32, d as u32, h, min, s)
}
