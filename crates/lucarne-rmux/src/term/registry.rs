//! registry — the in-process terminal-session registry.

use std::collections::HashMap;

use serde::{Deserialize, Serialize};

use crate::term::grid::Dims;

/// Stable session handle, e.g. "{session}:{window}:{pane}".
pub type SessionId = String;

/// Current rmux monitor scope: one primary pane per session.
pub const PRIMARY_WINDOW: u32 = 0;
/// Current rmux monitor scope: one primary pane per session.
pub const PRIMARY_PANE: u32 = 0;

/// Error returned when a caller asks for a pane outside the supported monitor
/// scope. This is explicit because the public id shape includes
/// `{session}:{window}:{pane}`, while the current backend only tracks `(0,0)`.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum PaneSelectionError {
    #[error("session id `{0}` is not a primary pane id of the form <session>:0:0")]
    Invalid(String),
    #[error("only primary pane <session>:0:0 is supported; got window={window}, pane={pane}")]
    Unsupported { window: u32, pane: u32 },
}

/// Build the current stable id for the primary pane of an rmux session.
pub fn primary_pane_session_id(name: &str) -> SessionId {
    format!("{name}:{PRIMARY_WINDOW}:{PRIMARY_PANE}")
}

/// Split a primary-pane session id back into its rmux session name.
pub fn split_primary_pane_session_id(id: &str) -> Result<&str, PaneSelectionError> {
    let Some((head, pane_raw)) = id.rsplit_once(':') else {
        return Err(PaneSelectionError::Invalid(id.to_string()));
    };
    let Some((session, window_raw)) = head.rsplit_once(':') else {
        return Err(PaneSelectionError::Invalid(id.to_string()));
    };
    if session.is_empty() {
        return Err(PaneSelectionError::Invalid(id.to_string()));
    }
    let window = window_raw
        .parse::<u32>()
        .map_err(|_| PaneSelectionError::Invalid(id.to_string()))?;
    let pane = pane_raw
        .parse::<u32>()
        .map_err(|_| PaneSelectionError::Invalid(id.to_string()))?;
    if window != PRIMARY_WINDOW || pane != PRIMARY_PANE {
        return Err(PaneSelectionError::Unsupported { window, pane });
    }
    Ok(session)
}

/// Validate that an incoming id targets the primary pane currently mirrored by
/// the rmux monitor.
pub fn validate_primary_pane_session_id(id: &str) -> Result<(), PaneSelectionError> {
    split_primary_pane_session_id(id).map(|_| ())
}

/// Session provenance on the monitored system rmux daemon.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Origin {
    /// Discovered on the system daemon (monitor model — not created by us).
    Adopted,
    /// Created by this process via the CLI / `new` command.
    Managed,
}

/// A monitored session and the metadata the mirror / CLI needs.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct SessionDescriptor {
    pub id: SessionId,
    pub title: String,
    /// Provenance: discovered (Adopted) vs created-by-us (Managed).
    pub origin: Origin,
    /// PTY grid size last seen at registration.
    pub dims: Dims,
    /// The pane's current working directory, if known.
    #[serde(default)]
    pub cwd: Option<String>,
}

/// In-process map of `SessionId -> SessionDescriptor`.
#[derive(Debug, Default)]
pub struct SessionRegistry {
    sessions: HashMap<SessionId, SessionDescriptor>,
}

impl SessionRegistry {
    /// Creates an empty registry.
    #[must_use]
    pub fn new() -> Self {
        Self {
            sessions: HashMap::new(),
        }
    }

    /// Registers (or replaces) a session (cwd unknown).
    pub fn register(
        &mut self,
        id: SessionId,
        title: impl Into<String>,
        origin: Origin,
        dims: Dims,
    ) -> SessionDescriptor {
        self.register_with_cwd(id, title, origin, dims, None)
    }

    /// Registers (or replaces) a session, recording its pane cwd.
    pub fn register_with_cwd(
        &mut self,
        id: SessionId,
        title: impl Into<String>,
        origin: Origin,
        dims: Dims,
        cwd: Option<String>,
    ) -> SessionDescriptor {
        let descriptor = SessionDescriptor {
            id: id.clone(),
            title: title.into(),
            origin,
            dims,
            cwd,
        };
        self.sessions.insert(id, descriptor.clone());
        descriptor
    }

    /// Looks up a session by id.
    #[must_use]
    pub fn get(&self, id: &str) -> Option<&SessionDescriptor> {
        self.sessions.get(id)
    }

    /// Returns whether a session is registered.
    #[must_use]
    pub fn contains(&self, id: &str) -> bool {
        self.sessions.contains_key(id)
    }

    /// Number of registered sessions.
    #[must_use]
    pub fn len(&self) -> usize {
        self.sessions.len()
    }

    /// Whether the registry holds no sessions.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.sessions.is_empty()
    }

    /// Removes a session, returning the previously-stored descriptor.
    pub fn remove(&mut self, id: &str) -> Option<SessionDescriptor> {
        self.sessions.remove(id)
    }

    /// Snapshot of all registered sessions (unordered).
    #[must_use]
    pub fn list(&self) -> Vec<SessionDescriptor> {
        self.sessions.values().cloned().collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn dims() -> Dims {
        Dims {
            cols: 120,
            rows: 32,
        }
    }

    #[test]
    fn register_get_list_roundtrip() {
        let mut reg = SessionRegistry::new();
        assert!(reg.is_empty());

        reg.register("s:0:0".to_string(), "shell", Origin::Adopted, dims());
        reg.register("s:0:1".to_string(), "ours", Origin::Managed, dims());

        assert_eq!(reg.len(), 2);
        assert!(reg.contains("s:0:0"));
        assert_eq!(reg.get("s:0:0").map(|d| d.origin), Some(Origin::Adopted));
        assert_eq!(reg.get("s:0:1").map(|d| d.origin), Some(Origin::Managed));
        assert_eq!(reg.get("missing"), None);

        let mut titles: Vec<_> = reg.list().into_iter().map(|d| d.title).collect();
        titles.sort();
        assert_eq!(titles, vec!["ours", "shell"]);
    }

    #[test]
    fn register_replaces_existing_and_remove_works() {
        let mut reg = SessionRegistry::new();
        reg.register("s:0:0".to_string(), "old", Origin::Adopted, dims());
        let d = reg.register("s:0:0".to_string(), "new", Origin::Managed, dims());
        assert_eq!(reg.len(), 1);
        assert_eq!(d.origin, Origin::Managed);

        let removed = reg.remove("s:0:0");
        assert_eq!(removed.map(|d| d.title), Some("new".to_string()));
        assert!(reg.is_empty());
    }

    #[test]
    fn primary_pane_id_round_trips_session_names_with_colons() {
        let id = primary_pane_session_id("10:30: standup");
        assert_eq!(id, "10:30: standup:0:0");
        assert_eq!(
            split_primary_pane_session_id(&id).expect("primary pane id"),
            "10:30: standup"
        );
    }

    #[test]
    fn non_primary_panes_are_rejected_explicitly() {
        assert!(matches!(
            split_primary_pane_session_id("work:1:0"),
            Err(PaneSelectionError::Unsupported { window: 1, pane: 0 })
        ));
        assert!(matches!(
            split_primary_pane_session_id("work:0:3"),
            Err(PaneSelectionError::Unsupported { window: 0, pane: 3 })
        ));
        assert!(matches!(
            split_primary_pane_session_id("bad"),
            Err(PaneSelectionError::Invalid(_))
        ));
    }
}
