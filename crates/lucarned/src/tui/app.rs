//! TUI application state machine.
//!
//! The dashboard is a small enum state machine (Free decision F: opencode-style
//! left list + right detail + bottom hints). `App` tracks the active [`Panel`]
//! and a per-panel [`ListState`] selection. Panel-specific content + actions are
//! filled in by the panel tasks (Sessions → TASK-002, GoPublic → TASK-003,
//! Config → TASK-004); this skeleton only owns navigation + selection so those
//! tasks can run in parallel.

use ratatui::widgets::ListState;

use super::config::ConfigPanel;
use super::remote::GoPublicPanel;
use super::sessions::{SessionAction, SessionsPanel};

/// The three top-level panels of the dashboard.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Panel {
    /// rmux session list + actions (attach / detach / kill / archive).
    Sessions,
    /// Public tunnel start/status + login QR + access key.
    GoPublic,
    /// Remote-access provider configuration editor.
    Config,
}

impl Panel {
    /// Panels in tab order.
    pub const ALL: [Panel; 3] = [Panel::Sessions, Panel::GoPublic, Panel::Config];

    /// Short title shown in the left list / tab bar.
    pub fn title(self) -> &'static str {
        match self {
            Panel::Sessions => "Sessions",
            Panel::GoPublic => "Go Public",
            Panel::Config => "Config",
        }
    }

    /// The next panel in tab order (wraps).
    pub fn next(self) -> Panel {
        let idx = Self::ALL.iter().position(|&p| p == self).unwrap_or(0);
        Self::ALL[(idx + 1) % Self::ALL.len()]
    }

    /// The previous panel in tab order (wraps).
    pub fn prev(self) -> Panel {
        let idx = Self::ALL.iter().position(|&p| p == self).unwrap_or(0);
        Self::ALL[(idx + Self::ALL.len() - 1) % Self::ALL.len()]
    }
}

/// Top-level dashboard state.
pub struct App {
    /// Whether the event loop should keep running.
    pub running: bool,
    /// The currently focused panel.
    pub active: Panel,
    /// Selection state for the left panel selector list. Mirrors `active` so the
    /// highlighted entry always matches the focused panel.
    pub list: ListState,
    /// The Sessions panel: rmux session list + actions (TASK-002).
    pub sessions: SessionsPanel,
    /// The Go-Public panel: tunnel start/stop/status + login QR (TASK-003).
    pub go_public: GoPublicPanel,
    /// The Config panel: descriptor-driven provider-field editor (TASK-004).
    pub config: ConfigPanel,
}

impl Default for App {
    fn default() -> Self {
        let mut list = ListState::default();
        // Select the first panel by default (index-bound: ALL is non-empty).
        list.select(Some(0));
        Self {
            running: true,
            active: Panel::Sessions,
            list,
            sessions: SessionsPanel::new(),
            go_public: GoPublicPanel::new(),
            config: ConfigPanel::new(),
        }
    }
}

impl App {
    /// Create a fresh dashboard focused on the Sessions panel.
    pub fn new() -> Self {
        Self::default()
    }

    /// Request the event loop to stop on the next iteration.
    pub fn quit(&mut self) {
        self.running = false;
    }

    /// Switch focus to the next panel and keep the selector list in sync.
    pub fn focus_next(&mut self) {
        self.active = self.active.next();
        self.sync_list_to_active();
    }

    /// Switch focus to the previous panel and keep the selector list in sync.
    pub fn focus_prev(&mut self) {
        self.active = self.active.prev();
        self.sync_list_to_active();
    }

    /// Move the active panel's content selection down by one. Routed to the
    /// focused panel's own list (Sessions today; GoPublic/Config add theirs in
    /// TASK-003/004). Never touches the panel selector.
    pub fn select_next(&mut self) {
        match self.active {
            Panel::Sessions => self.sessions.select_next(),
            Panel::Config => self.config.select_next(),
            Panel::GoPublic => {}
        }
    }

    /// Move the active panel's content selection up by one (see [`Self::select_next`]).
    pub fn select_previous(&mut self) {
        match self.active {
            Panel::Sessions => self.sessions.select_previous(),
            Panel::Config => self.config.select_previous(),
            Panel::GoPublic => {}
        }
    }

    /// Dispatch a non-navigation key to the active panel. Returns the panel's
    /// [`SessionAction`] so the event loop can perform deferred work (the attach
    /// pop-out handoff) that needs the terminal owner. The Sessions panel is the
    /// only one with deferred work; the Go-Public and Config panels run inline and
    /// always resolve to [`SessionAction::None`].
    ///
    /// PART 1: when the Go-Public panel is active and the key is `s` (start), the
    /// App BRIDGES the two panels it owns — it reads the Config panel's live
    /// [`start_params`](ConfigPanel::start_params) and drives
    /// [`GoPublicPanel::start_with`] with them, so the operator's in-TUI provider +
    /// fields are used without saving `lucarned.yaml` first (an empty Config →
    /// empty params → the daemon's pre-configured tunnel). Every other Go-Public
    /// key (`x`/`r`/`Enter`/`Esc`) is routed to the panel as before.
    pub fn handle_panel_key(&mut self, code: crossterm::event::KeyCode) -> SessionAction {
        use crossterm::event::KeyCode;
        match self.active {
            Panel::Sessions => self.sessions.handle_key(code),
            Panel::GoPublic => {
                // Bridge: `s` starts using the Config panel's live edits (PART 1);
                // the QR modal (when open) still consumes keys via handle_key below.
                if code == KeyCode::Char('s') && !self.go_public.qr_open {
                    let (provider, fields) = self.config.start_params();
                    self.go_public.start_with(provider, fields);
                } else {
                    self.go_public.handle_key(code);
                }
                SessionAction::None
            }
            Panel::Config => {
                self.config.handle_key(code);
                SessionAction::None
            }
        }
    }

    /// Whether the active panel has a modal open that must consume `q`/`Esc`
    /// (so those keys close the modal / are captured by an inline editor instead
    /// of quitting the app). The Go-Public login-QR modal does; so does the Config
    /// panel's inline field editor (so a literal `q` types into the field and `Esc`
    /// cancels the edit rather than quitting).
    pub fn modal_open(&self) -> bool {
        (matches!(self.active, Panel::GoPublic) && self.go_public.qr_open)
            || (matches!(self.active, Panel::Config) && self.config.editing.is_some())
    }

    /// Mirror the active panel into the selector list selection.
    fn sync_list_to_active(&mut self) {
        if let Some(idx) = Panel::ALL.iter().position(|&p| p == self.active) {
            self.list.select(Some(idx));
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn panel_tab_order_wraps() {
        assert_eq!(Panel::Sessions.next(), Panel::GoPublic);
        assert_eq!(Panel::GoPublic.next(), Panel::Config);
        assert_eq!(Panel::Config.next(), Panel::Sessions);
        assert_eq!(Panel::Sessions.prev(), Panel::Config);
    }

    #[test]
    fn focus_next_syncs_list_and_active() {
        let mut app = App::new();
        assert_eq!(app.active, Panel::Sessions);
        assert_eq!(app.list.selected(), Some(0));
        app.focus_next();
        assert_eq!(app.active, Panel::GoPublic);
        assert_eq!(app.list.selected(), Some(1));
    }

    #[test]
    fn focus_prev_wraps_and_syncs_list() {
        let mut app = App::new();
        app.focus_prev();
        assert_eq!(app.active, Panel::Config);
        assert_eq!(app.list.selected(), Some(2));
    }

    #[test]
    fn navigation_on_non_sessions_panel_is_noop() {
        let mut app = App::new();
        app.focus_next(); // GoPublic — no content list yet
        assert_eq!(app.active, Panel::GoPublic);
        // Must not panic and must leave the selector untouched.
        app.select_next();
        app.select_previous();
        assert_eq!(app.active, Panel::GoPublic);
        assert_eq!(app.list.selected(), Some(1));
    }

    #[test]
    fn quit_stops_running() {
        let mut app = App::new();
        assert!(app.running);
        app.quit();
        assert!(!app.running);
    }

    #[test]
    fn go_public_start_uses_config_params_and_validation_blocks_bad_config() {
        // PART 1: pressing `s` on the Go-Public panel must bridge through the App to
        // the Config panel's live start_params. When the Config provider has a
        // missing REQUIRED field, the provider's validate_config rejects it inline
        // and NOTHING is sent (no daemon needed for this path).
        use crossterm::event::KeyCode;
        let mut app = App::new();
        app.active = Panel::GoPublic;

        // Configure cloudflared with NO fields. cloudflared requires a tunnel
        // `token` for a named tunnel; with a token absent but a public_url present
        // it would be invalid — but more simply, drive a config the descriptor
        // rejects. We set a provider + a token-gated public_url is required when a
        // token is set, so set a token and omit the conditionally-required field.
        app.config.edits.provider = "cloudflared".to_string();
        app.config
            .edits
            .fields
            .insert("token".to_string(), "abc".to_string());
        // The conditional `public_url` (required_when token present) is omitted, so
        // validate_config must fail and block the start before any network call.

        let before = app.go_public.message.clone();
        app.handle_panel_key(KeyCode::Char('s'));
        let msg = app.go_public.message.clone();
        assert_ne!(msg, before, "a blocked start must surface a message");
        assert!(
            msg.as_deref().unwrap_or("").starts_with("start blocked — "),
            "validation failure must block + surface inline, got: {msg:?}"
        );
        // No tunnel status was set (the start was blocked before sending).
        assert!(app.go_public.status.is_none());
        assert!(!app.go_public.qr_open);
    }
}
