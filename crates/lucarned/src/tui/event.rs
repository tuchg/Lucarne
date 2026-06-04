//! Keyboard event handling for the dashboard.
//!
//! Reads one `crossterm` key event and maps it onto an [`App`] transition:
//! Tab/Right and BackTab/Left switch panel focus, Up/Down move the active panel's
//! content selection, and `q` / Esc / Ctrl-C quit. Fixed keybinds for v1 (Free
//! decision: fixed keybind set; customization deferred). Any other key is routed
//! to the active panel (Sessions: Enter=attach, d=detach, k/Del=kill, a=archive,
//! r=refresh); a panel may ask the loop to perform deferred work (the attach
//! pop-out handoff) via the returned [`SessionAction`].

use std::time::Duration;

use crossterm::event::{self, Event, KeyCode, KeyEventKind, KeyModifiers};

use super::app::App;
use super::sessions::SessionAction;

/// COR-003: poll for a terminal event for up to one second, returning whether one
/// is now ready to [`read`](event::read). The 1s timeout (not a spin) lets the loop
/// redraw periodically (e.g. to reflect an out-of-band status change) without
/// busy-waiting — when this returns `false` the caller simply redraws and polls
/// again; when it returns `true` the caller drives [`handle_next`].
pub fn poll_ready() -> std::io::Result<bool> {
    event::poll(Duration::from_millis(1000))
}

/// Read the next (already-[`poll_ready`]'d) terminal event and apply it to `app`.
/// Returns the panel [`SessionAction`] the caller must act on (the attach pop-out
/// handoff needs the terminal owner). The caller also checks `app.running` to
/// decide whether to keep looping. Non-key events (resize, mouse) are ignored —
/// the next draw handles a resize.
///
/// MUST be called only after [`poll_ready`] returned `true` so the underlying
/// [`event::read`] does not block the loop (COR-003).
pub fn handle_next(app: &mut App) -> std::io::Result<SessionAction> {
    match event::read()? {
        Event::Key(key) if key.kind == KeyEventKind::Press => {
            // Ctrl-C always quits regardless of the focused panel.
            if key.modifiers.contains(KeyModifiers::CONTROL) && key.code == KeyCode::Char('c') {
                app.quit();
                return Ok(SessionAction::None);
            }
            // When the active panel has a modal/editor open (the Go-Public login
            // QR, or the Config panel's inline field editor), route the key to the
            // panel first so it captures input (typing, commit/cancel, closing the
            // QR) instead of the global nav/quit binds acting on it. Ctrl-C above
            // is the unconditional escape hatch.
            if app.modal_open() {
                return Ok(app.handle_panel_key(key.code));
            }
            match key.code {
                KeyCode::Char('q') | KeyCode::Esc => app.quit(),
                KeyCode::Tab | KeyCode::Right => app.focus_next(),
                KeyCode::BackTab | KeyCode::Left => app.focus_prev(),
                KeyCode::Down => app.select_next(),
                KeyCode::Up => app.select_previous(),
                // Arrow keys move the selection; every other key (Enter, d, k,
                // Del, a, r) is routed to the active panel's action handler. We do
                // NOT bind vim `j`/`k` to navigation here because the Sessions
                // panel claims `k` for kill — arrows are the one nav path.
                other => return Ok(app.handle_panel_key(other)),
            }
        }
        _ => {}
    }
    Ok(SessionAction::None)
}
