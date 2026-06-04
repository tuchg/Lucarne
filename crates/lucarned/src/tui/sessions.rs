//! Sessions panel — the rmux-native session list + action console.
//!
//! The backend helpers (`run`, `rmux_out`, `pane_cwd`,
//! `archive_session`, …) were migrated verbatim from the old `lucarne-termctl`
//! `term` CLI (Decision 2: migrate-don't-rewrite). The gateway-monitored sessions
//! are driven through the SAME system rmux daemon via its own CLI
//! (`list-sessions` / `attach-session` / `detach-client` / `kill-session`), plus
//! the shared [`lucarne_rmux::archive`] store, so no control-plane IPC is introduced
//! (Decision 5).
//!
//! On top of those primitives this module hosts [`SessionsPanel`]: a ratatui
//! `ListState`-driven list of running rmux sessions with key actions Enter=attach
//! (pop-out handoff), `d`=detach, `k`/`Del`=kill, `a`=archive. Attach is a
//! suspend → run → resume terminal handoff (Decision 3); because the TUI owns the
//! terminal we must NOT `exec`-replace the process (that kills the TUI), so attach
//! spawns `rmux attach-session` and WAITS for it, then control returns to the TUI.

use std::io::{self, Stdout};

use lucarne_rmux::{archive, cli, monitor::scrollback_capture_start_arg};
use ratatui::{backend::CrosstermBackend, widgets::ListState, Terminal};

/// Operator hint shown when `rmux list-sessions` could not be reached (COR-005):
/// an empty list then means "rmux is down", not "no sessions". Shared by the
/// refresh status line and the empty-state render so the message is identical.
pub const RMUX_UNREACHABLE_HINT: &str = "rmux unreachable — is the rmux daemon running?";

/// Run `rmux <args>` inheriting stdio; return its exit code.
pub fn run(args: &[&str]) -> i32 {
    match cli::run_status(args) {
        Ok(status) => status.code().unwrap_or(1),
        Err(e) => {
            eprintln!("term: failed to run rmux: {e}");
            1
        }
    }
}

/// Run `rmux <args>` and capture stdout (None on failure).
pub fn rmux_out(args: &[&str]) -> Option<String> {
    let out = cli::output(args).ok()?;
    if !out.status.success() {
        return None;
    }
    Some(String::from_utf8_lossy(&out.stdout).into_owned())
}

/// The current working directory of a session's active pane (None when absent).
pub fn pane_cwd(name: &str) -> Option<String> {
    rmux_out(&["display-message", "-p", "-t", name, "#{pane_current_path}"])
        .map(|s| s.trim().to_string())
        .filter(|s| !s.is_empty())
}

fn capture_scrollback_args<'a>(start: &'a str, name: &'a str) -> [&'a str; 6] {
    ["capture-pane", "-p", "-S", start, "-t", name]
}

fn capture_scrollback(name: &str) -> Option<String> {
    let start = scrollback_capture_start_arg();
    rmux_out(&capture_scrollback_args(&start, name))
}

/// Capture `rmux list-sessions` output with an EXPLICIT format (COR-006) so the
/// authoritative, delimiter-free session name is available for action targets.
///
/// We pass `-F '#{session_name}\t<meta…>'`: the FIRST field (up to the first tab)
/// is the exact `session_name` used for every `-t`/`-s` action — a name that
/// contains `": "` (or other punctuation) therefore no longer corrupts the action
/// target the way scraping the default `"<name>: <meta>"` output did. The trailing
/// tab-separated text is human metadata for the detail pane only. Returns `None`
/// on failure / no daemon.
pub fn list_sessions_raw() -> Option<String> {
    rmux_out(&[
        "list-sessions",
        "-F",
        "#{session_name}\t#{session_windows} windows (created #{session_created_string})",
    ])
}

/// Run `rmux attach-session -t <name>` and WAIT for it to finish, returning the
/// exit code. Unlike the old `term attach` (which `exec`-replaced the process),
/// the TUI spawns + waits so the terminal is handed over for the lifetime of the
/// attach and control RETURNS to the TUI on detach/exit (Decision 3). Callers
/// should drive this through [`attach_handoff`], which suspends/resumes the TUI
/// around it; this raw form exists for the (cfg-guarded) test of the argv.
pub fn attach_session_wait(name: &str) -> i32 {
    match cli::run_status_interactive(&["attach-session", "-t", name]) {
        Ok(status) => status.code().unwrap_or(1),
        Err(e) => {
            eprintln!("term: failed to attach rmux session: {e}");
            1
        }
    }
}

/// Pop-out handoff (Decision 3): SUSPEND the TUI (`LeaveAlternateScreen` +
/// `disable_raw_mode`), hand the REAL terminal to `rmux attach-session -t <name>`
/// and WAIT for it (spawn+wait — never an `exec`-replace, which would kill the
/// TUI process), then RE-ENTER the TUI (`enable_raw_mode` + `EnterAlternateScreen`)
/// and force a full redraw + session-list refresh so the dashboard reflects any
/// change made while attached.
///
/// All rmux + terminal-handoff specifics live here in the sessions module so the
/// shared loop never learns provider details (it just forwards the request).
pub fn attach_handoff(
    terminal: &mut Terminal<CrosstermBackend<Stdout>>,
    panel: &mut SessionsPanel,
    name: &str,
) -> Result<(), String> {
    use crossterm::{
        execute,
        terminal::{disable_raw_mode, enable_raw_mode, EnterAlternateScreen, LeaveAlternateScreen},
    };

    // Suspend: hand the terminal back to a normal cooked-mode child.
    execute!(io::stdout(), LeaveAlternateScreen)
        .map_err(|e| format!("failed to leave alternate screen for attach: {e}"))?;
    disable_raw_mode().map_err(|e| format!("failed to disable raw mode for attach: {e}"))?;

    let code = attach_session_wait(name);
    panel.status = Some(if code == 0 {
        format!("attached to '{name}' (returned)")
    } else {
        format!("attach '{name}' exited {code}")
    });

    // Re-enter: re-enable raw mode FIRST, then arm an RAII guard (COR-002) so any
    // error on the rest of the resume path (EnterAlternateScreen / clear) cannot
    // leave the terminal in raw mode without the alternate screen. The guard is
    // disarmed on the happy path so the TUI keeps its raw + alt-screen state.
    enable_raw_mode().map_err(|e| format!("failed to re-enable raw mode after attach: {e}"))?;
    let mut restore_guard = RawModeGuard::armed();
    execute!(io::stdout(), EnterAlternateScreen)
        .map_err(|e| format!("failed to re-enter alternate screen after attach: {e}"))?;
    terminal
        .clear()
        .map_err(|e| format!("failed to clear terminal after attach: {e}"))?;
    // Resume succeeded: keep the raw + alternate-screen state for the live TUI.
    restore_guard.disarm();
    panel.refresh();
    Ok(())
}

/// RAII terminal-restore guard for the attach resume path (COR-002).
///
/// Armed right after `enable_raw_mode()` succeeds: if any later step on the
/// resume path early-returns (an `EnterAlternateScreen` / `clear` error), the
/// guard's `Drop` runs `disable_raw_mode()` + `LeaveAlternateScreen` so the user
/// is never dropped back to a raw terminal with no alternate screen. On the happy
/// path the caller [`disarm`](RawModeGuard::disarm)s it so the TUI keeps running
/// in raw + alt-screen mode.
struct RawModeGuard {
    armed: bool,
}

impl RawModeGuard {
    /// An armed guard — its `Drop` will restore the terminal unless disarmed.
    fn armed() -> Self {
        Self { armed: true }
    }

    /// Disarm the guard (the happy path succeeded): `Drop` becomes a no-op.
    fn disarm(&mut self) {
        self.armed = false;
    }
}

impl Drop for RawModeGuard {
    fn drop(&mut self) {
        if self.armed {
            use crossterm::terminal::{disable_raw_mode, LeaveAlternateScreen};
            // Best-effort restore on an error path: drop raw mode and leave the
            // alternate screen so the terminal is usable again.
            let _ = disable_raw_mode();
            let _ = crossterm::execute!(io::stdout(), LeaveAlternateScreen);
        }
    }
}

/// Retract: detach clients from a session; the session keeps running.
pub fn detach_client(name: &str) -> i32 {
    run(&["detach-client", "-s", name])
}

/// Delete (kill) a session.
pub fn kill_session(name: &str) -> i32 {
    run(&["kill-session", "-t", name])
}

/// Capture a session's content into the shared archive store, then close it.
///
/// Migrated from `term archive` (`lucarne-termctl/src/main.rs`): the same
/// capture/save/kill flow. Adapted from the CLI's `-> !` exit form to a `Result`
/// so the Sessions panel can surface success/failure inline rather than
/// terminating the process; the archive logic itself is unchanged.
pub fn archive_session(name: &str) -> Result<String, String> {
    let session_id = format!("{name}:0:0");
    let cwd = pane_cwd(name);
    let content = capture_scrollback(name).unwrap_or_default();
    match archive::save(
        &session_id,
        name,
        cwd.as_deref(),
        &content,
        archive::now_epoch(),
    ) {
        Ok(archive_id) => {
            run(&["kill-session", "-t", name]);
            Ok(archive_id)
        }
        Err(e) => Err(format!("term archive: {e}")),
    }
}

/// One parsed row of `rmux list-sessions` output.
///
/// `name` is the session id used for every action (`-t <name>` / `-s <name>`);
/// `meta` is the remaining descriptive text (e.g. `1 windows (created ...)`)
/// shown in the detail pane. Parsing is brittle text scraping (Decision 5
/// tradeoff: reuse the stable CLI, accept text parsing) and is isolated here.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SessionRow {
    /// The rmux session name (everything before the first `": "`).
    pub name: String,
    /// The trailing descriptive metadata, if any.
    pub meta: String,
}

/// Parse `rmux list-sessions -F '#{session_name}\t<meta>'` text into
/// [`SessionRow`]s (COR-006).
///
/// Each non-empty line is `"<session_name>\t<meta>"`; we split on the FIRST TAB so
/// the authoritative, delimiter-free `session_name` becomes the action target
/// `name` (used for `-t`/`-s`) even when it contains `": "` or other punctuation.
/// The remainder (if any) is the human metadata. Lines with no tab are a bare
/// name with empty metadata. Pure (no I/O) so it is unit-testable.
pub fn parse_sessions(output: &str) -> Vec<SessionRow> {
    output
        .lines()
        .map(|line| line.trim_end_matches(['\r', '\n']))
        .filter(|line| !line.trim().is_empty())
        .map(|line| match line.split_once('\t') {
            Some((name, meta)) => SessionRow {
                name: name.to_string(),
                meta: meta.trim().to_string(),
            },
            None => SessionRow {
                name: line.trim().to_string(),
                meta: String::new(),
            },
        })
        .collect()
}

/// What a key press on the Sessions panel asks the event loop to do. Most actions
/// run inline inside [`SessionsPanel`]; `Attach` is special because it needs the
/// terminal owner (the [`crate::tui`] loop) to suspend/resume around the handoff.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SessionAction {
    /// Nothing to do (key not handled by this panel).
    None,
    /// The loop must suspend the TUI, run `rmux attach-session -t <name>`, then
    /// re-enter the TUI and refresh — Decision 3 pop-out handoff.
    Attach(String),
}

/// The Sessions panel state: the parsed session list, its `ListState`, a
/// transient status line for action feedback (archive id, errors, hints), and
/// whether the last `rmux list-sessions` call was UNREACHABLE (COR-005) — so the
/// renderer can distinguish "rmux is down" from "rmux is up but has no sessions".
#[derive(Default)]
pub struct SessionsPanel {
    /// Sessions parsed from the last `rmux list-sessions`.
    pub sessions: Vec<SessionRow>,
    /// Selection state for the session list (clamped to `0..sessions.len()`).
    pub list: ListState,
    /// Last action result / hint, shown in the detail pane.
    pub status: Option<String>,
    /// COR-005: `true` when the last [`refresh`](Self::refresh) could not reach the
    /// rmux daemon (the `list-sessions` call returned `None` / non-zero), so an
    /// empty `sessions` means "unreachable", not "no sessions". `false` after a
    /// successful call (even one that returns zero sessions).
    pub rmux_unreachable: bool,
}

impl SessionsPanel {
    /// Build a fresh, EMPTY panel. Construction does no I/O (so it is cheap and
    /// test-friendly); call [`Self::refresh`] to populate it from the live rmux
    /// daemon — the event loop does this once before the first draw.
    pub fn new() -> Self {
        Self::default()
    }

    /// Re-run `rmux list-sessions` and re-parse, then re-clamp the selection so an
    /// index is never left past the (possibly shorter) new list. COR-005: a
    /// successful-but-empty result (`Some(out)` parsing to zero rows) sets
    /// [`rmux_unreachable`](Self::rmux_unreachable) `false` (genuinely no sessions),
    /// while a failed call (`None` — missing/non-zero rmux) sets it `true` so the
    /// renderer can tell "rmux is down" apart from "rmux is up but empty". Never
    /// panics.
    pub fn refresh(&mut self) {
        match list_sessions_raw() {
            Some(out) => {
                self.sessions = parse_sessions(&out);
                self.rmux_unreachable = false;
            }
            None => {
                self.sessions = Vec::new();
                self.rmux_unreachable = true;
            }
        }
        self.clamp_selection();
    }

    /// The currently selected session name, if any (None on an empty list).
    pub fn selected_name(&self) -> Option<&str> {
        self.list
            .selected()
            .and_then(|i| self.sessions.get(i))
            .map(|row| row.name.as_str())
    }

    /// Move the selection down one row, clamped to the list length. No-op (and no
    /// panic) on an empty list. Delegates to the shared [`super::nav::step`].
    pub fn select_next(&mut self) {
        super::nav::step(&mut self.list, self.sessions.len(), true);
    }

    /// Move the selection up one row, clamped at the top. No-op on an empty list.
    /// Delegates to the shared [`super::nav::step`].
    pub fn select_previous(&mut self) {
        super::nav::step(&mut self.list, self.sessions.len(), false);
    }

    /// Clamp the selection into `[0, len)` (or `None` on an empty list) so a stale
    /// index after a kill/archive can never be rendered out of bounds. Uses the
    /// shared [`super::nav::clamp`] with [`super::nav::EmptyPolicy::SelectFirst`]
    /// so a refresh of a non-empty list with no selection lands on the first row
    /// (preserving the prior behavior of seeding index 0).
    fn clamp_selection(&mut self) {
        super::nav::clamp(
            &mut self.list,
            self.sessions.len(),
            super::nav::EmptyPolicy::SelectFirst,
        );
    }

    /// Handle a key for the Sessions panel. Inline actions (detach/kill/archive)
    /// run immediately and refresh; Attach is deferred to the loop via
    /// [`SessionAction::Attach`] so the terminal can be suspended/resumed around it.
    ///
    /// Keys: Enter = attach, `d` = detach, `k`/`Delete` = kill, `a` = archive,
    /// `r` = refresh.
    pub fn handle_key(&mut self, code: crossterm::event::KeyCode) -> SessionAction {
        use crossterm::event::KeyCode;

        match code {
            KeyCode::Enter => {
                if let Some(name) = self.selected_name() {
                    return SessionAction::Attach(name.to_string());
                }
                self.status = Some("no session selected".to_string());
            }
            KeyCode::Char('d') => self.detach_selected(),
            KeyCode::Char('k') | KeyCode::Delete => self.kill_selected(),
            KeyCode::Char('a') => self.archive_selected(),
            KeyCode::Char('r') => {
                self.refresh();
                self.status = Some(if self.rmux_unreachable {
                    RMUX_UNREACHABLE_HINT.to_string()
                } else {
                    format!("refreshed — {} session(s)", self.sessions.len())
                });
            }
            _ => return SessionAction::None,
        }
        SessionAction::None
    }

    /// `d` — detach clients from the selected session; the session keeps running.
    fn detach_selected(&mut self) {
        let Some(name) = self.selected_name().map(str::to_string) else {
            self.status = Some("no session selected".to_string());
            return;
        };
        let code = detach_client(&name);
        self.status = Some(if code == 0 {
            format!("detached clients from '{name}'")
        } else {
            format!("detach '{name}' failed (exit {code})")
        });
        self.refresh();
    }

    /// `k` / `Del` — kill (delete) the selected session, then refresh + re-clamp.
    fn kill_selected(&mut self) {
        let Some(name) = self.selected_name().map(str::to_string) else {
            self.status = Some("no session selected".to_string());
            return;
        };
        let code = kill_session(&name);
        self.status = Some(if code == 0 {
            format!("killed '{name}'")
        } else {
            format!("kill '{name}' failed (exit {code})")
        });
        self.refresh();
    }

    /// `a` — archive the selected session (capture → save → kill), then refresh.
    fn archive_selected(&mut self) {
        let Some(name) = self.selected_name().map(str::to_string) else {
            self.status = Some("no session selected".to_string());
            return;
        };
        self.status = Some(match archive_session(&name) {
            Ok(archive_id) => format!("archived '{name}' -> {archive_id} (closed)"),
            Err(e) => e,
        });
        self.refresh();
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crossterm::event::KeyCode;

    fn panel_with(names: &[&str]) -> SessionsPanel {
        let sessions = names
            .iter()
            .map(|n| SessionRow {
                name: (*n).to_string(),
                meta: String::new(),
            })
            .collect::<Vec<_>>();
        let mut list = ListState::default();
        if !sessions.is_empty() {
            list.select(Some(0));
        }
        SessionsPanel {
            sessions,
            list,
            status: None,
            rmux_unreachable: false,
        }
    }

    #[test]
    fn parse_sessions_splits_name_and_meta() {
        // COR-006: the format is `#{session_name}\t<meta>` — split on the first tab.
        let out = "work\t1 windows (created Sat)\nside\t2 windows (created Sun)\n";
        let rows = parse_sessions(out);
        assert_eq!(rows.len(), 2);
        assert_eq!(rows[0].name, "work");
        assert_eq!(rows[0].meta, "1 windows (created Sat)");
        assert_eq!(rows[1].name, "side");
    }

    #[test]
    fn parse_sessions_ignores_blank_lines_and_bare_names() {
        let out = "\n\nbare\n  \nnamed\tmeta here\n";
        let rows = parse_sessions(out);
        assert_eq!(rows.len(), 2);
        assert_eq!(rows[0].name, "bare");
        assert_eq!(rows[0].meta, "");
        assert_eq!(rows[1].name, "named");
        assert_eq!(rows[1].meta, "meta here");
    }

    #[test]
    fn parse_sessions_preserves_colon_space_in_authoritative_name() {
        // COR-006: a session name containing `": "` must survive intact as the
        // action target (the old `": "` split corrupted it). The `-F` name is the
        // first tab-delimited field, so the whole name — colons and all — is kept.
        let out = "10:30: standup\t1 windows (created Mon)\n";
        let rows = parse_sessions(out);
        assert_eq!(rows.len(), 1);
        assert_eq!(
            rows[0].name, "10:30: standup",
            "the `-F` session_name (with `: `) must be the action key, uncorrupted"
        );
        assert_eq!(rows[0].meta, "1 windows (created Mon)");
    }

    #[test]
    fn parse_sessions_empty_output_is_empty() {
        assert!(parse_sessions("").is_empty());
        assert!(parse_sessions("   \n\t\n").is_empty());
    }

    #[test]
    fn archive_capture_args_are_bounded() {
        let start = scrollback_capture_start_arg();
        assert_ne!(start, "-", "TUI archive must not capture full pane history");
        assert_eq!(
            capture_scrollback_args(&start, "work"),
            ["capture-pane", "-p", "-S", "-1000", "-t", "work"]
        );
    }

    #[test]
    fn empty_list_navigation_never_panics() {
        let mut panel = panel_with(&[]);
        // Both directions on an empty list must be safe no-ops with no selection.
        panel.select_next();
        assert_eq!(panel.list.selected(), None);
        panel.select_previous();
        assert_eq!(panel.list.selected(), None);
        assert_eq!(panel.selected_name(), None);
    }

    #[test]
    fn navigation_clamps_to_bounds() {
        let mut panel = panel_with(&["a", "b", "c"]);
        assert_eq!(panel.list.selected(), Some(0));
        // Walking up past the top stays at 0.
        panel.select_previous();
        assert_eq!(panel.list.selected(), Some(0));
        // Walking down stops at the last index, never past it.
        for _ in 0..10 {
            panel.select_next();
        }
        assert_eq!(panel.list.selected(), Some(2));
        assert_eq!(panel.selected_name(), Some("c"));
    }

    #[test]
    fn clamp_after_shrink_keeps_index_in_range() {
        let mut panel = panel_with(&["a", "b", "c"]);
        for _ in 0..2 {
            panel.select_next();
        }
        assert_eq!(panel.list.selected(), Some(2));
        // Simulate a kill/archive shrinking the list under a stale selection.
        panel.sessions.truncate(1);
        panel.clamp_selection();
        assert_eq!(panel.list.selected(), Some(0));
        // Shrinking to empty drops the selection entirely (no out-of-range index).
        panel.sessions.clear();
        panel.clamp_selection();
        assert_eq!(panel.list.selected(), None);
    }

    #[test]
    fn enter_on_empty_list_does_not_request_attach() {
        let mut panel = panel_with(&[]);
        assert_eq!(panel.handle_key(KeyCode::Enter), SessionAction::None);
        assert!(panel.status.is_some());
    }

    #[test]
    fn enter_requests_attach_for_selected_session() {
        let mut panel = panel_with(&["work", "side"]);
        assert_eq!(
            panel.handle_key(KeyCode::Enter),
            SessionAction::Attach("work".to_string())
        );
    }

    #[test]
    fn unhandled_key_returns_none() {
        let mut panel = panel_with(&["work"]);
        assert_eq!(panel.handle_key(KeyCode::Char('z')), SessionAction::None);
    }

    #[test]
    fn empty_distinguishes_reachable_from_unreachable() {
        // COR-005: a successful `rmux list-sessions` that parses to zero rows is a
        // genuinely-empty list (reachable); a None call (rmux down) is unreachable.
        // Both end with an empty `sessions`, but the flag tells them apart so the
        // renderer can pick the right message.

        // Reachable-but-empty: a successful call returning no session lines.
        assert!(
            parse_sessions("").is_empty(),
            "a successful empty output parses to zero sessions"
        );
        let reachable_empty = {
            // Simulate what refresh() does for `Some("")`: empty list + reachable.
            let mut p = panel_with(&["stale"]);
            p.sessions = parse_sessions("");
            p.rmux_unreachable = false;
            p.clamp_selection();
            p
        };
        assert!(reachable_empty.sessions.is_empty());
        assert!(
            !reachable_empty.rmux_unreachable,
            "a reachable-but-empty list must NOT be flagged unreachable"
        );

        // Unreachable: simulate what refresh() does for `None` (rmux down).
        let mut unreachable = panel_with(&["stale"]);
        unreachable.sessions = Vec::new();
        unreachable.rmux_unreachable = true;
        unreachable.clamp_selection();
        assert!(unreachable.sessions.is_empty());
        assert!(
            unreachable.rmux_unreachable,
            "a failed (None) list call must be flagged unreachable"
        );
        // The hint the renderer shows is the shared constant.
        assert!(RMUX_UNREACHABLE_HINT.contains("rmux unreachable"));
    }
}
