//! Go-Public panel backend — the tunnel-control-plane helpers now share the
//! `lucarned remote start` control path. The TUI stays a thin wrapper (Locked decision L6): it
//! never spawns a tunnel itself. It picks a built-in provider, collects that
//! provider's required fields (see [`crate::tui::config`]), then drives the
//! daemon's loopback-only control plane (`POST /api/remote/start`) so the daemon
//! — which owns the tunnel lifecycle — brings the tunnel up and hands back the
//! public URL + access token. The TUI then renders the URL, a terminal QR of the
//! login link (Decision 4), and the access key.
//!
//! Panel rendering / interaction wiring lands in TASK-003; this module only
//! houses the reusable go-public primitives + their migrated tests.

/// Default loopback gateway port the daemon binds in remote mode
/// (`lucarned` `DEFAULT_REMOTE_GATEWAY_ADDR = 127.0.0.1:7800`). Overridable with
/// `--gateway-port <P>`.
pub const DEFAULT_GATEWAY_PORT: u16 = 7800;

/// Default loopback CONTROL-plane port (SEC-002): the daemon serves
/// `/api/remote/*` on a DISTINCT loopback port the tunnel never targets
/// (`lucarned` `DEFAULT_REMOTE_CONTROL_ADDR = 127.0.0.1:7801`). When not given
/// explicitly it is derived as `gateway-port + 1`, matching the daemon's default.
/// Overridable with `--control-port <P>`.
///
/// L1: uses `checked_add` so a gateway bound to port 65535 does not silently
/// wrap to 0 — `None` means the caller must pass an explicit `--control-port`.
/// `const` so the config panel can derive its `DEFAULT_CONTROL_PORT` from the
/// gateway default at compile time (MNT-004 single source of truth).
pub const fn default_control_port(gateway_port: u16) -> Option<u16> {
    gateway_port.checked_add(1)
}

/// What `go-public` resolved before touching the network: the selected provider,
/// the collected (non-secret-aware) field map, and the loopback URL it will POST.
/// Kept as a plain value so the control-plane call (`call_remote_start`) and the
/// panel's `start` path can assemble + send it without a running daemon in tests.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GoPublicPlan {
    pub provider: String,
    pub fields: std::collections::BTreeMap<String, String>,
    pub control_url: String,
}

/// Render `content` as a small terminal QR code (half-block rows). Mirrors
/// `lucarne-wechat`'s `render_terminal_qr` (`adapter.rs:900-938`) so the visual
/// is identical to the WeChat login QR.
pub fn render_terminal_qr(content: &str) -> Result<String, qrcode::types::QrError> {
    use qrcode::types::Color;
    use qrcode::{EcLevel, QrCode};

    let code = QrCode::with_error_correction_level(content.trim().as_bytes(), EcLevel::L)?;
    let module_count = code.width();
    let modules = code.to_colors();
    let color_at = |row: usize, col: usize| modules[row * module_count + col];

    let odd_row = module_count % 2 == 1;
    let output_rows = module_count.div_ceil(2);
    let mut output = String::new();

    output.push_str(&"▄".repeat(module_count + 2));
    output.push('\n');

    for row in 0..output_rows {
        output.push('█');
        for col in 0..module_count {
            let top = color_at(row * 2, col);
            let bottom = if row * 2 + 1 < module_count {
                color_at(row * 2 + 1, col)
            } else {
                Color::Light
            };
            output.push(match (top, bottom) {
                (Color::Light, Color::Light) => '█',
                (Color::Light, Color::Dark) => '▀',
                (Color::Dark, Color::Light) => '▄',
                (Color::Dark, Color::Dark) => ' ',
            });
        }
        output.push('█');
        output.push('\n');
    }

    if !odd_row {
        output.push_str(&"▀".repeat(module_count + 2));
        output.push('\n');
    }

    Ok(output)
}

/// The login URL a remote client opens: the public URL with the access token
/// carried in the fragment (`#token=…`), matching the gateway's
/// `RemoteControlStatus` doc contract. Returns the bare URL when no token.
pub fn login_url(public_url: &str, access_token: Option<&str>) -> String {
    match access_token {
        Some(token) if !token.is_empty() => format!("{public_url}#token={token}"),
        _ => public_url.to_string(),
    }
}

// The `go-public` CLI-entry cluster (arg parsing + the textual `report_tunnel_up`
// reporter) was removed with the standalone `term` binary (Decision 1): the v1
// TUI panel drives the control plane directly (`GoPublicPanel::start` →
// `call_remote_start`) and never routed through a CLI arg path. The reusable
// control-plane primitives (`call_remote_start`/`status`/`stop`) and the QR/URL
// helpers (`render_terminal_qr`/`login_url`) below are the live surface.

/// Shared `reqwest::blocking` control-plane round-trip (MNT-003): run the prepared
/// `request`, map a transport error to a "failed to reach daemon at {url}" message,
/// reject a non-2xx with `daemon returned {code}: {detail}`, then parse the body as
/// the `RemoteStartStatus` JSON contract. Every `/api/remote/*` call goes through
/// this one place so the send → status-check → parse boilerplate lives once.
fn send_control(
    request: reqwest::blocking::RequestBuilder,
    url: &str,
) -> Result<lucarne_remote_status::RemoteStartStatus, String> {
    let resp = request
        .send()
        .map_err(|e| format!("failed to reach daemon at {url}: {e}"))?;
    if !resp.status().is_success() {
        let code = resp.status();
        let detail = resp.text().unwrap_or_default();
        return Err(format!("daemon returned {code}: {detail}"));
    }
    resp.json::<lucarne_remote_status::RemoteStartStatus>()
        .map_err(|e| format!("failed to parse daemon response: {e}"))
}

/// POST `/api/remote/start` to the daemon loopback control plane and parse the
/// `RemoteControlStatus` response. The TUI sends the chosen provider id + that
/// provider's fields as the JSON body ([`RemoteStartParams`]); the daemon uses
/// them to override / merge its pre-configured tunnel (G3) and, on a cold daemon,
/// lazily brings the gateway + tunnel up on this first call.
pub fn call_remote_start(
    plan: &GoPublicPlan,
) -> Result<lucarne_remote_status::RemoteStartStatus, String> {
    let body = serde_json::json!({
        "provider": plan.provider,
        "fields": plan.fields,
    });
    let client = reqwest::blocking::Client::new();
    send_control(
        client.post(&plan.control_url).json(&body),
        &plan.control_url,
    )
}

/// The loopback control URL for one `/api/remote/<verb>` route on `control_port`.
/// Shared by `start`/`stop`/`status` so the panel never hardcodes the host.
pub fn control_url(control_port: u16, verb: &str) -> String {
    format!("http://127.0.0.1:{control_port}/api/remote/{verb}")
}

/// GET `/api/remote/status` on the loopback control plane and parse the
/// `RemoteControlStatus` response. Reports whether the daemon-owned tunnel is up,
/// its provider, public URL, and (presence of) the access token.
pub fn call_remote_status(
    control_port: u16,
) -> Result<lucarne_remote_status::RemoteStartStatus, String> {
    let url = control_url(control_port, "status");
    let client = reqwest::blocking::Client::new();
    send_control(client.get(&url), &url)
}

/// POST `/api/remote/stop` on the loopback control plane (idempotent: stopping an
/// already-down tunnel succeeds) and parse the resulting `RemoteControlStatus`.
pub fn call_remote_stop(
    control_port: u16,
) -> Result<lucarne_remote_status::RemoteStartStatus, String> {
    let url = control_url(control_port, "stop");
    let client = reqwest::blocking::Client::new();
    send_control(client.post(&url), &url)
}

/// Mirror of `lucarne_termgw::RemoteControlStatus` for deserialization (the TUI
/// does not depend on the gateway crate; the JSON shape is the stable contract).
pub mod lucarne_remote_status {
    use serde::Deserialize;

    #[derive(Debug, Clone, Default, Deserialize)]
    pub struct RemoteStartStatus {
        pub running: bool,
        pub provider: Option<String>,
        pub public_url: Option<String>,
        pub access_token: Option<String>,
    }
}

// ---- Go-Public panel (TASK-003): state + control-plane actions + QR modal ----

use ratatui::layout::Rect;
use ratatui::style::{Color, Style};
use ratatui::text::{Line, Text};
use ratatui::widgets::{Block, Borders, Clear, Paragraph};
use ratatui::Frame;

/// The dimensions of a rendered QR grid (rows × the widest line, in cells).
///
/// `render_terminal_qr` returns half-block rows of equal width, so the grid is a
/// rectangle: `cols` = the glyph count of any line, `rows` = the line count. Kept
/// as a pure value so the fits-the-modal decision is unit-testable without a
/// terminal.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct QrGrid {
    pub cols: u16,
    pub rows: u16,
}

impl QrGrid {
    /// Measure a rendered QR string. `cols` is the widest line's char count (the
    /// grid is rectangular, so all rows match) and `rows` is the non-empty line
    /// count. Saturates at `u16::MAX` so an absurdly large grid never overflows.
    pub fn measure(qr: &str) -> QrGrid {
        let mut cols: usize = 0;
        let mut rows: usize = 0;
        for line in qr.lines() {
            if line.is_empty() {
                continue;
            }
            rows += 1;
            cols = cols.max(line.chars().count());
        }
        QrGrid {
            cols: cols.min(u16::MAX as usize) as u16,
            rows: rows.min(u16::MAX as usize) as u16,
        }
    }

    /// True when this grid fits inside `inner` (the modal rect MINUS its border).
    /// The fit is exact on both axes — if either dimension is short, the caller
    /// falls back to the plain login URL + a "terminal too small" hint so the
    /// layout never breaks (Decision 4).
    pub fn fits_within(&self, inner: Rect) -> bool {
        inner.width >= self.cols && inner.height >= self.rows
    }
}

/// A centered sub-`Rect` of `width`×`height` clamped to `area` (never larger than
/// the available space). Used to float the QR modal over the panel body.
pub fn centered_rect(width: u16, height: u16, area: Rect) -> Rect {
    let w = width.min(area.width);
    let h = height.min(area.height);
    let x = area.x + (area.width.saturating_sub(w)) / 2;
    let y = area.y + (area.height.saturating_sub(h)) / 2;
    Rect::new(x, y, w, h)
}

/// Validate a CLI-supplied provider id + field map against the provider's own
/// descriptor BEFORE sending `/api/remote/start` (PART 1 step 5). The check lives
/// behind `lucarne_remote::builtin()` so the provider boundary (AGENTS.md) stays
/// intact — the panel never enumerates concrete provider fields, it just builds an
/// opaque [`ProviderConfig`] from the (already non-empty) field values and asks the
/// provider to enforce its own rules. Returns `Err(detail)` on an unknown provider
/// or a failed `validate_config`; `Ok(())` when the config is acceptable.
fn validate_start_config(
    provider: &str,
    fields: &std::collections::BTreeMap<String, String>,
) -> Result<(), String> {
    let registry = lucarne_remote::builtin();
    let descriptor = registry
        .get(provider)
        .ok_or_else(|| format!("unknown provider `{provider}`"))?;
    let mut cfg = lucarne_remote::ProviderConfig::new();
    for (k, v) in fields {
        if !v.is_empty() {
            cfg.fields.insert(k.clone(), v.clone());
        }
    }
    descriptor.validate_config(&cfg)
}

/// Turn a control-plane call error into an actionable panel message. The
/// `call_remote_*` helpers prefix connectivity failures with "failed to reach
/// daemon at" — that almost always means the lucarned daemon isn't running.
/// The daemon owns the tunnel lifecycle and serves the loopback control plane,
/// so `lucarned tui` cannot reach it standalone; surface a next step instead of
/// a raw reqwest error. Non-connectivity errors pass through verbatim.
fn explain_control_error(op: &str, control_port: u16, e: &str) -> String {
    if e.contains("failed to reach daemon") {
        format!(
            "{op}: control plane unreachable on 127.0.0.1:{control_port} — the lucarned \
             daemon isn't running. Start it first (`lucarned autostart start`, or \
             `brew services start lucarned`); it owns the tunnel lifecycle."
        )
    } else {
        format!("{op}: {e}")
    }
}

/// The Go-Public panel state: the loopback control port it drives, the latest
/// tunnel status, a transient status/error line, and whether the QR login modal
/// is open. All control-plane + QR specifics live here so the shared UI/loop never
/// learns provider details (AGENTS.md boundary).
pub struct GoPublicPanel {
    /// Loopback control-plane port (`gateway 7800 + 1 = 7801` by default).
    pub control_port: u16,
    /// Last `RemoteControlStatus` fetched / returned by start/stop, if any.
    pub status: Option<lucarne_remote_status::RemoteStartStatus>,
    /// Last action result / error line shown under the panel body.
    pub message: Option<String>,
    /// Whether the centered login-QR modal is currently shown.
    pub qr_open: bool,
}

impl Default for GoPublicPanel {
    fn default() -> Self {
        // Default control port = the daemon default (gateway 7800 + 1). 7800 can
        // never overflow the +1, so the unwrap is infallible here.
        let control_port = default_control_port(DEFAULT_GATEWAY_PORT).unwrap_or(7801);
        Self {
            control_port,
            status: None,
            message: None,
            qr_open: false,
        }
    }
}

impl GoPublicPanel {
    /// Build a fresh panel bound to the default loopback control port. Does no
    /// I/O (construction is cheap + test-friendly); call [`Self::refresh`] to pull
    /// the live status.
    pub fn new() -> Self {
        Self::default()
    }

    /// The login URL (public URL + `#token=` fragment) when a tunnel is up with a
    /// public URL, else `None`. Reuses the migrated [`login_url`].
    pub fn login(&self) -> Option<String> {
        let status = self.status.as_ref()?;
        let public_url = status.public_url.as_deref()?;
        Some(login_url(public_url, status.access_token.as_deref()))
    }

    /// GET the current tunnel status from the loopback control plane. Errors
    /// (daemon unreachable, etc.) land in `message` — never a panic.
    pub fn refresh(&mut self) {
        match call_remote_status(self.control_port) {
            Ok(status) => {
                self.message = Some(describe_status(&status));
                self.status = Some(status);
            }
            Err(e) => {
                self.message = Some(explain_control_error(
                    "status failed",
                    self.control_port,
                    &e,
                ));
            }
        }
    }

    /// Start remote access via the loopback control plane (`POST /api/remote/start`)
    /// using the daemon's PRE-CONFIGURED tunnel: an empty provider + field map tells
    /// the daemon to fall back to its `lucarned.yaml` config (G3). Thin wrapper over
    /// [`Self::start_with`] with empty params — kept as the "just go public with
    /// whatever the daemon already has" convenience.
    ///
    /// The interactive `s` key no longer calls this directly: the [`App`] bridges
    /// the Config panel's live params into [`Self::start_with`] (PART 1), and an
    /// empty Config yields empty params (the same daemon-default fallback). This
    /// method stays for callers / future entry points that want the daemon default
    /// unconditionally without consulting the Config panel.
    ///
    /// [`App`]: crate::tui::app::App
    #[allow(dead_code)]
    pub fn start(&mut self) {
        self.start_with(String::new(), std::collections::BTreeMap::new());
    }

    /// Start remote access via the loopback control plane (`POST /api/remote/start`)
    /// with an EXPLICIT provider + field map (PART 1: wired from the Config panel's
    /// live in-TUI edits via [`crate::tui::config::ConfigPanel::start_params`]). The
    /// daemon merges these over its pre-config (G3), so "configure in Config, then
    /// `s` in Go Public" works without saving `lucarned.yaml` first.
    ///
    /// An EMPTY `provider` (with empty `fields`) means "use the daemon's
    /// pre-configured tunnel" (the v1 behavior). When a provider id IS set it is
    /// validated against its descriptor first; an invalid config surfaces inline in
    /// `message` and NOTHING is sent. On success the returned public URL + token are
    /// stored and the QR modal is opened. Other errors land in `message`.
    pub fn start_with(
        &mut self,
        provider: String,
        fields: std::collections::BTreeMap<String, String>,
    ) {
        // Validate a non-empty provider's config against its descriptor before
        // touching the network, so a missing required field is caught inline rather
        // than as a daemon 400 (the provider boundary stays in lucarne_remote).
        if !provider.is_empty() {
            if let Err(detail) = validate_start_config(&provider, &fields) {
                self.message = Some(format!("start blocked — {detail}"));
                return;
            }
        }
        let plan = GoPublicPlan {
            provider,
            fields,
            control_url: control_url(self.control_port, "start"),
        };
        match call_remote_start(&plan) {
            Ok(status) => {
                self.message = Some(describe_status(&status));
                let has_login = status.public_url.is_some();
                self.status = Some(status);
                // Open the QR modal when there is something to scan.
                self.qr_open = has_login;
            }
            Err(e) => {
                self.message = Some(explain_control_error("start failed", self.control_port, &e));
            }
        }
    }

    /// Stop remote access (`POST /api/remote/stop`). Closes the QR modal and
    /// records the resulting status. Errors land in `message`.
    pub fn stop(&mut self) {
        match call_remote_stop(self.control_port) {
            Ok(status) => {
                self.message = Some(describe_status(&status));
                self.status = Some(status);
                self.qr_open = false;
            }
            Err(e) => {
                self.message = Some(explain_control_error("stop failed", self.control_port, &e));
            }
        }
    }

    /// Handle a key for the Go-Public panel. `x` stop, `r` refresh status, `Enter`
    /// (re)open the login QR modal when a login is available, and `Esc`/`q` close
    /// the modal.
    ///
    /// NOTE: the `s` (start) key is deliberately NOT handled here — the [`App`] owns
    /// both this panel and the Config panel, so it intercepts `s` and calls
    /// [`Self::start_with`] using the Config panel's live
    /// [`start_params`](crate::tui::config::ConfigPanel::start_params) (PART 1).
    /// This handler can therefore stay panel-local with no return value (MNT-005:
    /// the single-variant `GoPublicAction` is gone).
    ///
    /// [`App`]: crate::tui::app::App
    pub fn handle_key(&mut self, code: crossterm::event::KeyCode) {
        use crossterm::event::KeyCode;

        // While the modal is open, Esc/q just closes it (the loop maps q→quit only
        // when no panel consumes it; here the panel consumes it to close the QR).
        if self.qr_open {
            if matches!(code, KeyCode::Esc | KeyCode::Char('q') | KeyCode::Enter) {
                self.qr_open = false;
            }
            return;
        }
        match code {
            KeyCode::Char('x') => self.stop(),
            KeyCode::Char('r') => self.refresh(),
            KeyCode::Enter => {
                if self.login().is_some() {
                    self.qr_open = true;
                } else {
                    self.message =
                        Some("no public URL yet — press s to start remote access".to_string());
                }
            }
            _ => {}
        }
    }
}

/// One-line human summary of a tunnel status for the panel body / message line.
fn describe_status(status: &lucarne_remote_status::RemoteStartStatus) -> String {
    if !status.running {
        return "remote access: stopped".to_string();
    }
    let provider = status.provider.as_deref().unwrap_or("(unknown provider)");
    let url = status.public_url.as_deref().unwrap_or("(no public URL)");
    let token = match status.access_token.as_deref() {
        Some(t) if !t.is_empty() => "token: set",
        _ => "token: none",
    };
    format!("remote access: running via {provider} — {url} — {token}")
}

/// Render the centered login-QR modal over `area` (Decision 4).
///
/// The QR is drawn with an EXPLICIT high-contrast `Style` —
/// `fg(Color::Black).bg(Color::White)` — so it stays scannable regardless of the
/// terminal theme (never inheriting a dark background that would invert it). If
/// the modal's inner rect (border-stripped) is smaller than the QR grid we FALL
/// BACK to the plain login URL + a "terminal too small to display QR" hint so the
/// layout never breaks. The access token / public URL are shown by the panel body
/// (this function only owns the modal).
pub fn render_qr_modal(frame: &mut Frame, panel: &GoPublicPanel, area: Rect) {
    let Some(login) = panel.login() else {
        return;
    };

    // High-contrast QR style, independent of the terminal theme (Decision 4).
    let qr_style = Style::default().fg(Color::Black).bg(Color::White);
    let block = Block::default()
        .borders(Borders::ALL)
        .title("Scan to open (Esc to close)");

    match render_terminal_qr(&login) {
        Ok(qr) => {
            let grid = QrGrid::measure(&qr);
            // Modal size = QR grid + the 2-cell border on each axis, clamped to area.
            let modal = centered_rect(grid.cols + 2, grid.rows + 2, area);
            let inner = modal.inner(ratatui::layout::Margin::new(1, 1));

            // Always clear the area we draw over so the underlying body never bleeds
            // through the white QR background.
            frame.render_widget(Clear, modal);

            if grid.fits_within(inner) {
                let lines: Vec<Line> = qr.lines().map(|l| Line::styled(l, qr_style)).collect();
                let qr_widget = Paragraph::new(Text::from(lines))
                    .block(block)
                    .style(qr_style);
                frame.render_widget(qr_widget, modal);
            } else {
                render_qr_fallback(frame, &login, area, block);
            }
        }
        Err(e) => {
            // QR generation failed (e.g. data too long) — show the plain URL.
            // COR-004: size the modal by the login's CHAR count (matching
            // `QrGrid::measure`), not its byte length — a multi-byte URL would
            // otherwise over-size the modal via the lossy `len() as u16`.
            let body = format!("QR render failed: {e}\n\nlogin URL:\n{login}");
            let login_cols = login.chars().count().min(u16::MAX as usize) as u16;
            let modal = centered_rect(login_cols.saturating_add(4).min(area.width), 6, area);
            frame.render_widget(Clear, modal);
            let widget = Paragraph::new(body)
                .block(block)
                .wrap(ratatui::widgets::Wrap { trim: false });
            frame.render_widget(widget, modal);
        }
    }
}

/// Fallback modal when the terminal is too small for the QR grid: a centered box
/// showing the plain login URL + a hint to open it manually (Decision 4). Wraps
/// so a long URL never overflows the (small) box.
fn render_qr_fallback(frame: &mut Frame, login: &str, area: Rect, block: Block) {
    let body = format!("terminal too small for QR — open this URL:\n\n{login}");
    // A modest box; the URL wraps inside it.
    let w = area.width.saturating_sub(2).max(1);
    let h = 7u16.min(area.height);
    let modal = centered_rect(w, h, area);
    frame.render_widget(Clear, modal);
    let widget = Paragraph::new(body)
        .block(block)
        .wrap(ratatui::widgets::Wrap { trim: false });
    frame.render_widget(widget, modal);
}

#[cfg(test)]
mod go_public_tests {
    use super::*;

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn blocking_control_plane_call_runs_off_runtime_without_panic() {
        // Regression for the `lucarned tui` startup panic: the control-plane
        // calls use `reqwest::blocking`, which builds+drops its own Tokio
        // runtime. Dropping a runtime inside an async (tokio) context panics
        // ("Cannot drop a runtime in a context where blocking is not allowed").
        // Production runs the TUI on a dedicated thread with no ambient runtime;
        // mirror that here. No daemon is needed — a connection error is fine, the
        // bug was on runtime DROP which happens regardless of success.
        let handle = std::thread::spawn(|| {
            // Port 1: nothing listening → fast connection error; the point is to
            // exercise the reqwest::blocking runtime build+drop, not the response.
            let _ = call_remote_status(1);
        });
        handle
            .join()
            .expect("a reqwest::blocking control-plane call on a dedicated thread must not panic");
    }

    #[test]
    fn explain_control_error_guides_to_daemon_on_connectivity_failure() {
        // Connectivity failure (call_remote_* prefix "failed to reach daemon") →
        // actionable hint to start the daemon, not a raw reqwest error.
        let connectivity = explain_control_error(
            "status failed",
            7801,
            "failed to reach daemon at http://127.0.0.1:7801/api/remote/status: error sending request",
        );
        assert!(connectivity.contains("daemon isn't running"));
        assert!(connectivity.contains("lucarned autostart start"));
        assert!(connectivity.contains("7801"));
        // Non-connectivity errors pass through verbatim under the op label.
        let other = explain_control_error("start failed", 7801, "daemon returned 400: bad config");
        assert_eq!(other, "start failed: daemon returned 400: bad config");
    }

    #[test]
    fn control_port_defaults_to_gateway_port_plus_one() {
        // SEC-002: the CLI derives the control port from the gateway port + 1,
        // matching the daemon's default off-tunnel control listener.
        assert_eq!(default_control_port(7800), Some(7801));
        assert_eq!(default_control_port(9000), Some(9001));
        // L1: port 65535 + 1 overflows → None (explicit --control-port required).
        assert_eq!(default_control_port(65535), None);
    }

    #[test]
    fn built_in_providers_are_enumerable() {
        // Mirrors the interactive-selection source of truth: the CLI lists what
        // RemoteRegistry::builtin() advertises.
        let registry = lucarne_remote::builtin();
        assert!(registry.ids().contains(&"cloudflared"));
    }

    #[test]
    fn login_url_appends_token_fragment() {
        assert_eq!(
            login_url("https://demo.example.test", Some("secret123")),
            "https://demo.example.test#token=secret123"
        );
        assert_eq!(
            login_url("https://demo.example.test", None),
            "https://demo.example.test"
        );
        // Empty token is treated as absent.
        assert_eq!(
            login_url("https://demo.example.test", Some("")),
            "https://demo.example.test"
        );
    }

    #[test]
    fn qr_renders_for_login_url() {
        let url = login_url("https://demo.example.test", Some("k"));
        let qr = render_terminal_qr(&url).expect("qr renders");
        // Half-block QR uses the block glyphs from the wechat renderer.
        assert!(qr.contains('█'));
        assert!(qr.lines().count() > 3);
    }

    // ---- TASK-003: Go-Public panel pure logic ----

    fn status(
        running: bool,
        url: Option<&str>,
        token: Option<&str>,
    ) -> lucarne_remote_status::RemoteStartStatus {
        lucarne_remote_status::RemoteStartStatus {
            running,
            provider: Some("cloudflared".to_string()),
            public_url: url.map(str::to_string),
            access_token: token.map(str::to_string),
        }
    }

    #[test]
    fn panel_defaults_to_control_port_7801() {
        // Default control port = gateway 7800 + 1, matching the daemon default and
        // the migrated derivation. start() must target that loopback /start route.
        let panel = GoPublicPanel::new();
        assert_eq!(panel.control_port, 7801);
        assert_eq!(
            control_url(panel.control_port, "start"),
            "http://127.0.0.1:7801/api/remote/start"
        );
        assert_eq!(
            control_url(panel.control_port, "stop"),
            "http://127.0.0.1:7801/api/remote/stop"
        );
        assert_eq!(
            control_url(panel.control_port, "status"),
            "http://127.0.0.1:7801/api/remote/status"
        );
    }

    #[test]
    fn panel_login_builds_token_fragment_only_when_url_present() {
        let mut panel = GoPublicPanel::new();
        // No status yet → no login string.
        assert!(panel.login().is_none());
        // Running with a URL + token → the `#token=` fragment login (reusing login_url).
        panel.status = Some(status(
            true,
            Some("https://demo.example.test"),
            Some("secret"),
        ));
        assert_eq!(
            panel.login().as_deref(),
            Some("https://demo.example.test#token=secret")
        );
        // Running but no public URL → no login string.
        panel.status = Some(status(true, None, Some("secret")));
        assert!(panel.login().is_none());
    }

    #[test]
    fn qr_grid_measure_is_rectangular_and_counts_rows() {
        // 3 equal-width rows + a blank line that must be ignored.
        let qr = "███\n█ █\n███\n\n";
        let grid = QrGrid::measure(qr);
        assert_eq!(grid.rows, 3);
        assert_eq!(grid.cols, 3);
        // Empty input → zero grid (degenerate but well-defined).
        assert_eq!(QrGrid::measure(""), QrGrid { cols: 0, rows: 0 });
    }

    #[test]
    fn qr_fits_within_decision_picks_qr_vs_fallback() {
        let grid = QrGrid { cols: 10, rows: 8 };
        // Inner rect exactly the grid size → fits (renders the QR).
        assert!(grid.fits_within(Rect::new(0, 0, 10, 8)));
        // One column short → does NOT fit (falls back to the plain URL).
        assert!(!grid.fits_within(Rect::new(0, 0, 9, 8)));
        // One row short → does NOT fit.
        assert!(!grid.fits_within(Rect::new(0, 0, 10, 7)));
        // Larger than the grid → fits.
        assert!(grid.fits_within(Rect::new(0, 0, 40, 20)));
    }

    #[test]
    fn real_login_qr_fit_decision_against_small_and_large_rects() {
        // The actual login QR for a representative URL: small terminals fall back,
        // a roomy terminal renders the scannable grid.
        let login = login_url("https://demo.example.test", Some("k"));
        let qr = render_terminal_qr(&login).expect("qr renders");
        let grid = QrGrid::measure(&qr);
        assert!(grid.cols > 0 && grid.rows > 0);
        // A tiny inner rect cannot hold the grid → fallback.
        assert!(!grid.fits_within(Rect::new(0, 0, 4, 4)));
        // A generous inner rect holds it → render the QR.
        assert!(grid.fits_within(Rect::new(0, 0, grid.cols, grid.rows)));
    }

    #[test]
    fn centered_rect_centers_and_clamps_to_area() {
        let area = Rect::new(0, 0, 80, 24);
        // A 20x10 modal centers inside an 80x24 area.
        let r = centered_rect(20, 10, area);
        assert_eq!((r.width, r.height), (20, 10));
        assert_eq!(r.x, (80 - 20) / 2);
        assert_eq!(r.y, (24 - 10) / 2);
        // A request larger than the area is clamped to the area (never overflows).
        let big = centered_rect(200, 200, area);
        assert_eq!((big.width, big.height), (80, 24));
        assert_eq!((big.x, big.y), (0, 0));
    }

    #[test]
    fn describe_status_summarizes_running_and_stopped() {
        let stopped = describe_status(&status(false, None, None));
        assert_eq!(stopped, "remote access: stopped");
        let running = describe_status(&status(true, Some("https://demo.example.test"), Some("k")));
        assert!(running.contains("running via cloudflared"));
        assert!(running.contains("https://demo.example.test"));
        assert!(running.contains("token: set"));
        // Running without a token reports token: none.
        let no_token = describe_status(&status(true, Some("https://demo.example.test"), None));
        assert!(no_token.contains("token: none"));
    }

    #[test]
    fn modal_key_opens_and_closes_qr() {
        use crossterm::event::KeyCode;
        let mut panel = GoPublicPanel::new();
        panel.status = Some(status(true, Some("https://demo.example.test"), Some("k")));
        // Enter opens the modal when a login is available.
        panel.handle_key(KeyCode::Enter);
        assert!(panel.qr_open);
        // While open, Esc closes it.
        panel.handle_key(KeyCode::Esc);
        assert!(!panel.qr_open);
        // Enter with no login available sets a hint instead of opening.
        let mut empty = GoPublicPanel::new();
        empty.handle_key(KeyCode::Enter);
        assert!(!empty.qr_open);
        assert!(empty.message.is_some());
    }
}
