//! monitor — the live rmux-sdk binding: connect to the SYSTEM rmux daemon, adopt
//! its sessions, mirror each pane (render_stream → adapter → fan-out), and inject
//! input (send_text / send_key).
//!
//! ## Monitor model
//! We connect to the DEFAULT system socket — the same daemon the user's own
//! `rmux` CLI uses — and treat its sessions as `Origin::Adopted` (we observe; we
//! don't own them). Sessions created via [`RmuxMonitor::create`] register as
//! `Origin::Managed`. The SDK is a control-mode observer; it coexists with a CLI
//! `attach-session` client on the same session (proven in spike4-attach-handoff),
//! which is exactly what "pop out into a local terminal / retract" relies on.
//!
//! ## Boundary
//! This module names `rmux_sdk` runtime handles (`Rmux`/`Pane`/`Session`); every
//! value it emits is the stable terminal vocabulary re-exported by this crate.

use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;

use rmux_sdk::{
    EnsureSession, EnsureSessionPolicy, Pane, ProcessSpec, Rmux, SessionName, TerminalSizeSpec,
};
use tokio::sync::{broadcast, Mutex};
use tokio::task::JoinHandle;

use crate::cli;
use crate::term::{
    control_key_token, key_token, primary_pane_session_id, validate_primary_pane_session_id,
    Cursor, Dims, Origin, PaneGrid, SessionDescriptor, SessionId, SessionRegistry, TermInput,
};

use crate::adapter;

const CONNECT_TIMEOUT: Duration = Duration::from_secs(5);
const DEFAULT_COLS: u16 = 120;
const DEFAULT_ROWS: u16 = 32;
/// Bounded scrollback window for [`RmuxMonitor::capture_scrollback`]: the most
/// recent `SCROLLBACK_CAPTURE_LINES` lines, never the whole pane. ADR
/// `2026-05-30-rmux-terminal-monitor-subsystem.md` boundary rule 5 requires
/// terminal scrollback/history reads to be bounded windows, not whole-pane scans.
pub const SCROLLBACK_CAPTURE_LINES: u32 = 1000;
const _: () = assert!(
    SCROLLBACK_CAPTURE_LINES > 0 && SCROLLBACK_CAPTURE_LINES <= 1000,
    "scrollback capture must be a bounded, sane window"
);
/// Fan-out buffer for mirror grid updates. A slow subscriber lags (and gets a
/// `RecvError::Lagged`) rather than back-pressuring the source loops.
const GRID_BROADCAST_CAP: usize = 256;

/// Errors from the rmux binding. rmux-sdk errors are flattened to their `Display`
/// string so the preview SDK's concrete error type never leaks across the
/// boundary (matches Lucarne's `thiserror` convention; no `anyhow` in the lib).
#[derive(Debug, thiserror::Error)]
pub enum MonitorError {
    #[error("rmux: {0}")]
    Rmux(String),
    #[error("session not tracked: {0}")]
    NotFound(SessionId),
    #[error("invalid session name: {0}")]
    InvalidName(String),
}

type Result<T> = std::result::Result<T, MonitorError>;

/// `rmux capture-pane -S` start argument for bounded archive/scrollback reads.
///
/// Keep this shared between the live gateway monitor and the TUI archive path so
/// every terminal-history capture uses the same capped window.
pub fn scrollback_capture_start_arg() -> String {
    format!("-{SCROLLBACK_CAPTURE_LINES}")
}

/// A fresh full grid for one monitored pane, fanned out to mirror subscribers.
/// (The differ that turns these into deltas lives downstream, per-client, in the
/// gateway — the monitor publishes full frames.)
#[derive(Clone, Debug)]
pub struct GridUpdate {
    pub session: SessionId,
    pub grid: PaneGrid,
    pub cursor: Cursor,
}

/// A pane we mirror, plus the rmux session name needed for lifecycle ops (kill).
struct Tracked {
    pane: Pane,
    name: SessionName,
}

/// Live handle to the monitored SYSTEM rmux daemon.
pub struct RmuxMonitor {
    rmux: Rmux,
    registry: Arc<Mutex<SessionRegistry>>,
    tracked: Arc<Mutex<HashMap<SessionId, Tracked>>>,
    updates: broadcast::Sender<GridUpdate>,
    /// JoinHandles for every detached per-pane source loop spawned by
    /// [`RmuxMonitor::spawn_source_loop`]. Held so they can be aborted when the
    /// monitor is dropped (Drop-based abort, mirroring
    /// `lucarne_adapter::AdapterSupervisorHandle::drop`): without this the loops
    /// would leak — they only `break` when rmux closes their render stream, so a
    /// dropped monitor whose streams stay open would otherwise spin tasks forever.
    source_loops: Arc<Mutex<Vec<JoinHandle<()>>>>,
}

/// Abort every spawned source loop when the monitor is dropped (mirrors
/// `AdapterSupervisorHandle::drop`'s task abort). Each loop's only self-exit is
/// "rmux closed the render stream"; aborting here guarantees the detached tasks
/// are reclaimed promptly on monitor teardown rather than leaking until the
/// runtime stops. `try_lock` (not blocking) keeps `Drop` non-blocking; the
/// `tracked`/`source_loops` mutex is uncontended at drop time (no other holder
/// remains once the monitor is being dropped), so the abort lands in practice.
impl Drop for RmuxMonitor {
    fn drop(&mut self) {
        if let Ok(loops) = self.source_loops.try_lock() {
            for handle in loops.iter() {
                handle.abort();
            }
        }
    }
}

impl RmuxMonitor {
    /// Connect to the system rmux daemon (default socket — the daemon the user's
    /// own `rmux` uses). Starts the hidden daemon if none is running.
    pub async fn connect() -> Result<Self> {
        let rmux = Rmux::builder()
            .default_timeout(CONNECT_TIMEOUT)
            .connect_or_start()
            .await
            .map_err(|e| MonitorError::Rmux(format!("connect_or_start: {e}")))?;
        tracing::info!(
            target: "lucarne_rmux",
            endpoint = ?rmux.endpoint(),
            "connected to system rmux daemon"
        );
        let (updates, _) = broadcast::channel(GRID_BROADCAST_CAP);
        Ok(Self {
            rmux,
            registry: Arc::new(Mutex::new(SessionRegistry::new())),
            tracked: Arc::new(Mutex::new(HashMap::new())),
            updates,
            source_loops: Arc::new(Mutex::new(Vec::new())),
        })
    }

    /// Subscribe to the fan-out of fresh pane grids (one stream, all sessions).
    pub fn subscribe(&self) -> broadcast::Receiver<GridUpdate> {
        self.updates.subscribe()
    }

    /// Snapshot of the registry (what the CLI `ls` / gateway `SessionList` read).
    pub async fn sessions(&self) -> Vec<SessionDescriptor> {
        self.registry.lock().await.list()
    }

    /// Adopt every pre-existing session on the system daemon (monitor model):
    /// register each as `Origin::Adopted`, open its pane, spawn its source loop.
    pub async fn adopt_all(&self) -> Result<Vec<SessionDescriptor>> {
        let names = self
            .rmux
            .list_sessions()
            .await
            .map_err(|e| MonitorError::Rmux(format!("list_sessions: {e}")))?;
        let mut out = Vec::new();
        for name in names {
            let title = name.as_str().to_string();
            match self.track(name, Origin::Adopted, title).await {
                Ok(desc) => out.push(desc),
                Err(e) => tracing::warn!(
                    target: "lucarne_rmux",
                    error = %e,
                    "adopt failed; skipping session"
                ),
            }
        }
        Ok(out)
    }

    /// Create a new shell session on the system daemon (registers as Managed).
    pub async fn create(&self, title: impl Into<String>) -> Result<SessionDescriptor> {
        let raw = unique_session_name();
        let name =
            SessionName::new(&raw).map_err(|e| MonitorError::InvalidName(format!("{raw}: {e}")))?;
        self.rmux
            .ensure_session(
                EnsureSession::named(name.clone())
                    .policy(EnsureSessionPolicy::CreateOrReuse)
                    .detached(true)
                    .size(TerminalSizeSpec::new(DEFAULT_COLS, DEFAULT_ROWS))
                    .process(ProcessSpec::argv([default_shell()])),
            )
            .await
            .map_err(|e| MonitorError::Rmux(format!("ensure_session: {e}")))?;
        self.track(name, Origin::Managed, title.into()).await
    }

    /// One-shot full grid for a tracked session (gateway resync / CLI peek).
    pub async fn snapshot_grid(&self, id: &SessionId) -> Result<(PaneGrid, Cursor)> {
        // Concurrency: clone out only the `Pane` handle under the `tracked` lock,
        // then DROP the guard before the rmux `.await`. Holding the global
        // `tracked` lock across the await would serialize snapshot/inject across
        // ALL sessions (one slow pane blocks every other session). `Pane` is a
        // cheap clonable handle, so this is behavior-preserving.
        let pane = {
            let guard = self.tracked.lock().await;
            let tracked = guard
                .get(id)
                .ok_or_else(|| MonitorError::NotFound(id.clone()))?;
            tracked.pane.clone()
        };
        let snap = pane
            .snapshot()
            .await
            .map_err(|e| MonitorError::Rmux(format!("snapshot {id}: {e}")))?;
        Ok((
            adapter::snapshot_to_grid(&snap),
            adapter::map_cursor(snap.cursor),
        ))
    }

    /// Inject input into a tracked pane. Resize is a hint only (#5).
    pub async fn inject(&self, id: &SessionId, input: TermInput) -> Result<()> {
        // Concurrency: clone the `Pane` under the lock and drop the guard before
        // the rmux `.await` (same rationale as `snapshot_grid`): never hold the
        // global `tracked` lock across a per-pane rmux await, so input to one
        // pane cannot block snapshots/input on every other session.
        let pane = {
            let guard = self.tracked.lock().await;
            let tracked = guard
                .get(id)
                .ok_or_else(|| MonitorError::NotFound(id.clone()))?;
            tracked.pane.clone()
        };
        match input {
            TermInput::Text { text } => pane
                .send_text(&text)
                .await
                .map_err(|e| MonitorError::Rmux(format!("send_text {id}: {e}")))?,
            TermInput::Key { code, mods } => pane
                .send_key(key_token(&code, mods))
                .await
                .map_err(|e| MonitorError::Rmux(format!("send_key(key) {id}: {e}")))?,
            TermInput::Control { key } => pane
                .send_key(control_key_token(&key))
                .await
                .map_err(|e| MonitorError::Rmux(format!("send_key {id}: {e}")))?,
            TermInput::ResizeHint { cols, rows } => {
                tracing::debug!(
                    target: "lucarne_rmux",
                    %id,
                    cols,
                    rows,
                    "resize hint (no PTY resize)"
                );
            }
        }
        Ok(())
    }

    /// Kill a tracked session on the daemon and drop it from the registry.
    pub async fn kill(&self, id: &SessionId) -> Result<()> {
        let name = {
            let guard = self.tracked.lock().await;
            guard
                .get(id)
                .map(|t| t.name.clone())
                .ok_or_else(|| MonitorError::NotFound(id.clone()))?
        };
        let session = self
            .rmux
            .session(name)
            .await
            .map_err(|e| MonitorError::Rmux(format!("open {id} for kill: {e}")))?;
        session
            .kill()
            .await
            .map_err(|e| MonitorError::Rmux(format!("kill {id}: {e}")))?;
        self.tracked.lock().await.remove(id);
        self.registry.lock().await.remove(id);
        Ok(())
    }

    /// Capture the recent scrollback of a session as text (for archiving), via
    /// the rmux CLI. Best-effort: a capture failure returns an error the caller
    /// can downgrade to empty content.
    ///
    /// ADR `2026-05-30-rmux-terminal-monitor-subsystem.md` (boundary rule 5):
    /// terminal scrollback reads must be BOUNDED windows, never whole-pane scans.
    /// So this captures the most recent [`SCROLLBACK_CAPTURE_LINES`] lines
    /// (`-S -<N>`) rather than the entire history (`-S -`). The CLI call goes
    /// through [`crate::cli::output_async`], which resolves the binary once and
    /// bounds the process wait with a timeout.
    pub async fn capture_scrollback(&self, id: &SessionId) -> Result<String> {
        let name = {
            let guard = self.tracked.lock().await;
            guard
                .get(id)
                .map(|t| t.name.as_str().to_string())
                .ok_or_else(|| MonitorError::NotFound(id.clone()))?
        };
        let start = scrollback_capture_start_arg();
        let out = cli::output_async(&["capture-pane", "-p", "-S", &start, "-t", &name])
            .await
            .map_err(|e| MonitorError::Rmux(format!("capture-pane: {e}")))?;
        if !out.status.success() {
            return Err(MonitorError::Rmux("capture-pane failed".to_string()));
        }
        Ok(String::from_utf8_lossy(&out.stdout).into_owned())
    }

    /// Open a session by rmux name, register it, and spawn its mirror loop.
    async fn track(
        &self,
        name: SessionName,
        origin: Origin,
        title: String,
    ) -> Result<SessionDescriptor> {
        let session = self
            .rmux
            .session(name.clone())
            .await
            .map_err(|e| MonitorError::Rmux(format!("open session {}: {e}", name.as_str())))?;
        let pane = session.pane(0, 0);
        let id = session_id(name.as_str());
        validate_primary_pane_session_id(&id)
            .map_err(|e| MonitorError::Rmux(format!("primary pane id: {e}")))?;

        // Seed dims from the first snapshot (best-effort; fall back to defaults).
        let dims = match pane.snapshot().await {
            Ok(snap) => Dims {
                cols: snap.cols,
                rows: snap.rows,
            },
            Err(e) => {
                tracing::debug!(
                    target: "lucarne_rmux",
                    %id,
                    error = %e,
                    "dims probe failed; using defaults"
                );
                Dims {
                    cols: DEFAULT_COLS,
                    rows: DEFAULT_ROWS,
                }
            }
        };

        let cwd = pane_cwd(name.as_str());
        let desc =
            self.registry
                .lock()
                .await
                .register_with_cwd(id.clone(), title, origin, dims, cwd);
        self.tracked.lock().await.insert(
            id.clone(),
            Tracked {
                pane: pane.clone(),
                name,
            },
        );
        // Detached source loop, but its JoinHandle is RETAINED (not dropped) so
        // `impl Drop for RmuxMonitor` can abort it on teardown — no leaked tasks.
        let handle = self.spawn_source_loop(id, pane);
        self.source_loops.lock().await.push(handle);
        Ok(desc)
    }

    /// Per-pane source loop: seed one full snapshot, then stream render updates
    /// (each a full `PaneSnapshot` — rmux has no native delta) into the fan-out.
    ///
    /// Returns the spawned task's [`JoinHandle`] so the caller can retain it for
    /// Drop-based abort (mirroring `AdapterSupervisorHandle`, which keeps its
    /// task handles and aborts them in `Drop`). The loop's only self-exit is
    /// "rmux closed the render stream"; without retaining + aborting the handle a
    /// dropped monitor with still-open streams would leak the task.
    fn spawn_source_loop(&self, id: SessionId, pane: Pane) -> JoinHandle<()> {
        let tx = self.updates.clone();
        tokio::spawn(async move {
            match pane.snapshot().await {
                Ok(snap) => {
                    let _ = tx.send(GridUpdate {
                        session: id.clone(),
                        grid: adapter::snapshot_to_grid(&snap),
                        cursor: adapter::map_cursor(snap.cursor),
                    });
                }
                Err(e) => tracing::warn!(
                    target: "lucarne_rmux",
                    session = %id,
                    error = %e,
                    "initial snapshot failed"
                ),
            }

            let mut stream = match pane.render_stream().await {
                Ok(s) => s,
                Err(e) => {
                    tracing::warn!(
                        target: "lucarne_rmux",
                        session = %id,
                        error = %e,
                        "render_stream unavailable; single snapshot only"
                    );
                    return;
                }
            };
            loop {
                match stream.next().await {
                    Ok(Some(update)) => {
                        let snap = update.snapshot();
                        let _ = tx.send(GridUpdate {
                            session: id.clone(),
                            grid: adapter::snapshot_to_grid(snap),
                            cursor: adapter::map_cursor(snap.cursor),
                        });
                    }
                    Ok(None) => {
                        tracing::info!(
                            target: "lucarne_rmux",
                            session = %id,
                            "render stream closed"
                        );
                        break;
                    }
                    Err(e) => {
                        tracing::warn!(
                            target: "lucarne_rmux",
                            session = %id,
                            error = %e,
                            "render stream error; ending loop"
                        );
                        break;
                    }
                }
            }
        })
    }
}

/// `session:window:pane` stable handle (window 0 / pane 0).
fn session_id(name: &str) -> SessionId {
    primary_pane_session_id(name)
}

/// The user's interactive shell (`$SHELL`, falling back to `/bin/sh`). A freshly
/// created rmux session must spawn this explicitly — an empty `ProcessSpec` gives
/// a dead pane (blank mirror, input goes nowhere).
fn default_shell() -> String {
    std::env::var("SHELL").unwrap_or_else(|_| "/bin/sh".to_string())
}

/// A process-unique session name for `create` (pid + monotonic counter — no
/// wall-clock / RNG so it stays deterministic within a run).
fn unique_session_name() -> String {
    static COUNTER: AtomicU64 = AtomicU64::new(0);
    let n = COUNTER.fetch_add(1, Ordering::Relaxed);
    format!("lucarne-{}-{}", std::process::id(), n)
}

/// Best-effort pane cwd via the rmux CLI (`#{pane_current_path}`). The SDK has no
/// cwd accessor, so we ask the same daemon over its tmux-compatible CLI.
fn pane_cwd(name: &str) -> Option<String> {
    let out = cli::output(&["display-message", "-p", "-t", name, "#{pane_current_path}"]).ok()?;
    if !out.status.success() {
        return None;
    }
    let s = String::from_utf8_lossy(&out.stdout).trim().to_string();
    (!s.is_empty()).then_some(s)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn session_id_shape() {
        assert_eq!(session_id("work"), "work:0:0");
    }

    #[test]
    fn unique_names_are_distinct() {
        let a = unique_session_name();
        let b = unique_session_name();
        assert_ne!(a, b);
        assert!(a.starts_with("lucarne-"));
    }

    // R3-4 / ADR boundary rule 5: scrollback capture must use a BOUNDED window
    // (`-S -<N>`), never the whole-pane `-S -`. Assert the capped constant and the
    // exact `-S` argument the CLI is invoked with so a regression to a full-pane
    // scan is caught without needing a live rmux daemon.
    #[test]
    fn scrollback_capture_window_is_bounded() {
        let start = format!("-{SCROLLBACK_CAPTURE_LINES}");
        assert_eq!(
            start, "-1000",
            "capture-pane -S start must bound the window"
        );
        assert_ne!(start, "-", "must NOT capture the whole pane (-S -)");
    }
}
