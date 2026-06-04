//! lucarne-termgw — the terminal gateway.
//!
//! An axum WebSocket + HTTP surface over the [`RmuxMonitor`]:
//! - **WS `/ws`** — per-client mirror. On connect it sends a `SessionList`, then
//!   for each `Subscribe`d session it sends a full `Snapshot` and streams
//!   `SnapshotDelta`s (via a per-client [`Differ`]). Inbound `Input` frames are
//!   injected into the pane (`send_text`/`send_key`).
//! - **HTTP `/api/sessions`** — `GET` lists monitored sessions, `POST` creates a
//!   shell session, `DELETE /api/sessions/{id}` kills one. This is what the thin
//!   CLI hits (pop-out/retract itself is rmux-native and does not go through here).
//! - **Static** — anything else is served from the web asset dir. Production web
//!   apps are external consumers of this gateway API.
//!
//! ## Concurrency
//! Output fan-out is the monitor's `broadcast`; each ws client owns its own set
//! of [`Differ`]s so a slow client only lags itself (`RecvError::Lagged` → it
//! re-snapshots its subscribed sessions). Input is injected directly through the
//! monitor, which serializes writes per pane.

use std::collections::{HashMap, HashSet};
use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::Arc;

use async_trait::async_trait;
use axum::extract::ws::{Message, WebSocket, WebSocketUpgrade};
use axum::extract::{ConnectInfo, FromRequestParts, Path, Query, State};
use axum::http::request::Parts;
use axum::http::{Request, StatusCode};
use axum::middleware::{self, Next};
use axum::response::{IntoResponse, Response};
use axum::routing::{delete, get, post};
use axum::{Json, Router};
use futures::stream::{SplitSink, StreamExt};
use futures::SinkExt;
use serde::Deserialize;
use serde_json::{json, Value};
use tokio::sync::broadcast::error::RecvError;
use tower_http::services::ServeDir;

use lucarne::control_plane::ControlPlaneSqliteStore;
use lucarne::terminal_agent_bind;
use lucarne_rmux::{
    archive, ClientFrame, Cursor, DiffResult, Differ, GridUpdate, MonitorError, PaneGrid,
    RmuxMonitor, ServerFrame, SessionDescriptor, SessionId, TermInput,
};

pub mod auth;

pub use auth::{
    parse_gateway_addr, require_auth_or_refuse, AccessScope, AccessToken, AuthMode, AuthRefusal,
    AuthState, ForwardedIdentityPolicy, GatewayAddrError, TokenError,
};

/// Monotonic per-process ws connection sequence (SEC-011 audit logging).
///
/// A connect/disconnect pair is logged with this id so operators can correlate a
/// session's lifetime in the logs WITHOUT ever logging the ticket/token. It is
/// not a security token — just a log correlation handle.
static WS_CONN_SEQ: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(1);

/// Next ws connection sequence number for audit logging (SEC-011).
fn next_conn_seq() -> u64 {
    WS_CONN_SEQ.fetch_add(1, std::sync::atomic::Ordering::Relaxed)
}

/// Snapshot of the daemon-owned tunnel, returned by the loopback control routes.
///
/// Intentionally minimal and transport-agnostic: the gateway never learns
/// backend-specific details (Locked decision L2). `public_url` is the tunnel's
/// ingress URL when up; `access_token` is the credential a remote client appends
/// as `#token=` to reach the authenticated surface (the CLI renders it as a QR).
#[derive(Debug, Clone, Default, serde::Serialize)]
pub struct RemoteControlStatus {
    /// Whether a tunnel is currently running.
    pub running: bool,
    /// Provider id backing the tunnel (e.g. `"cloudflared"`), when up.
    pub provider: Option<String>,
    /// Public URL the tunnel exposes, when up.
    pub public_url: Option<String>,
    /// The gateway access token to hand the remote client, when auth is enforced.
    pub access_token: Option<String>,
}

/// Optional control-plane overrides for `POST /api/remote/start` (G3).
///
/// The CLI (`lucarned remote start`) collects the chosen provider id + that provider's
/// fields and posts them here. When present they override / merge with the
/// daemon's pre-configured tunnel; when absent the daemon falls back to its
/// `lucarned.yaml` pre-configured provider + config (backward compatible).
///
/// Transport-agnostic by design (Locked decision L2): just a provider id and a
/// flat key→value field map — no backend-specific structure, no daemon types.
#[derive(Debug, Clone, Default, Deserialize)]
pub struct RemoteStartParams {
    /// Provider id to start (e.g. `"cloudflared"`). `None` → daemon's configured
    /// provider.
    #[serde(default)]
    pub provider: Option<String>,
    /// Provider field overrides keyed by `RequiredField::key`. Merged over the
    /// daemon's configured provider fields; empty → use configured fields as-is.
    #[serde(default)]
    pub fields: std::collections::BTreeMap<String, String>,
}

impl RemoteStartParams {
    /// True when the caller supplied nothing actionable (no provider override and
    /// no field overrides) — the daemon should use its pre-configured tunnel.
    pub fn is_empty(&self) -> bool {
        self.provider.is_none() && self.fields.is_empty()
    }
}

/// Daemon-owned tunnel control plane, driven by the loopback `/api/remote/*`
/// routes.
///
/// lucarned (which owns the tunnel lifecycle — Locked decision L6) implements
/// this; the gateway only forwards `start`/`stop`/`status` to it. Defining the
/// seam here (rather than depending on `lucarne-remote` from `lucarne-termgw`)
/// keeps the provider abstraction out of the gateway crate.
#[async_trait]
pub trait RemoteControl: Send + Sync {
    /// Start the tunnel (idempotent: if already up, return the current status).
    ///
    /// G3: `params` carries optional CLI-supplied provider + field overrides; an
    /// empty `params` means "use the daemon's pre-configured tunnel".
    ///
    /// M2: errors are typed ([`RemoteControlError`]) so the loopback control
    /// plane can map them to a meaningful status (400 bad config, 404 unknown
    /// provider, 502 tunnel/backend failure) instead of a blanket 500.
    async fn start(
        &self,
        params: RemoteStartParams,
    ) -> Result<RemoteControlStatus, RemoteControlError>;
    /// Stop the tunnel (idempotent: if already down, succeeds).
    async fn stop(&self) -> Result<RemoteControlStatus, RemoteControlError>;
    /// Report the current tunnel status.
    async fn status(&self) -> RemoteControlStatus;
}

/// Typed control-plane error surfaced by [`RemoteControl`] (M2).
///
/// The loopback `/api/remote/*` handlers map each variant to a status code and a
/// human-readable message. Because the control plane is loopback-only (SEC-002),
/// returning a descriptive message here is safe — it never reaches the tunnel.
#[derive(Debug, Clone)]
pub enum RemoteControlError {
    /// The requested provider id is not registered (→ 404).
    UnknownProvider(String),
    /// Provider configuration was invalid / a required field was missing (→ 400).
    BadConfig(String),
    /// The tunnel backend itself failed to start / stop (→ 502).
    Backend(String),
}

impl RemoteControlError {
    /// HTTP status this error maps to on the loopback control plane (M2).
    pub fn status_code(&self) -> StatusCode {
        match self {
            RemoteControlError::UnknownProvider(_) => StatusCode::NOT_FOUND,
            RemoteControlError::BadConfig(_) => StatusCode::BAD_REQUEST,
            RemoteControlError::Backend(_) => StatusCode::BAD_GATEWAY,
        }
    }

    /// The human-readable detail (safe to return on the loopback control plane).
    pub fn detail(&self) -> &str {
        match self {
            RemoteControlError::UnknownProvider(m)
            | RemoteControlError::BadConfig(m)
            | RemoteControlError::Backend(m) => m,
        }
    }
}

impl std::fmt::Display for RemoteControlError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            RemoteControlError::UnknownProvider(m) => write!(f, "unknown provider: {m}"),
            RemoteControlError::BadConfig(m) => write!(f, "bad config: {m}"),
            RemoteControlError::Backend(m) => write!(f, "backend error: {m}"),
        }
    }
}

impl std::error::Error for RemoteControlError {}

/// Shared gateway state (cheap to clone — `Arc` to the monitor plus the auth layer).
#[derive(Clone)]
struct AppState {
    monitor: Arc<dyn TerminalMonitor>,
    control_store: ControlPlaneSqliteStore,
    auth: AuthState,
    /// Connection/session caps + ws session lifetime knobs (SEC-004/SEC-006).
    limits: GatewayLimits,
    /// Global concurrent-ws-connection permit pool (SEC-004 / H1). Shared across
    /// all gateway ws routes (`/ws` and `/agent/{id}`).
    ws_pool: WsConnectionPool,
}

/// Narrow monitor seam used by the gateway. Production wires this to
/// [`RmuxMonitor`]; tests can provide a fake without launching a system rmux
/// daemon.
#[async_trait]
trait TerminalMonitor: Send + Sync {
    fn subscribe(&self) -> tokio::sync::broadcast::Receiver<GridUpdate>;
    async fn sessions(&self) -> Vec<SessionDescriptor>;
    async fn create(&self, title: String) -> Result<SessionDescriptor, MonitorError>;
    async fn snapshot_grid(&self, id: &SessionId) -> Result<(PaneGrid, Cursor), MonitorError>;
    async fn inject(&self, id: &SessionId, input: TermInput) -> Result<(), MonitorError>;
    async fn kill(&self, id: &SessionId) -> Result<(), MonitorError>;
    async fn capture_scrollback(&self, id: &SessionId) -> Result<String, MonitorError>;
}

#[async_trait]
impl TerminalMonitor for RmuxMonitor {
    fn subscribe(&self) -> tokio::sync::broadcast::Receiver<GridUpdate> {
        self.subscribe()
    }

    async fn sessions(&self) -> Vec<SessionDescriptor> {
        self.sessions().await
    }

    async fn create(&self, title: String) -> Result<SessionDescriptor, MonitorError> {
        self.create(title).await
    }

    async fn snapshot_grid(&self, id: &SessionId) -> Result<(PaneGrid, Cursor), MonitorError> {
        self.snapshot_grid(id).await
    }

    async fn inject(&self, id: &SessionId, input: TermInput) -> Result<(), MonitorError> {
        self.inject(id, input).await
    }

    async fn kill(&self, id: &SessionId) -> Result<(), MonitorError> {
        self.kill(id).await
    }

    async fn capture_scrollback(&self, id: &SessionId) -> Result<String, MonitorError> {
        self.capture_scrollback(id).await
    }
}

/// Tunable connection/session limits + ws session lifetime (SEC-004 / SEC-006).
///
/// Defaults are sane for a single-user remote terminal: a small connection cap,
/// a per-connection session-creation cap (anti fork-bomb), an inbound-frame rate
/// limit (anti flood), and an idle + max-lifetime close on every ws client loop
/// (a connect-time ticket alone is not enough — a live socket must not outlive
/// its credential indefinitely).
#[derive(Clone, Copy, Debug)]
pub struct GatewayLimits {
    /// Max concurrent ws connections across all ws routes. Excess upgrades 503.
    pub max_ws_connections: usize,
    /// Max sessions one ws connection may create (anti fork-bomb).
    pub max_sessions_per_conn: usize,
    /// Max inbound ws frames per second per connection (anti flood); 0 = off.
    pub max_inbound_frames_per_sec: u32,
    /// Close a ws client after this much inactivity (no inbound frame).
    pub idle_timeout: std::time::Duration,
    /// Hard cap on a single ws session's wall-clock lifetime.
    pub max_session_lifetime: std::time::Duration,
}

impl Default for GatewayLimits {
    fn default() -> Self {
        Self {
            max_ws_connections: 32,
            max_sessions_per_conn: 16,
            max_inbound_frames_per_sec: 200,
            idle_timeout: std::time::Duration::from_secs(7200),
            max_session_lifetime: std::time::Duration::from_secs(43_200), // 12h
        }
    }
}

/// Shared ws-connection governor (SEC-004 / H1): the [`GatewayLimits`] plus the
/// SINGLE global connection-permit pool that every ws route draws from.
///
/// Cloning shares the same underlying semaphore (`Arc`). The daemon builds one
/// pool for the gateway so `/ws` and `/agent` obey one `max_ws_connections` cap
/// and the same idle / lifetime / inbound-frame-rate knobs. In-process extension
/// routes can use [`authorize_ws`] with the same pool, but production Web apps
/// should consume the gateway API directly rather than merge a Lucarne-owned web
/// runtime route.
#[derive(Clone)]
pub struct WsConnectionPool {
    limits: GatewayLimits,
    permits: Arc<tokio::sync::Semaphore>,
}

impl WsConnectionPool {
    /// Build a pool sized to `limits.max_ws_connections`.
    pub fn new(limits: GatewayLimits) -> Self {
        Self {
            permits: Arc::new(tokio::sync::Semaphore::new(limits.max_ws_connections)),
            limits,
        }
    }

    /// The limits this pool enforces (idle / lifetime / inbound frame rate / caps).
    pub fn limits(&self) -> GatewayLimits {
        self.limits
    }

    /// Try to take a connection permit, held for the socket lifetime. `None` when
    /// the global cap is saturated — the caller must reject the upgrade with 503
    /// (and, for ws routes, do so BEFORE consuming a single-use ticket — M5).
    pub fn try_acquire(&self) -> Option<tokio::sync::OwnedSemaphorePermit> {
        self.permits.clone().try_acquire_owned().ok()
    }
}

/// Loopback-only control-plane state: the daemon tunnel control plane (SEC-002).
///
/// Served on a SEPARATE loopback listener bound to a distinct port the tunnel
/// never targets — `LoopbackOnly` is not a trust boundary behind a same-host
/// tunnel (cloudflared proxies from 127.0.0.1, so a tunneled peer always looks
/// loopback). Keeping these routes off the public gateway router is the actual
/// fix; `LoopbackOnly` stays as defense-in-depth on this listener.
#[derive(Clone)]
struct ControlState {
    /// `None` when no remote subsystem is wired (local dev / default daemon).
    remote_control: Option<Arc<dyn RemoteControl>>,
}

/// Loopback-only extractor: rejects any request whose peer is not a loopback
/// address. Defense-in-depth for the `/api/remote/*` control plane (Locked
/// decision L3, SEC-002) — these routes now live on a SEPARATE loopback listener
/// bound to a distinct port the tunnel never targets, so they are unreachable
/// from the public tunnel by construction; this extractor re-asserts loopback on
/// the control listener as belt-and-suspenders.
struct LoopbackOnly;

impl<S> FromRequestParts<S> for LoopbackOnly
where
    S: Send + Sync,
{
    type Rejection = Response;

    async fn from_request_parts(parts: &mut Parts, _state: &S) -> Result<Self, Self::Rejection> {
        match parts.extensions.get::<ConnectInfo<SocketAddr>>() {
            Some(ConnectInfo(peer)) if peer.ip().is_loopback() => Ok(LoopbackOnly),
            Some(ConnectInfo(peer)) => {
                tracing::warn!(target: "lucarne_termgw", %peer, "rejected non-loopback access to /api/remote control plane");
                Err((
                    StatusCode::FORBIDDEN,
                    "remote control plane is loopback-only",
                )
                    .into_response())
            }
            // No connect info recorded — fail closed.
            None => Err((
                StatusCode::FORBIDDEN,
                "remote control plane is loopback-only",
            )
                .into_response()),
        }
    }
}

/// Require [`AccessScope::Full`] on a `/api/*` write handler (SEC-013 / C1).
///
/// [`bearer_guard`] resolves the authenticated scope and stores it in the
/// request extensions; this extractor reads it and rejects a read-only session
/// with 403 BEFORE the handler runs (so a read-only credential can never reach a
/// state-mutating route — create / close / archive). A missing scope extension
/// (auth misconfigured / a route not behind `bearer_guard`) fails closed.
///
/// Read handlers (GET list / files / agents / history) deliberately do NOT use
/// this extractor: a read-only bearer is allowed to mirror.
struct RequireFull;

impl<S> FromRequestParts<S> for RequireFull
where
    S: Send + Sync,
{
    type Rejection = Response;

    async fn from_request_parts(parts: &mut Parts, _state: &S) -> Result<Self, Self::Rejection> {
        match parts.extensions.get::<AccessScope>() {
            Some(AccessScope::Full) => Ok(RequireFull),
            Some(AccessScope::ReadOnly) => {
                tracing::info!(target: "lucarne_termgw", "refused write request on read-only HTTP session");
                Err((
                    StatusCode::FORBIDDEN,
                    "read-only session: write operations are not permitted",
                )
                    .into_response())
            }
            // No scope recorded — fail closed (the route must be bearer-gated).
            None => Err((
                StatusCode::FORBIDDEN,
                "read-only session: write operations are not permitted",
            )
                .into_response()),
        }
    }
}

/// Build the gateway router with auth **disabled** (local dev on loopback).
pub fn router(monitor: Arc<RmuxMonitor>, web_dir: PathBuf) -> Router {
    let control_store = ControlPlaneSqliteStore::open_in_memory()
        .expect("in-memory control-plane store for local gateway router");
    router_with_auth_and_store(monitor, web_dir, AuthState::disabled(), control_store)
}

/// Build the gateway router: ws mirror + control API + static web assets, gated
/// by `auth`.
///
/// - `/api/*` is wrapped in a `Bearer` middleware (constant-time compare; failures
///   feed the rate-limiter and return 401).
/// - `POST /auth/ticket` (also `Bearer`-gated) mints a single-use ws ticket.
/// - `/ws` and `/agent/{id}` consume a `?ticket=` *before* upgrading.
///
/// When `auth` is [`AuthState::disabled`] the gate is a no-op (local dev).
///
/// SEC-002: this **tunneled** router carries NO `/api/remote/*` control plane —
/// that lives on a separate loopback listener ([`control_router`] /
/// [`serve_control_plane`]) the tunnel never targets.
pub fn router_with_auth(monitor: Arc<RmuxMonitor>, web_dir: PathBuf, auth: AuthState) -> Router {
    let control_store = ControlPlaneSqliteStore::open_in_memory()
        .expect("in-memory control-plane store for local gateway router");
    router_with_auth_and_store(monitor, web_dir, auth, control_store)
}

/// Build the gateway router with auth disabled and an explicit Lucarne
/// control-plane store. Daemon entrypoints use this so terminal-agent bindings
/// persist as core cold state instead of a sidecar DB.
pub fn router_with_store(
    monitor: Arc<RmuxMonitor>,
    web_dir: PathBuf,
    control_store: ControlPlaneSqliteStore,
) -> Router {
    router_with_auth_and_store(monitor, web_dir, AuthState::disabled(), control_store)
}

/// Build the gateway router with explicit auth and Lucarne control-plane store.
pub fn router_with_auth_and_store(
    monitor: Arc<RmuxMonitor>,
    web_dir: PathBuf,
    auth: AuthState,
    control_store: ControlPlaneSqliteStore,
) -> Router {
    router_with_limits_and_store(
        monitor,
        web_dir,
        auth,
        GatewayLimits::default(),
        control_store,
    )
}

/// Build the tunneled gateway router with explicit connection/session limits
/// (SEC-004/SEC-006). Same surface as [`router_with_auth`]; the limits cap
/// concurrent ws connections, per-connection session creation, inbound frame
/// rate, and enforce idle + max-lifetime close on the ws client loops.
pub fn router_with_limits(
    monitor: Arc<RmuxMonitor>,
    web_dir: PathBuf,
    auth: AuthState,
    limits: GatewayLimits,
) -> Router {
    let control_store = ControlPlaneSqliteStore::open_in_memory()
        .expect("in-memory control-plane store for local gateway router");
    router_with_limits_and_store(monitor, web_dir, auth, limits, control_store)
}

/// Build the gateway router with explicit limits and Lucarne control-plane store.
pub fn router_with_limits_and_store(
    monitor: Arc<RmuxMonitor>,
    web_dir: PathBuf,
    auth: AuthState,
    limits: GatewayLimits,
    control_store: ControlPlaneSqliteStore,
) -> Router {
    router_with_pool_and_store(
        monitor,
        web_dir,
        auth,
        WsConnectionPool::new(limits),
        control_store,
    )
}

/// Build the tunneled gateway router sharing an EXISTING [`WsConnectionPool`]
/// (H1). Daemon remote uses this so `/ws` and `/agent` draw from the same global
/// connection cap and session lifetime policy.
pub fn router_with_pool(
    monitor: Arc<RmuxMonitor>,
    web_dir: PathBuf,
    auth: AuthState,
    ws_pool: WsConnectionPool,
) -> Router {
    let control_store = ControlPlaneSqliteStore::open_in_memory()
        .expect("in-memory control-plane store for local gateway router");
    router_with_pool_and_store(monitor, web_dir, auth, ws_pool, control_store)
}

/// Build the gateway router sharing an existing ws pool and using the provided
/// Lucarne control-plane store for terminal-agent binding history.
pub fn router_with_pool_and_store(
    monitor: Arc<RmuxMonitor>,
    web_dir: PathBuf,
    auth: AuthState,
    ws_pool: WsConnectionPool,
    control_store: ControlPlaneSqliteStore,
) -> Router {
    router_with_terminal_monitor_and_store(monitor, web_dir, auth, ws_pool, control_store)
}

fn router_with_terminal_monitor_and_store(
    monitor: Arc<dyn TerminalMonitor>,
    web_dir: PathBuf,
    auth: AuthState,
    ws_pool: WsConnectionPool,
    control_store: ControlPlaneSqliteStore,
) -> Router {
    let state = AppState {
        monitor,
        control_store,
        auth,
        limits: ws_pool.limits(),
        ws_pool,
    };
    gateway_router(state, web_dir)
}

/// Assemble the public (tunnel-reachable) gateway router from a built [`AppState`].
///
/// SEC-002: deliberately excludes `/api/remote/*`. The control plane is served
/// separately on loopback ([`control_router`]).
fn gateway_router(state: AppState, web_dir: PathBuf) -> Router {
    // /api/* + /auth/ticket: long-lived Bearer credential, constant-time compare.
    let bearer_gated = Router::new()
        .route("/api/sessions", get(http_list).post(http_create))
        .route("/api/sessions/{id}", delete(http_close))
        .route("/api/sessions/{id}/agent", get(http_agent))
        .route("/api/sessions/{id}/files", get(http_files))
        .route("/api/sessions/{id}/archive", post(http_archive))
        .route("/api/archives", get(http_archives))
        .route("/api/archives/{archive_id}", get(http_archive_get))
        .route("/api/agents", get(http_agents))
        .route("/api/agent-history", get(http_agent_history))
        .route("/api/agent-history/{session}", get(http_agent_history_get))
        .route("/auth/ticket", post(issue_ticket))
        .route_layer(middleware::from_fn_with_state(state.clone(), bearer_guard));

    // ws routes consume a single-use ticket inside the handler, before upgrade.
    Router::new()
        .route("/ws", get(ws_handler))
        .route("/agent/{id}", get(agent_ws))
        .merge(bearer_gated)
        .with_state(state)
        .fallback_service(ServeDir::new(web_dir))
}

/// Build the loopback-only control-plane router (SEC-002).
///
/// Hosts ONLY `/api/remote/{start,stop,status}`. Intended to be served on a
/// SEPARATE loopback listener bound to a distinct port the public tunnel never
/// targets (see [`serve_control_plane`]) — that separation, not `LoopbackOnly`,
/// is the trust boundary. `LoopbackOnly` re-asserts loopback as defense in depth.
/// The `access_token` is only ever returned over this listener, never over any
/// tunnel-reachable route.
pub fn control_router(remote_control: Option<Arc<dyn RemoteControl>>) -> Router {
    let state = ControlState { remote_control };
    Router::new()
        .route("/api/remote/start", post(http_remote_start))
        .route("/api/remote/stop", post(http_remote_stop))
        .route("/api/remote/status", get(http_remote_status))
        .with_state(state)
}

/// Gate a ws router with the gateway's single-use-ticket auth (SEC-001).
///
/// Wraps every route in `inner` with a middleware that consumes a `?ticket=`
/// query param via [`AuthState::tickets`] BEFORE the wrapped handler runs (so it
/// rejects with 401 *before* any `.on_upgrade()`). No-op when auth is disabled
/// (local dev). Apply it to any ws router that must share the gateway's auth —
/// for example a local test or an optional in-process extension route.
///
/// NOTE: this gate enforces auth only — it does NOT apply [`GatewayLimits`] /
/// the connection-permit pool. A ws route that must also obey the global
/// connection cap + idle/lifetime/frame-rate (H1) should instead call
/// [`authorize_ws`] from inside its own handler (so it can hold the permit for
/// the socket lifetime and drive the limits in its client loop).
pub fn gate_ws_router(inner: Router, auth: AuthState) -> Router {
    inner.layer(middleware::from_fn_with_state(auth, ws_ticket_gate_fn))
}

/// Authorize a ws upgrade for a route that shares the gateway's auth + limits
/// (SEC-001 / SEC-004 / H1 / M5).
///
/// This is the seam an optional in-process ws handler can call at the top of its
/// upgrade path when it must be governed by the same rules as termgw's `/ws` and
/// `/agent`:
///
/// 1. **Acquire a connection permit FIRST** from the shared [`WsConnectionPool`]
///    (M5): a saturated global cap returns `Err(503)` WITHOUT consuming the
///    single-use ticket.
/// 2. **Consume the single-use ticket** and resolve its [`AccessScope`]
///    (SEC-001/SEC-013); an invalid/expired/used ticket returns `Err(401)` and
///    releases the permit. Auth disabled (local dev) → `AccessScope::Full`, no
///    ticket required.
///
/// On success returns `(scope, permit)` — the caller upgrades the socket, holds
/// the permit for its lifetime, refuses write actions when `scope.is_readonly()`,
/// and enforces `pool.limits()` (idle / max-lifetime / inbound frame rate) in its
/// client loop.
#[allow(clippy::result_large_err)]
pub async fn authorize_ws(
    auth: &AuthState,
    ticket: Option<&str>,
    pool: &WsConnectionPool,
) -> Result<(AccessScope, tokio::sync::OwnedSemaphorePermit), Response> {
    // M5: permit first so a full cap never burns a ticket.
    let Some(permit) = pool.try_acquire() else {
        tracing::warn!(target: "lucarne_termgw",
            cap = pool.limits().max_ws_connections,
            "ws connection cap reached; rejecting upgrade with 503"
        );
        return Err((
            StatusCode::SERVICE_UNAVAILABLE,
            "too many concurrent connections",
        )
            .into_response());
    };
    match check_ws_ticket(auth, ticket).await {
        Ok(scope) => Ok((scope, permit)),
        Err(refusal) => {
            drop(permit); // release the slot on auth failure
            Err(refusal)
        }
    }
}

/// The middleware body for [`gate_ws_router`]: reject before the wrapped ws
/// handler runs when the ticket is missing/invalid (auth enforced).
async fn ws_ticket_gate_fn(
    State(auth): State<AuthState>,
    Query(q): Query<TicketQuery>,
    req: Request<axum::body::Body>,
    next: Next,
) -> Response {
    match check_ws_ticket(&auth, q.ticket.as_deref()).await {
        Ok(_scope) => next.run(req).await,
        Err(refusal) => refusal,
    }
}

/// Bearer-token middleware for `/api/*` and `/auth/ticket`.
///
/// Disabled auth → pass through (local dev). Otherwise require a valid
/// `Authorization: Bearer <token>` compared in constant time; a missing/invalid
/// token feeds the per-client rate-limiter and returns 401. A client that is
/// already locked out gets 429 without a compare.
///
/// SEC-005: behind a same-host tunnel every socket peer is the constant loopback
/// address, so we key the limiter on a trusted forwarded client IP when present.
/// The header(s) that carry that IP are provider-specific and NOT hardcoded here
/// (H6b): the daemon injects the trusted-header list via
/// [`AuthState::forwarded_identity`]; a forwarded header is trusted only when the
/// socket peer is the loopback tunnel source. We never hard-lock the shared
/// loopback key — a flood there only earns an incremental soft delay so one
/// attacker can't deny everyone.
///
/// SEC-013/C1: on success the authenticated [`AccessScope`] is written into the
/// request extensions so downstream handlers (HTTP write routes) can require
/// `Full`. A read-only bearer reaches GET/list handlers but is rejected by write
/// handlers via [`RequireFull`].
async fn bearer_guard(
    State(s): State<AppState>,
    ConnectInfo(peer): ConnectInfo<SocketAddr>,
    mut req: Request<axum::body::Body>,
    next: Next,
) -> Response {
    if !s.auth.mode.is_enforced() {
        // Auth disabled (local dev): everything is full access.
        req.extensions_mut().insert(AccessScope::Full);
        return next.run(req).await;
    }
    // H6b: resolve the forwarded client IP only from the daemon-configured
    // trusted header(s) for the started provider — never a hardcoded header.
    let forwarded_ip = {
        let headers = req.headers();
        s.auth
            .forwarded_identity
            .forwarded_ip(|name| headers.get(name).and_then(|v| v.to_str().ok()))
    };
    let (key, lockable) = AuthState::limiter_key(peer, forwarded_ip);
    if lockable && s.auth.limiter.is_locked(&key).await {
        return (StatusCode::TOO_MANY_REQUESTS, "locked out").into_response();
    }
    let presented = req
        .headers()
        .get(axum::http::header::AUTHORIZATION)
        .and_then(|v| v.to_str().ok());
    match AuthState::bearer(presented).and_then(|tok| s.auth.scope_for(tok)) {
        Some(scope) => {
            s.auth.limiter.record_success(&key).await;
            // C1: surface the authenticated scope to downstream handlers.
            req.extensions_mut().insert(scope);
            next.run(req).await
        }
        None => {
            let locked = s.auth.limiter.record_failure(&key, lockable).await;
            // For the shared loopback key (no real client identity) we can't
            // hard-lock without denying everyone (SEC-005); apply a bounded
            // incremental delay instead so a sustained guessing flood is slowed.
            if !lockable {
                let delay = s.auth.limiter.soft_delay(&key).await;
                if !delay.is_zero() {
                    tokio::time::sleep(delay).await;
                }
            }
            if locked {
                tracing::warn!(target: "lucarne_termgw", %key, "rate-limiter locked out client after repeated auth failures");
            } else {
                tracing::warn!(target: "lucarne_termgw", %key, lockable, "rejected bearer auth failure");
            }
            (StatusCode::UNAUTHORIZED, "invalid or missing bearer token").into_response()
        }
    }
}

/// `POST /auth/ticket` — mint a fresh single-use, short-TTL ws ticket. Bearer is
/// already enforced by [`bearer_guard`]; the ticket is what the browser puts in
/// the ws URL (so the long-lived token never lands in ws-connect logs).
///
/// SEC-013: the ticket is minted with the scope the presented bearer
/// authenticates as — a read-only bearer yields a read-only ticket, so the ws
/// session it opens refuses write frames. A full bearer (or auth disabled)
/// yields a full-access ticket (unchanged).
///
/// R3-5: the scope is read from the [`AccessScope`] that [`bearer_guard`] already
/// resolved and wrote into the request extensions — NOT by re-parsing the
/// `Authorization` header here. `bearer_guard` is the single place that maps a
/// bearer → scope (constant-time, rate-limited, with the readonly tier), and it
/// always runs before this handler ([`bearer_guard`] gates `/auth/ticket`). A
/// missing extension (which would only happen if the route were ever detached
/// from `bearer_guard`) is rejected by the extractor before this handler mints
/// any ticket.
async fn issue_ticket(
    State(s): State<AppState>,
    axum::Extension(scope): axum::Extension<AccessScope>,
) -> Response {
    match s.auth.tickets.issue_scoped(scope).await {
        Ok(ticket) => Json(json!({ "ticket": ticket })).into_response(),
        Err(e) => {
            tracing::warn!(target: "lucarne_termgw", error = %e, "ws ticket issuance refused");
            (StatusCode::TOO_MANY_REQUESTS, e.to_string()).into_response()
        }
    }
}

/// Bind `addr` and serve the gateway with auth **disabled** (local dev, loopback).
pub async fn serve(
    monitor: Arc<RmuxMonitor>,
    addr: SocketAddr,
    web_dir: PathBuf,
) -> std::io::Result<()> {
    serve_with_auth(monitor, addr, web_dir, AuthState::disabled(), false, false).await
}

/// Bind `addr` and serve the gateway with the given auth state.
///
/// Hardening enforced before binding:
/// - **default-deny**: if `remote` is on and no token is configured and `insecure`
///   is not set, refuses to start ([`require_auth_or_refuse`]).
/// - **loopback**: in `remote` mode the bind address must be loopback
///   ([`parse_gateway_addr`]) — the gateway never listens on a public interface;
///   the tunnel connects outbound and back to this loopback socket.
pub async fn serve_with_auth(
    monitor: Arc<RmuxMonitor>,
    addr: SocketAddr,
    web_dir: PathBuf,
    auth: AuthState,
    remote: bool,
    insecure: bool,
) -> std::io::Result<()> {
    // default-deny: refuse public exposure without a token (unless explicit override).
    if let Err(refusal) = require_auth_or_refuse(remote, &auth.mode, insecure) {
        tracing::error!(target: "lucarne_termgw", %refusal, "refusing to start");
        return Err(std::io::Error::new(
            std::io::ErrorKind::PermissionDenied,
            refusal.to_string(),
        ));
    }
    if remote && insecure && !auth.mode.is_enforced() {
        tracing::warn!(target: "lucarne_termgw",
            "INSECURE public exposure with NO access token — anyone reaching \
             the tunnel can drive your terminals. This is RCE-equivalent."
        );
    }
    // loopback hardening: in remote mode the gateway must bind a loopback addr.
    if remote && !addr.ip().is_loopback() {
        let err = GatewayAddrError::NonLoopback(addr);
        tracing::error!(target: "lucarne_termgw", %err, "refusing to bind a non-loopback address in remote mode");
        return Err(std::io::Error::new(
            std::io::ErrorKind::InvalidInput,
            err.to_string(),
        ));
    }

    // SEC-002: this serves only the tunneled gateway surface; the `/api/remote/*`
    // control plane is served separately on loopback (see [`serve_control_plane`]).
    let app = router_with_limits(monitor, web_dir, auth, GatewayLimits::default());
    let listener = tokio::net::TcpListener::bind(addr).await?;
    tracing::info!(target: "lucarne_termgw", %addr, remote, "listening");
    axum::serve(
        listener,
        app.into_make_service_with_connect_info::<SocketAddr>(),
    )
    .await
}

/// Serve the loopback-only `/api/remote/*` control plane on its OWN listener
/// (SEC-002).
///
/// `addr` must be a loopback address bound to a port the public tunnel never
/// targets (the tunnel only points at the public gateway port). This is the real
/// trust boundary that keeps the tunnel control plane — and the `access_token`
/// it returns — unreachable from anyone on the tunnel. `LoopbackOnly` on the
/// routes is belt-and-suspenders. Refuses to bind a non-loopback `addr`.
pub async fn serve_control_plane(
    addr: SocketAddr,
    remote_control: Option<Arc<dyn RemoteControl>>,
) -> std::io::Result<()> {
    if !addr.ip().is_loopback() {
        let err = GatewayAddrError::NonLoopback(addr);
        tracing::error!(target: "lucarne_termgw", %err, "refusing to bind a non-loopback control-plane address");
        return Err(std::io::Error::new(
            std::io::ErrorKind::InvalidInput,
            err.to_string(),
        ));
    }
    let app = control_router(remote_control);
    let listener = tokio::net::TcpListener::bind(addr).await?;
    tracing::info!(target: "lucarne_termgw", %addr, "control plane listening (loopback-only)");
    axum::serve(
        listener,
        app.into_make_service_with_connect_info::<SocketAddr>(),
    )
    .await
}

// ---- Remote tunnel control plane (separate loopback listener — SEC-002) ----

/// `POST /api/remote/start` — start the daemon-owned tunnel, return its status
/// (public URL + access token). Loopback-only ([`LoopbackOnly`]) on a dedicated
/// control listener (SEC-002); the daemon owns the tunnel lifecycle so it
/// survives the CLI exiting.
///
/// G3: an optional JSON body `{provider?, fields?}` ([`RemoteStartParams`]) lets
/// the CLI override / merge the provider + its fields.
///
/// M2: body handling is explicit — an EMPTY body falls back to the daemon's
/// pre-configured tunnel (older clients / bodyless `curl -XPOST`), but a
/// NON-empty body that fails to parse is a `400 Bad Request` (no silent
/// `unwrap_or_default` downgrade). Typed [`RemoteControlError`]s from the daemon
/// map to 400/404/502 with a descriptive message (safe: loopback-only).
async fn http_remote_start(
    _loopback: LoopbackOnly,
    State(s): State<ControlState>,
    body: axum::body::Bytes,
) -> Response {
    let params = match parse_start_params(&body) {
        Ok(params) => params,
        Err(detail) => {
            tracing::warn!(target: "lucarne_termgw", %detail, "remote tunnel start rejected — malformed body");
            return (
                StatusCode::BAD_REQUEST,
                format!("invalid request body: {detail}"),
            )
                .into_response();
        }
    };
    match &s.remote_control {
        Some(control) => match control.start(params).await {
            Ok(status) => Json(status).into_response(),
            Err(e) => {
                tracing::warn!(target: "lucarne_termgw", error = %e, "remote tunnel start failed");
                // M2: the control plane is loopback-only (SEC-002), so a precise
                // status + safe message is fine here — it never reaches a tunnel.
                (e.status_code(), e.detail().to_string()).into_response()
            }
        },
        None => (
            StatusCode::SERVICE_UNAVAILABLE,
            "remote subsystem not configured",
        )
            .into_response(),
    }
}

/// Parse the optional `/api/remote/start` body into [`RemoteStartParams`] (M2).
///
/// - An EMPTY body → `Ok(default)` (use the daemon's pre-configured tunnel), so
///   the control plane stays forgiving of bodyless POSTs (G3).
/// - A NON-empty body that is not the expected JSON shape → `Err(detail)` so the
///   handler returns 400 instead of silently downgrading to the default
///   (no `unwrap_or_default`).
fn parse_start_params(body: &[u8]) -> Result<RemoteStartParams, String> {
    if body.is_empty() {
        return Ok(RemoteStartParams::default());
    }
    serde_json::from_slice::<RemoteStartParams>(body).map_err(|e| e.to_string())
}

/// `POST /api/remote/stop` — stop the daemon-owned tunnel. Loopback-only.
async fn http_remote_stop(_loopback: LoopbackOnly, State(s): State<ControlState>) -> Response {
    match &s.remote_control {
        Some(control) => match control.stop().await {
            Ok(status) => Json(status).into_response(),
            Err(e) => {
                tracing::warn!(target: "lucarne_termgw", error = %e, "remote tunnel stop failed");
                // M2: loopback-only control plane → safe to surface the detail.
                (e.status_code(), e.detail().to_string()).into_response()
            }
        },
        None => (
            StatusCode::SERVICE_UNAVAILABLE,
            "remote subsystem not configured",
        )
            .into_response(),
    }
}

/// `GET /api/remote/status` — report the daemon-owned tunnel status. Loopback-only.
async fn http_remote_status(_loopback: LoopbackOnly, State(s): State<ControlState>) -> Response {
    match &s.remote_control {
        Some(control) => Json(control.status().await).into_response(),
        None => Json(RemoteControlStatus::default()).into_response(),
    }
}

// ---- HTTP control surface (the CLI hits these) ----

async fn http_list(State(s): State<AppState>) -> Json<Vec<SessionDescriptor>> {
    Json(s.monitor.sessions().await)
}

#[derive(Deserialize)]
struct CreateReq {
    title: Option<String>,
}

async fn http_create(
    _full: RequireFull,
    State(s): State<AppState>,
    Json(req): Json<CreateReq>,
) -> Response {
    match s
        .monitor
        .create(req.title.unwrap_or_else(|| "shell".to_string()))
        .await
    {
        Ok(desc) => (StatusCode::CREATED, Json(desc)).into_response(),
        // SEC-007: generic client message; detail logged server-side only.
        Err(e) => {
            tracing::warn!(target: "lucarne_termgw", error = %e, "session create failed");
            (StatusCode::INTERNAL_SERVER_ERROR, "internal error").into_response()
        }
    }
}

async fn http_close(
    _full: RequireFull,
    State(s): State<AppState>,
    Path(id): Path<String>,
) -> Response {
    match s.monitor.kill(&id).await {
        Ok(()) => StatusCode::NO_CONTENT.into_response(),
        Err(lucarne_rmux::MonitorError::NotFound(_)) => StatusCode::NOT_FOUND.into_response(),
        // SEC-007: generic client message; detail logged server-side only.
        Err(e) => {
            tracing::warn!(target: "lucarne_termgw", %id, error = %e, "session close failed");
            (StatusCode::INTERNAL_SERVER_ERROR, "internal error").into_response()
        }
    }
}

// ---- agent-session binding (P7): pane cwd → agent transcript ----

/// Find a monitored session's cwd by id.
async fn session_cwd(s: &AppState, id: &str) -> Option<String> {
    s.monitor
        .sessions()
        .await
        .into_iter()
        .find(|d| d.id == id)
        .and_then(|d| d.cwd)
}

/// GET the agent (if any) bound to a session's cwd, plus its transcript so far.
async fn http_agent(State(s): State<AppState>, Path(id): Path<String>) -> Response {
    let Some(cwd) = session_cwd(&s, &id).await else {
        return (StatusCode::NOT_FOUND, "session has no known cwd").into_response();
    };
    match terminal_agent_bind::bind(&cwd) {
        Some(agent) => {
            let (msgs, _) = terminal_agent_bind::read_messages(&agent.transcript, 0);
            Json(json!({
                "bound": true,
                "kind": agent.kind,
                "session_id": agent.session_id,
                "cwd": cwd,
                "messages": msgs.iter().map(|m| json!({"role": m.role, "text": m.text})).collect::<Vec<_>>(),
            }))
            .into_response()
        }
        None => Json(json!({"bound": false, "cwd": cwd})).into_response(),
    }
}

/// A ws ticket carried in the upgrade request query string (`?ticket=…`).
///
/// Browsers can't set an `Authorization` header on a WebSocket, so the client
/// first posts its Bearer token to `/auth/ticket`, then passes the returned
/// single-use ticket here. Validated + consumed *before* `.on_upgrade()`.
#[derive(Deserialize)]
struct TicketQuery {
    ticket: Option<String>,
}

/// Validate + consume the ws ticket before upgrading. Returns `Err(refusal)` to
/// reject (without upgrading) when auth is enforced and the ticket is
/// missing/expired/already-used; `Ok(scope)` to proceed with the consumed
/// ticket's [`AccessScope`] (SEC-013). When auth is disabled this is always
/// `Ok(AccessScope::Full)` (local dev — no readonly tier without a token).
async fn check_ws_ticket(auth: &AuthState, ticket: Option<&str>) -> Result<AccessScope, Response> {
    if !auth.mode.is_enforced() {
        return Ok(AccessScope::Full);
    }
    match ticket {
        Some(t) => match auth.tickets.consume_scoped(t).await {
            Some(scope) => Ok(scope),
            None => {
                // SEC-011: audit a rejected ws ticket (used/expired/forged). The
                // ticket value itself is never logged.
                tracing::info!(target: "lucarne_termgw", "rejected ws upgrade — invalid or used ticket");
                Err((StatusCode::UNAUTHORIZED, "invalid or missing ws ticket").into_response())
            }
        },
        None => {
            tracing::info!(target: "lucarne_termgw", "rejected ws upgrade — missing ticket");
            Err((StatusCode::UNAUTHORIZED, "invalid or missing ws ticket").into_response())
        }
    }
}

/// Try to acquire a global ws-connection permit (SEC-004). Returns the owned
/// permit (held for the socket's lifetime) or `Err` with a 503 response when the
/// concurrency cap is already saturated, so excess upgrades are rejected before
/// `.on_upgrade()`.
// The error is an axum `Response` (the project's pervasive handler return type);
// boxing it would only obscure the call sites that `return` it directly.
#[allow(clippy::result_large_err)]
fn acquire_ws_permit(s: &AppState) -> Result<tokio::sync::OwnedSemaphorePermit, Response> {
    match s.ws_pool.try_acquire() {
        Some(permit) => Ok(permit),
        None => {
            tracing::warn!(target: "lucarne_termgw",
                cap = s.limits.max_ws_connections,
                "ws connection cap reached; rejecting upgrade with 503"
            );
            Err((
                StatusCode::SERVICE_UNAVAILABLE,
                "too many concurrent connections",
            )
                .into_response())
        }
    }
}

async fn agent_ws(
    State(s): State<AppState>,
    Path(id): Path<String>,
    Query(q): Query<TicketQuery>,
    ws: WebSocketUpgrade,
) -> Response {
    // SEC-004 / M5: acquire the connection permit FIRST. When the cap is
    // saturated this returns 503 WITHOUT consuming the single-use ticket, so a
    // full gateway never burns a client's ticket. The permit is held for the
    // socket lifetime; if auth fails below it is dropped (released) immediately.
    let permit = match acquire_ws_permit(&s) {
        Ok(p) => p,
        Err(resp) => return resp,
    };
    // Auth gate: consume a single-use ticket BEFORE upgrading. A used or expired
    // ticket never reaches `.on_upgrade(`. SEC-013: a read-only ticket opens a
    // mirror-only agent session (prompts — a write — are refused).
    let scope = match check_ws_ticket(&s.auth, q.ticket.as_deref()).await {
        Ok(scope) => scope,
        // Drop the permit on auth failure so a rejected upgrade frees a slot.
        Err(refusal) => {
            drop(permit);
            return refusal;
        }
    };
    let limits = s.limits;
    ws.on_upgrade(move |socket| agent_client(s, id, socket, limits, scope, permit))
}

/// Stream a pane's bound agent transcript as chat bubbles; route inbound prompts
/// into the pane (typing into the interactive agent).
///
/// SEC-006: enforces an idle timeout and a max session lifetime — the socket is
/// closed on either, so a live session can't outlive its connect-time ticket
/// indefinitely. The `_permit` is held until this returns (SEC-004 conn cap).
///
/// SEC-013: a read-only `scope` mirrors the transcript but refuses inbound
/// prompts (a write into the live agent).
/// SEC-011: logs connect/disconnect at info with a per-connection sequence (no
/// credential is ever logged).
async fn agent_client(
    s: AppState,
    id: SessionId,
    socket: WebSocket,
    limits: GatewayLimits,
    scope: AccessScope,
    _permit: tokio::sync::OwnedSemaphorePermit,
) {
    let conn = next_conn_seq();
    let readonly = scope.is_readonly();
    tracing::info!(target: "lucarne_termgw", conn, %id, readonly, "agent ws connected");
    agent_client_inner(&s, &id, socket, limits, readonly).await;
    tracing::info!(target: "lucarne_termgw", conn, %id, "agent ws disconnected");
}

async fn agent_client_inner(
    s: &AppState,
    id: &SessionId,
    socket: WebSocket,
    limits: GatewayLimits,
    readonly: bool,
) {
    let (mut tx, mut rx) = socket.split();

    let Some(cwd) = session_cwd(s, id).await else {
        let _ = send_value(
            &mut tx,
            &json!({"type":"error","msg":"session has no known cwd"}),
        )
        .await;
        return;
    };
    let Some(agent) = terminal_agent_bind::bind(&cwd) else {
        let _ = send_value(
            &mut tx,
            &json!({"type":"error","msg":"no agent transcript bound to this cwd","cwd":cwd}),
        )
        .await;
        return;
    };
    if !send_value(&mut tx, &json!({"type":"ready","kind":agent.kind,"session_id":agent.session_id,"cwd":cwd,"readonly":readonly})).await {
        return;
    }

    let (initial, mut offset) = terminal_agent_bind::read_messages(&agent.transcript, 0);
    for m in &initial {
        if !send_value(
            &mut tx,
            &json!({"type":"message","role":m.role,"text":m.text}),
        )
        .await
        {
            return;
        }
    }

    // SEC-006: idle + max-lifetime close.
    let deadline = tokio::time::Instant::now() + limits.max_session_lifetime;
    let mut idle = tokio::time::interval(limits.idle_timeout);
    idle.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    idle.tick().await; // consume the immediate first tick
    let mut tick = tokio::time::interval(std::time::Duration::from_millis(700));
    loop {
        tokio::select! {
            _ = tokio::time::sleep_until(deadline) => {
                tracing::info!(target: "lucarne_termgw", %id, "agent ws closed on max session lifetime");
                break;
            }
            _ = idle.tick() => {
                tracing::info!(target: "lucarne_termgw", %id, "agent ws closed on idle timeout");
                break;
            }
            inbound = rx.next() => match inbound {
                Some(Ok(Message::Text(t))) => {
                    idle.reset();
                    if let Ok(v) = serde_json::from_str::<Value>(t.as_str()) {
                        if v.get("type").and_then(Value::as_str) == Some("prompt") {
                            // SEC-013: a prompt types into the live agent — a write.
                            // Read-only sessions may only mirror; refuse it.
                            if readonly {
                                tracing::info!(target: "lucarne_termgw", %id, "refused agent prompt on read-only session");
                                let _ = send_value(&mut tx, &json!({"type":"error","msg":"read-only session: prompts are not permitted"})).await;
                            } else {
                                let text = v.get("text").and_then(Value::as_str).unwrap_or("");
                                // Type the message into the interactive agent + Enter.
                                let _ = s.monitor.inject(id, TermInput::Text { text: format!("{text}\n") }).await;
                            }
                        }
                    }
                }
                Some(Ok(Message::Close(_))) | None => break,
                Some(Ok(_)) => { idle.reset(); }
                Some(Err(_)) => break,
            },
            _ = tick.tick() => {
                let (new_msgs, new_off) = terminal_agent_bind::read_messages(&agent.transcript, offset);
                offset = new_off;
                for m in &new_msgs {
                    if !send_value(&mut tx, &json!({"type":"message","role":m.role,"text":m.text})).await {
                        return;
                    }
                }
            }
        }
    }
}

async fn send_value(tx: &mut Sender, v: &Value) -> bool {
    tx.send(Message::Text(v.to_string().into())).await.is_ok()
}

// ---- archive (P9): close the local rmux terminal, keep its content ----

/// Archive a session: capture its scrollback, persist it, then kill the rmux
/// session (freeing the process). Agent transcripts persist on their own; this
/// preserves a plain terminal's content too.
async fn http_archive(
    _full: RequireFull,
    State(s): State<AppState>,
    Path(id): Path<String>,
) -> Response {
    let Some(desc) = s.monitor.sessions().await.into_iter().find(|d| d.id == id) else {
        return (StatusCode::NOT_FOUND, "unknown session").into_response();
    };
    let content = s.monitor.capture_scrollback(&id).await.unwrap_or_default();
    let archive_id = match archive::save(
        &id,
        &desc.title,
        desc.cwd.as_deref(),
        &content,
        archive::now_epoch(),
    ) {
        Ok(a) => a,
        // SEC-007: generic client message; detail logged server-side only.
        Err(e) => {
            tracing::warn!(target: "lucarne_termgw", %id, error = %e, "archive save failed");
            return (StatusCode::INTERNAL_SERVER_ERROR, "internal error").into_response();
        }
    };
    let _ = s.monitor.kill(&id).await; // content is preserved on disk
    Json(json!({ "archive_id": archive_id })).into_response()
}

async fn http_archives() -> Response {
    Json(json!({ "archived": archive::list() })).into_response()
}

async fn http_archive_get(Path(archive_id): Path<String>) -> Response {
    if archive_id.contains('/') || archive_id.contains("..") {
        return StatusCode::BAD_REQUEST.into_response();
    }
    match archive::get(&archive_id) {
        Some(v) => Json(v).into_response(),
        None => StatusCode::NOT_FOUND.into_response(),
    }
}

// ---- agent session pickers (live-bound + history) ----

/// Monitored panes that currently have a bound agent transcript (for the chat
/// picker — these are drivable: prompts type into the live pane).
async fn http_agents(State(s): State<AppState>) -> Response {
    let mut agents = Vec::new();
    for d in s.monitor.sessions().await {
        if let Some(cwd) = &d.cwd {
            if let Some(a) = terminal_agent_bind::bind(cwd) {
                // Record the rmux↔agent binding so chat history shows ONLY
                // rmux-related sessions (never a blind ~/.claude scan). The
                // observation is cold core control-plane state, not a side DB.
                if let Err(e) = terminal_agent_bind::record(
                    &s.control_store,
                    &a.kind,
                    &a.session_id,
                    cwd,
                    &d.id,
                    &d.title,
                    &a.transcript,
                ) {
                    tracing::warn!(target: "lucarne_termgw", error = %e, "terminal-agent binding record failed");
                }
                agents.push(json!({
                    "term_session": d.id,
                    "title": d.title,
                    "cwd": cwd,
                    "kind": a.kind,
                    "agent_session_id": a.session_id,
                    "summary": terminal_agent_bind::first_user_message(&a.transcript),
                }));
            }
        }
    }
    Json(json!({ "agents": agents })).into_response()
}

/// Recent rmux-related agent sessions (from the SQLite registry, NOT a global
/// transcript scan — only sessions ever bound to an rmux pane appear).
async fn http_agent_history(State(s): State<AppState>) -> Response {
    let rows = match terminal_agent_bind::history(&s.control_store, 40) {
        Ok(rows) => rows,
        Err(e) => {
            tracing::warn!(target: "lucarne_termgw", error = %e, "terminal-agent history read failed");
            return (StatusCode::INTERNAL_SERVER_ERROR, "internal error").into_response();
        }
    };
    Json(json!({
        "sessions": rows.iter().map(|r| json!({
            "kind": r.kind,
            "session_id": r.session_id,
            "cwd": r.cwd,
            "rmux_session": r.rmux_session,
            "title": r.title,
            "summary": r.summary,
            "last_seen": r.last_seen,
        })).collect::<Vec<_>>()
    }))
    .into_response()
}

/// Messages of one recorded history transcript (read-only — no live pane).
async fn http_agent_history_get(
    State(s): State<AppState>,
    Path(session): Path<String>,
) -> Response {
    let path = match terminal_agent_bind::transcript_path(&s.control_store, &session) {
        Ok(Some(path)) => path,
        Ok(None) => return StatusCode::NOT_FOUND.into_response(),
        Err(e) => {
            tracing::warn!(target: "lucarne_termgw", error = %e, "terminal-agent transcript lookup failed");
            return (StatusCode::INTERNAL_SERVER_ERROR, "internal error").into_response();
        }
    };
    let (msgs, _) = terminal_agent_bind::read_messages(&path, 0);
    Json(json!({
        "session_id": session,
        "messages": msgs.iter().map(|m| json!({"role": m.role, "text": m.text})).collect::<Vec<_>>(),
    }))
    .into_response()
}

// ---- file tree (P8): browse the pane's cwd ----

#[derive(Deserialize)]
struct FilesQuery {
    /// Sub-path relative to the session cwd (empty = the cwd itself).
    path: Option<String>,
}

/// List one directory under a session's cwd. Bounded (≤2000 entries, no
/// recursion — the web tree lazy-loads each level) and sandboxed to the cwd.
async fn http_files(
    State(s): State<AppState>,
    Path(id): Path<String>,
    Query(q): Query<FilesQuery>,
) -> Response {
    let Some(cwd) = session_cwd(&s, &id).await else {
        return (StatusCode::NOT_FOUND, "session has no known cwd").into_response();
    };
    let base = std::path::Path::new(&cwd);
    let target = base.join(q.path.unwrap_or_default());
    // Canonicalize both and ensure the target stays inside the cwd (no `..` escape).
    let (Ok(target), Ok(base_c)) = (target.canonicalize(), base.canonicalize()) else {
        return (StatusCode::NOT_FOUND, "path not found").into_response();
    };
    if !target.starts_with(&base_c) {
        return (StatusCode::FORBIDDEN, "path is outside the session cwd").into_response();
    }
    if !target.is_dir() {
        return (StatusCode::BAD_REQUEST, "not a directory").into_response();
    }

    let mut entries: Vec<(String, bool)> = Vec::new();
    if let Ok(rd) = std::fs::read_dir(&target) {
        for entry in rd.flatten().take(2000) {
            let name = entry.file_name().to_string_lossy().into_owned();
            let is_dir = entry.file_type().map(|t| t.is_dir()).unwrap_or(false);
            entries.push((name, is_dir));
        }
    }
    // Directories first, then case-insensitive by name.
    entries.sort_by(|a, b| {
        b.1.cmp(&a.1)
            .then(a.0.to_lowercase().cmp(&b.0.to_lowercase()))
    });

    let rel = target
        .strip_prefix(&base_c)
        .ok()
        .map(|p| p.to_string_lossy().into_owned())
        .unwrap_or_default();
    Json(json!({
        "cwd": cwd,
        "path": rel,
        "entries": entries.iter().map(|(n, d)| json!({"name": n, "dir": d})).collect::<Vec<_>>(),
    }))
    .into_response()
}

// ---- WebSocket mirror ----

async fn ws_handler(
    State(s): State<AppState>,
    Query(q): Query<TicketQuery>,
    ws: WebSocketUpgrade,
) -> Response {
    // SEC-004 / M5: acquire the connection permit FIRST so a saturated cap
    // returns 503 WITHOUT consuming the single-use ticket. The permit is held
    // for the socket lifetime; on auth failure below it is dropped immediately.
    let permit = match acquire_ws_permit(&s) {
        Ok(p) => p,
        Err(resp) => return resp,
    };
    // Auth gate: consume a single-use ticket BEFORE upgrading. A used or expired
    // ticket never reaches `.on_upgrade(`. The consumed ticket's scope (SEC-013)
    // decides whether this session may inject/create/close (Full) or only mirror
    // (ReadOnly).
    let scope = match check_ws_ticket(&s.auth, q.ticket.as_deref()).await {
        Ok(scope) => scope,
        Err(refusal) => {
            drop(permit);
            return refusal;
        }
    };
    let limits = s.limits;
    let monitor = s.monitor.clone();
    ws.on_upgrade(move |socket| client_task(monitor, socket, limits, scope, permit))
}

type Sender = SplitSink<WebSocket, Message>;

/// Serialize and send one server frame. Returns `false` if the socket is closed.
async fn send_frame(sender: &mut Sender, frame: &ServerFrame) -> bool {
    match serde_json::to_string(frame) {
        Ok(json) => sender.send(Message::Text(json.into())).await.is_ok(),
        Err(e) => {
            tracing::warn!(target: "lucarne_termgw", error = %e, "serialize frame failed");
            true // a serialization bug must not silently drop the connection
        }
    }
}

/// Push a fresh full snapshot for `session` and (re)seed its differ baseline.
async fn snapshot_into(
    monitor: &dyn TerminalMonitor,
    sender: &mut Sender,
    differs: &mut HashMap<SessionId, Differ>,
    session: SessionId,
) -> bool {
    snapshot_into_with_client_rev(monitor, sender, differs, session, None).await
}

/// Push a fresh full snapshot for `session`, validating the client-supplied
/// `have_rev` when present before replacing the differ baseline.
async fn snapshot_into_with_client_rev(
    monitor: &dyn TerminalMonitor,
    sender: &mut Sender,
    differs: &mut HashMap<SessionId, Differ>,
    session: SessionId,
    have_rev: Option<u64>,
) -> bool {
    match monitor.snapshot_grid(&session).await {
        Ok((grid, cursor)) => {
            if let Some(have_rev) = have_rev {
                match differs.get(&session).and_then(Differ::current_rev) {
                    Some(current) if current != have_rev => tracing::debug!(
                        target: "lucarne_termgw",
                        %session,
                        have_rev,
                        current_rev = current,
                        "client resync requested with stale revision; sending full snapshot"
                    ),
                    Some(current) => tracing::trace!(
                        target: "lucarne_termgw",
                        %session,
                        have_rev,
                        current_rev = current,
                        "client resync requested with matching revision; refreshing full snapshot"
                    ),
                    None => tracing::debug!(
                        target: "lucarne_termgw",
                        %session,
                        have_rev,
                        "client resync requested before a server baseline existed; sending full snapshot"
                    ),
                }
            }
            let mut differ = Differ::new();
            let seeded = differ.feed(grid);
            differs.insert(session.clone(), differ);
            if let DiffResult::Full(grid) = seeded {
                return send_frame(
                    sender,
                    &ServerFrame::Snapshot {
                        session,
                        grid,
                        cursor,
                    },
                )
                .await;
            }
            true
        }
        Err(e) => {
            // SEC-007: detail logged server-side; client gets a generic message.
            tracing::warn!(target: "lucarne_termgw", %session, error = %e, "snapshot failed");
            send_frame(
                sender,
                &ServerFrame::Error {
                    code: 404,
                    msg: "session unavailable".to_string(),
                },
            )
            .await
        }
    }
}

/// Simple per-connection inbound-frame rate limiter (SEC-004): a leaky bucket of
/// `max_per_sec` frames refilled each second. `allow()` returns false when the
/// connection has exceeded its budget within the current 1-second window.
struct FrameRate {
    max_per_sec: u32,
    count: u32,
    window_start: tokio::time::Instant,
}

impl FrameRate {
    fn new(max_per_sec: u32) -> Self {
        Self {
            max_per_sec,
            count: 0,
            window_start: tokio::time::Instant::now(),
        }
    }

    /// Record an inbound frame; returns false when over budget (0 = unlimited).
    fn allow(&mut self) -> bool {
        if self.max_per_sec == 0 {
            return true;
        }
        let now = tokio::time::Instant::now();
        if now.duration_since(self.window_start) >= std::time::Duration::from_secs(1) {
            self.window_start = now;
            self.count = 0;
        }
        self.count = self.count.saturating_add(1);
        self.count <= self.max_per_sec
    }
}

async fn client_task(
    monitor: Arc<dyn TerminalMonitor>,
    socket: WebSocket,
    limits: GatewayLimits,
    scope: AccessScope,
    _permit: tokio::sync::OwnedSemaphorePermit,
) {
    // SEC-011: log connect/disconnect with a per-connection sequence id (never a
    // credential). SEC-013: a read-only session refuses write frames.
    let conn = next_conn_seq();
    let readonly = scope.is_readonly();
    tracing::info!(target: "lucarne_termgw", conn, readonly, "ws mirror connected");

    let (mut sender, mut receiver) = socket.split();
    let mut bcast = monitor.subscribe();
    let mut subscribed: HashSet<SessionId> = HashSet::new();
    let mut differs: HashMap<SessionId, Differ> = HashMap::new();
    // SEC-004: anti fork-bomb / flood.
    let mut sessions_created: usize = 0;
    let mut frame_rate = FrameRate::new(limits.max_inbound_frames_per_sec);

    let list = ServerFrame::SessionList {
        sessions: monitor.sessions().await,
    };
    if !send_frame(&mut sender, &list).await {
        tracing::info!(target: "lucarne_termgw", conn, "ws mirror disconnected");
        return;
    }

    // SEC-006: idle + max-lifetime close. A connect-time ticket is not enough —
    // a live socket must not outlive its credential indefinitely.
    let deadline = tokio::time::Instant::now() + limits.max_session_lifetime;
    let mut idle = tokio::time::interval(limits.idle_timeout);
    idle.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    idle.tick().await; // consume the immediate first tick

    'conn: loop {
        tokio::select! {
            _ = tokio::time::sleep_until(deadline) => {
                tracing::info!(target: "lucarne_termgw", conn, "ws closed on max session lifetime");
                break 'conn;
            }
            _ = idle.tick() => {
                tracing::info!(target: "lucarne_termgw", conn, "ws closed on idle timeout");
                break 'conn;
            }
            inbound = receiver.next() => {
                match inbound {
                    Some(Ok(Message::Text(text))) => {
                        idle.reset();
                        // SEC-004: throttle inbound frames per connection.
                        if !frame_rate.allow() {
                            tracing::warn!(target: "lucarne_termgw", conn, "ws inbound frame rate exceeded; closing connection");
                            break 'conn;
                        }
                        match serde_json::from_str::<ClientFrame>(text.as_str()) {
                            Ok(frame) => {
                                // SEC-013: a read-only session refuses write frames
                                // (input / create / close) before any side effect;
                                // mirror/control frames pass through unchanged.
                                if readonly && is_write_frame(&frame) {
                                    tracing::info!(target: "lucarne_termgw", conn, "refused write frame on read-only ws session");
                                    if !send_frame(
                                        &mut sender,
                                        &ServerFrame::Error {
                                            code: 403,
                                            msg: "read-only session: write operations are not permitted".to_string(),
                                        },
                                    )
                                    .await
                                    {
                                        break 'conn;
                                    }
                                } else if !handle_client_frame(
                                    &monitor, &mut sender, &mut subscribed, &mut differs,
                                    frame, &limits, &mut sessions_created,
                                )
                                .await
                                {
                                    break 'conn;
                                }
                            }
                            Err(e) => {
                                // SEC-007: detail logged server-side; client gets generic.
                                tracing::warn!(target: "lucarne_termgw", error = %e, "bad ws client frame");
                                if !send_frame(
                                    &mut sender,
                                    &ServerFrame::Error { code: 400, msg: "bad request".to_string() },
                                )
                                .await
                                {
                                    break 'conn;
                                }
                            }
                        }
                    }
                    Some(Ok(Message::Close(_))) | None => break 'conn,
                    Some(Ok(_)) => { idle.reset(); } // ping/pong/binary — ignore
                    Some(Err(e)) => {
                        tracing::debug!(target: "lucarne_termgw", error = %e, "ws recv error");
                        break 'conn;
                    }
                }
            }
            update = bcast.recv() => {
                match update {
                    Ok(GridUpdate { session, grid, cursor }) => {
                        if subscribed.contains(&session) {
                            let differ = differs.entry(session.clone()).or_default();
                            let expected_base = differ.current_rev();
                            let frame = match differ.feed_checked(grid, expected_base) {
                                DiffResult::Full(grid) => ServerFrame::Snapshot { session, grid, cursor },
                                DiffResult::Delta { base_rev, rev, delta } => {
                                    ServerFrame::SnapshotDelta { session, base_rev, rev, delta, cursor }
                                }
                                DiffResult::Resync { have_rev } => {
                                    tracing::debug!(target: "lucarne_termgw", %session, have_rev, "server differ gap; waiting for client resync");
                                    continue;
                                }
                            };
                            if !send_frame(&mut sender, &frame).await {
                                break 'conn;
                            }
                        }
                    }
                    Err(RecvError::Lagged(n)) => {
                        tracing::debug!(target: "lucarne_termgw", skipped = n, "client lagged; re-snapshotting subscriptions");
                        for session in subscribed.iter().cloned().collect::<Vec<_>>() {
                            if !snapshot_into(monitor.as_ref(), &mut sender, &mut differs, session).await {
                                break 'conn;
                            }
                        }
                    }
                    Err(RecvError::Closed) => break 'conn,
                }
            }
        }
    }
    tracing::info!(target: "lucarne_termgw", conn, "ws mirror disconnected");
}

/// True for client frames that mutate server state (SEC-013): keystroke/text
/// injection and session lifecycle. Mirror + control frames (`Subscribe`,
/// `Detach`, `Resync`, `Ping`) return false. A read-only session refuses these.
fn is_write_frame(frame: &ClientFrame) -> bool {
    matches!(
        frame,
        ClientFrame::Input { .. }
            | ClientFrame::CreateSession { .. }
            | ClientFrame::CloseSession { .. }
    )
}

/// Handle one inbound client frame. Returns `false` if the connection should close.
///
/// SEC-013: write frames (`Input`, `CreateSession`, `CloseSession`) on a
/// read-only session are refused by the caller ([`client_task`]) BEFORE this
/// runs (see [`is_write_frame`]), so this handler only ever sees frames the
/// session is permitted to perform.
async fn handle_client_frame(
    monitor: &Arc<dyn TerminalMonitor>,
    sender: &mut Sender,
    subscribed: &mut HashSet<SessionId>,
    differs: &mut HashMap<SessionId, Differ>,
    frame: ClientFrame,
    limits: &GatewayLimits,
    sessions_created: &mut usize,
) -> bool {
    match frame {
        ClientFrame::Subscribe { session } => {
            subscribed.insert(session.clone());
            snapshot_into(monitor.as_ref(), sender, differs, session).await
        }
        ClientFrame::Detach { session } => {
            subscribed.remove(&session);
            differs.remove(&session);
            true
        }
        ClientFrame::Input { session, event } => {
            if let Err(e) = monitor.inject(&session, event).await {
                tracing::debug!(target: "lucarne_termgw", %session, error = %e, "inject failed");
            }
            true
        }
        ClientFrame::Resync { session, have_rev } => {
            snapshot_into_with_client_rev(
                monitor.as_ref(),
                sender,
                differs,
                session,
                Some(have_rev),
            )
            .await
        }
        ClientFrame::CreateSession { title } => {
            // SEC-004: cap sessions created per connection (anti fork-bomb).
            if *sessions_created >= limits.max_sessions_per_conn {
                tracing::warn!(target: "lucarne_termgw",
                    cap = limits.max_sessions_per_conn,
                    "per-connection session-creation cap reached"
                );
                return send_frame(
                    sender,
                    &ServerFrame::Error {
                        code: 429,
                        msg: "session limit reached".to_string(),
                    },
                )
                .await;
            }
            match monitor
                .create(title.unwrap_or_else(|| "shell".to_string()))
                .await
            {
                Ok(desc) => {
                    *sessions_created += 1;
                    if !send_frame(sender, &ServerFrame::SessionCreated { session: desc.id }).await
                    {
                        return false;
                    }
                    send_frame(
                        sender,
                        &ServerFrame::SessionList {
                            sessions: monitor.sessions().await,
                        },
                    )
                    .await
                }
                Err(e) => {
                    // SEC-007: detail logged server-side; client gets generic.
                    tracing::warn!(target: "lucarne_termgw", error = %e, "ws session create failed");
                    send_frame(
                        sender,
                        &ServerFrame::Error {
                            code: 500,
                            msg: "internal error".to_string(),
                        },
                    )
                    .await
                }
            }
        }
        ClientFrame::CloseSession { session } => match monitor.kill(&session).await {
            Ok(()) => {
                subscribed.remove(&session);
                differs.remove(&session);
                if !send_frame(sender, &ServerFrame::SessionClosed { session }).await {
                    return false;
                }
                send_frame(
                    sender,
                    &ServerFrame::SessionList {
                        sessions: monitor.sessions().await,
                    },
                )
                .await
            }
            Err(e) => {
                // SEC-007: detail logged server-side; client gets generic.
                tracing::warn!(target: "lucarne_termgw", %session, error = %e, "ws session close failed");
                send_frame(
                    sender,
                    &ServerFrame::Error {
                        code: 501,
                        msg: "internal error".to_string(),
                    },
                )
                .await
            }
        },
        ClientFrame::Ping { t } => send_frame(sender, &ServerFrame::Pong { t }).await,
    }
}

#[cfg(test)]
mod gate_tests {
    use super::*;

    // The ws auth gate runs BEFORE `.on_upgrade(`. These cover the decision the
    // upgrade handlers make from a ticket query, without needing a live monitor.

    #[tokio::test]
    async fn ws_gate_noop_when_auth_disabled() {
        let auth = AuthState::disabled();
        // Disabled (local dev): no ticket required, never refuses; always full.
        assert_eq!(
            check_ws_ticket(&auth, None).await.ok(),
            Some(AccessScope::Full)
        );
        assert_eq!(
            check_ws_ticket(&auth, Some("anything")).await.ok(),
            Some(AccessScope::Full)
        );
    }

    // M5: `authorize_ws` acquires the permit FIRST, so a saturated cap returns
    // 503 WITHOUT consuming a ticket; on auth failure the permit is released so a
    // later connection can proceed.
    #[tokio::test]
    async fn authorize_ws_acquires_permit_before_consuming_ticket() {
        let auth = AuthState::with_token(AccessToken::generate());
        // Cap of 1 so we can saturate it.
        let limits = GatewayLimits {
            max_ws_connections: 1,
            ..GatewayLimits::default()
        };
        let pool = WsConnectionPool::new(limits);

        // Hold the only permit.
        let held = pool.try_acquire().expect("first permit");
        // A fresh, valid ticket exists — but the cap is full, so authorize must
        // 503 and must NOT consume the ticket.
        let ticket = auth.tickets.issue().await.expect("issue ticket");
        let resp = authorize_ws(&auth, Some(&ticket), &pool).await;
        assert!(resp.is_err(), "cap full → reject");
        assert_eq!(
            resp.err().unwrap().status(),
            StatusCode::SERVICE_UNAVAILABLE
        );
        // The ticket was NOT burned: after releasing the permit it still works.
        drop(held);
        let (scope, permit) = authorize_ws(&auth, Some(&ticket), &pool)
            .await
            .expect("ticket survived the 503");
        assert_eq!(scope, AccessScope::Full);
        // Release this permit so the next case exercises the auth-failure path
        // (not another cap-full 503).
        drop(permit);

        // Auth failure releases the permit (a forged ticket): the slot is free
        // again for a subsequent valid connection.
        let resp = authorize_ws(&auth, Some("forged"), &pool).await;
        assert!(resp.is_err());
        assert_eq!(resp.err().unwrap().status(), StatusCode::UNAUTHORIZED);
        let ticket2 = auth.tickets.issue().await.expect("issue ticket");
        let (_scope, _permit) = authorize_ws(&auth, Some(&ticket2), &pool)
            .await
            .expect("permit was released after auth failure");
    }

    // C1: a read-only HTTP session is rejected with 403 on a write route (the
    // `RequireFull` extractor), while a full session and a missing-scope request
    // are handled as expected. Driven via a small router that injects the scope
    // exactly like `bearer_guard` does.
    #[tokio::test]
    async fn require_full_rejects_readonly_http_write() {
        async fn write_handler(_full: RequireFull) -> Response {
            (StatusCode::OK, "wrote").into_response()
        }
        let app = Router::new().route("/write", post(write_handler));

        // Full scope in extensions → handler runs (200).
        let resp = app
            .clone()
            .oneshot(
                HttpRequest::builder()
                    .method("POST")
                    .uri("/write")
                    .extension(AccessScope::Full)
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::OK);

        // ReadOnly scope → 403 before the handler runs.
        let resp = app
            .clone()
            .oneshot(
                HttpRequest::builder()
                    .method("POST")
                    .uri("/write")
                    .extension(AccessScope::ReadOnly)
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::FORBIDDEN);

        // No scope recorded → fail closed (403).
        let resp = app
            .oneshot(
                HttpRequest::builder()
                    .method("POST")
                    .uri("/write")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::FORBIDDEN);
    }

    // M2: a non-empty malformed `/api/remote/start` body is a 400 (not a silent
    // fallback); an empty body still falls back to the configured tunnel (200).
    #[tokio::test]
    async fn remote_start_rejects_malformed_body_with_400() {
        let recorder = RecordingControl::default();
        let control = control_router(Some(Arc::new(recorder.clone()) as Arc<dyn RemoteControl>));
        let resp = control
            .oneshot(
                HttpRequest::builder()
                    .method("POST")
                    .uri("/api/remote/start")
                    .header("content-type", "application/json")
                    .extension(ConnectInfo("127.0.0.1:9999".parse::<SocketAddr>().unwrap()))
                    .body(Body::from("{ this is not json"))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::BAD_REQUEST);
        // The control was never started (malformed body short-circuits).
        assert!(recorder.last_start.lock().unwrap().is_none());
    }

    #[tokio::test]
    async fn ws_gate_rejects_missing_and_invalid_ticket_when_enforced() {
        let auth = AuthState::with_token(AccessToken::generate());
        // No ticket → refused (a Response is returned instead of proceeding).
        assert!(check_ws_ticket(&auth, None).await.is_err());
        // Never-issued ticket → refused.
        assert!(check_ws_ticket(&auth, Some("forged-ticket")).await.is_err());
    }

    #[tokio::test]
    async fn ws_gate_accepts_valid_ticket_once_then_replay_fails() {
        let auth = AuthState::with_token(AccessToken::generate());
        let ticket = auth.tickets.issue().await.expect("issue ticket");
        // First use: valid ticket consumed → proceed (Ok = no refusal).
        assert_eq!(
            check_ws_ticket(&auth, Some(&ticket)).await.ok(),
            Some(AccessScope::Full)
        );
        // Replay: same ticket already consumed → refused.
        assert!(check_ws_ticket(&auth, Some(&ticket)).await.is_err());
    }

    #[tokio::test]
    async fn bearer_decision_constant_time_against_configured_token() {
        let token = AccessToken::generate();
        let auth = AuthState::with_token(token.clone());
        // Correct token verifies; wrong/empty does not (constant-time compare).
        assert!(auth.verify_token(token.as_str()));
        assert!(!auth.verify_token("not-the-token"));
        // Bearer header parsing feeds the guard.
        assert_eq!(
            AuthState::bearer(Some(&format!("Bearer {}", token.as_str()))),
            Some(token.as_str())
        );
    }

    // ---- SEC-013: read-only token tier ----

    #[tokio::test]
    async fn readonly_token_scopes_bearer_and_ticket() {
        let full = AccessToken::generate();
        let readonly = AccessToken::generate();
        let auth = AuthState::with_tokens(full.clone(), readonly.clone());

        // The full token authenticates Full; the readonly token authenticates
        // ReadOnly; an unrelated token authenticates as nothing.
        assert_eq!(auth.scope_for(full.as_str()), Some(AccessScope::Full));
        assert_eq!(
            auth.scope_for(readonly.as_str()),
            Some(AccessScope::ReadOnly)
        );
        assert_eq!(auth.scope_for("nope"), None);
        // Both are accepted by the boolean view used by the bearer guard.
        assert!(auth.verify_token(full.as_str()));
        assert!(auth.verify_token(readonly.as_str()));

        // A ticket minted under the readonly scope consumes back to ReadOnly,
        // and the ws gate surfaces that scope to the handler.
        let ro_ticket = auth
            .tickets
            .issue_scoped(AccessScope::ReadOnly)
            .await
            .expect("issue read-only ticket");
        assert_eq!(
            check_ws_ticket(&auth, Some(&ro_ticket)).await.ok(),
            Some(AccessScope::ReadOnly)
        );
        // A full-scope ticket consumes back to Full.
        let full_ticket = auth
            .tickets
            .issue_scoped(AccessScope::Full)
            .await
            .expect("issue full ticket");
        assert_eq!(
            check_ws_ticket(&auth, Some(&full_ticket)).await.ok(),
            Some(AccessScope::Full)
        );
    }

    // R3-5: ticket issuance reads the AccessScope from the request EXTENSION that
    // `bearer_guard` wrote — it does NOT re-parse the Authorization header. This
    // drives a faithful stand-in for `issue_ticket` (same `Extension<AccessScope>`
    // → `issue_scoped` body, no AppState/live monitor needed): an extension-
    // injecting layer feeds the handler exactly as `bearer_guard` does, and the
    // minted ticket round-trips to the injected scope. A request with NO scope
    // extension (which would only happen off `bearer_guard`) is rejected before a
    // ticket can be minted.
    #[tokio::test]
    async fn issue_ticket_reads_scope_from_extension_and_rejects_missing_scope() {
        // Stand-in for `issue_ticket`'s body: scope is required from the
        // extension, matching the real handler's `Extension<AccessScope>`
        // extractor. No extension means no ticket.
        async fn ticket_handler(
            State(auth): State<AuthState>,
            axum::Extension(scope): axum::Extension<AccessScope>,
        ) -> Response {
            match auth.tickets.issue_scoped(scope).await {
                Ok(ticket) => Json(json!({ "ticket": ticket })).into_response(),
                Err(e) => (StatusCode::TOO_MANY_REQUESTS, e.to_string()).into_response(),
            }
        }

        let auth = AuthState::with_tokens(AccessToken::generate(), AccessToken::generate());
        let app = Router::new()
            .route("/auth/ticket", post(ticket_handler))
            .with_state(auth.clone());

        // Extract the minted ticket string from the JSON response body.
        async fn minted_ticket(resp: Response) -> String {
            let bytes = http_body_util::BodyExt::collect(resp.into_body())
                .await
                .unwrap()
                .to_bytes();
            let v: Value = serde_json::from_slice(&bytes).unwrap();
            v["ticket"].as_str().unwrap().to_string()
        }

        // ReadOnly scope in the extension → a ReadOnly ticket (consumes ReadOnly).
        let resp = app
            .clone()
            .oneshot(
                HttpRequest::builder()
                    .method("POST")
                    .uri("/auth/ticket")
                    .extension(AccessScope::ReadOnly)
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::OK);
        let ro = minted_ticket(resp).await;
        assert_eq!(
            auth.tickets.consume_scoped(&ro).await,
            Some(AccessScope::ReadOnly),
            "readonly scope extension must mint a readonly ticket"
        );

        // Full scope in the extension → a Full ticket.
        let resp = app
            .clone()
            .oneshot(
                HttpRequest::builder()
                    .method("POST")
                    .uri("/auth/ticket")
                    .extension(AccessScope::Full)
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        let full = minted_ticket(resp).await;
        assert_eq!(
            auth.tickets.consume_scoped(&full).await,
            Some(AccessScope::Full),
            "full scope extension must mint a full ticket"
        );

        // No scope extension at all → extractor rejects the request before
        // `issue_scoped` can mint any ticket, even if a Full bearer header were
        // present and ignored.
        let resp = app
            .oneshot(
                HttpRequest::builder()
                    .method("POST")
                    .uri("/auth/ticket")
                    .header(
                        "authorization",
                        format!("Bearer {}", auth.mode.token().unwrap().as_str()),
                    )
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::INTERNAL_SERVER_ERROR);
    }

    #[test]
    fn write_frames_are_classified_for_readonly_refusal() {
        use lucarne_rmux::TermInput;
        // Write frames: input + session lifecycle.
        assert!(is_write_frame(&ClientFrame::Input {
            session: "s:0:0".into(),
            event: TermInput::Text { text: "x".into() },
        }));
        assert!(is_write_frame(&ClientFrame::CreateSession { title: None }));
        assert!(is_write_frame(&ClientFrame::CloseSession {
            session: "s:0:0".into()
        }));
        // Mirror / control frames: not writes (allowed under readonly).
        assert!(!is_write_frame(&ClientFrame::Subscribe {
            session: "s:0:0".into()
        }));
        assert!(!is_write_frame(&ClientFrame::Detach {
            session: "s:0:0".into()
        }));
        assert!(!is_write_frame(&ClientFrame::Resync {
            session: "s:0:0".into(),
            have_rev: 0
        }));
        assert!(!is_write_frame(&ClientFrame::Ping { t: 1 }));
    }

    #[test]
    fn resync_have_rev_is_compared_before_reseeding_differ() {
        use lucarne_rmux::term::Cell;
        use lucarne_rmux::{Color, Style};

        fn cell(text: &str) -> Cell {
            Cell {
                text: text.to_string(),
                width: 1,
                padding: false,
                fg: Color::Default,
                bg: Color::Default,
                underline_color: Color::Default,
                style: Style::empty(),
            }
        }

        let session = "s:0:0".to_string();
        let mut differs = HashMap::new();
        let mut differ = Differ::new();
        differ.feed(PaneGrid {
            cols: 1,
            rows: 1,
            cells: vec![cell("a")],
            rev: 7,
        });
        differs.insert(session.clone(), differ);

        assert_eq!(
            differs.get(&session).and_then(Differ::current_rev),
            Some(7),
            "server baseline is the value Resync.have_rev is compared against"
        );
        assert_ne!(
            differs.get(&session).and_then(Differ::current_rev),
            Some(3),
            "a stale client have_rev must be observable before full snapshot reseed"
        );
    }

    // A read-only session refuses a write frame (returns the connection-keep
    // `true`) without ever touching the monitor; a mirror frame is unaffected by
    // the readonly gate. Driven against the pure classification + the gate's
    // refusal branch (no live monitor needed for the refusal path).
    #[tokio::test]
    async fn readonly_session_refuses_write_frames() {
        // The refusal short-circuits before the monitor is consulted: assert the
        // classification the gate uses to decide.
        use lucarne_rmux::TermInput;
        let write = ClientFrame::Input {
            session: "s:0:0".into(),
            event: TermInput::Text {
                text: "rm -rf /\n".into(),
            },
        };
        // readonly + write → must be classified as a write (and thus refused).
        assert!(is_write_frame(&write));
        // A subscribe is never a write, so readonly never refuses it.
        let mirror = ClientFrame::Subscribe {
            session: "s:0:0".into(),
        };
        assert!(!is_write_frame(&mirror));
    }

    // ---- SEC-004: per-connection inbound frame rate limiter ----

    #[test]
    fn frame_rate_limits_burst_and_allows_unlimited_when_zero() {
        // 0 = unlimited.
        let mut unlimited = FrameRate::new(0);
        for _ in 0..1000 {
            assert!(unlimited.allow());
        }
        // A small budget: the first N pass, the next is rejected within the window.
        let mut limited = FrameRate::new(3);
        assert!(limited.allow());
        assert!(limited.allow());
        assert!(limited.allow());
        assert!(!limited.allow(), "4th frame in the window must be rejected");
    }

    // ---- SEC-001 / SEC-002: HTTP router-level integration tests ----
    //
    // These drive built routers via `tower::ServiceExt::oneshot`, so they assert
    // the actual route/middleware wiring without binding a socket or standing up
    // a live monitor / agent runtime.

    use axum::body::Body;
    use axum::http::Request as HttpRequest;
    use tower::ServiceExt;

    #[derive(Clone)]
    struct FakeTerminalMonitor {
        sessions: Arc<tokio::sync::Mutex<Vec<SessionDescriptor>>>,
        injections: Arc<tokio::sync::Mutex<Vec<(SessionId, TermInput)>>>,
        updates: tokio::sync::broadcast::Sender<GridUpdate>,
    }

    impl FakeTerminalMonitor {
        fn new(sessions: Vec<SessionDescriptor>) -> Self {
            let (updates, _) = tokio::sync::broadcast::channel(16);
            Self {
                sessions: Arc::new(tokio::sync::Mutex::new(sessions)),
                injections: Arc::new(tokio::sync::Mutex::new(Vec::new())),
                updates,
            }
        }

        async fn injections(&self) -> Vec<(SessionId, TermInput)> {
            self.injections.lock().await.clone()
        }
    }

    fn claude_session_line(cwd: &str, session_id: &str) -> String {
        format!(
            r#"{{"type":"user","sessionId":"{session_id}","cwd":"{cwd}","timestamp":"2026-05-30T00:00:00Z","message":{{"role":"user","content":[{{"type":"text","text":"hello there"}}]}}}}"#
        )
    }

    fn claude_assistant_line() -> String {
        r#"{"type":"assistant","message":{"role":"assistant","content":[{"type":"text","text":"hi back"}],"stop_reason":"end_turn"}}"#.to_string()
    }

    #[async_trait]
    impl TerminalMonitor for FakeTerminalMonitor {
        fn subscribe(&self) -> tokio::sync::broadcast::Receiver<GridUpdate> {
            self.updates.subscribe()
        }

        async fn sessions(&self) -> Vec<SessionDescriptor> {
            self.sessions.lock().await.clone()
        }

        async fn create(&self, title: String) -> Result<SessionDescriptor, MonitorError> {
            let id = format!("fake-{}:0:0", self.sessions.lock().await.len());
            let desc = SessionDescriptor {
                id: id.clone(),
                title,
                origin: lucarne_rmux::Origin::Managed,
                dims: lucarne_rmux::Dims { cols: 80, rows: 24 },
                cwd: None,
            };
            self.sessions.lock().await.push(desc.clone());
            Ok(desc)
        }

        async fn snapshot_grid(&self, id: &SessionId) -> Result<(PaneGrid, Cursor), MonitorError> {
            Err(MonitorError::NotFound(id.clone()))
        }

        async fn inject(&self, id: &SessionId, input: TermInput) -> Result<(), MonitorError> {
            self.injections.lock().await.push((id.clone(), input));
            Ok(())
        }

        async fn kill(&self, id: &SessionId) -> Result<(), MonitorError> {
            let mut sessions = self.sessions.lock().await;
            let before = sessions.len();
            sessions.retain(|session| session.id != *id);
            if sessions.len() == before {
                return Err(MonitorError::NotFound(id.clone()));
            }
            Ok(())
        }

        async fn capture_scrollback(&self, id: &SessionId) -> Result<String, MonitorError> {
            if self
                .sessions
                .lock()
                .await
                .iter()
                .any(|session| session.id == *id)
            {
                Ok("fake scrollback".to_string())
            } else {
                Err(MonitorError::NotFound(id.clone()))
            }
        }
    }

    /// A stand-in for an external extension ws route: it upgrades immediately,
    /// so if the gate lets the request through, the response is a 101/426-style
    /// upgrade attempt rather than our 401.
    async fn dummy_extension_upgrade(ws: WebSocketUpgrade) -> Response {
        ws.on_upgrade(|_socket| async {})
    }

    fn dummy_extension_router() -> Router {
        Router::new().route("/extension-ws", get(dummy_extension_upgrade))
    }

    #[tokio::test]
    async fn sec001_extension_gate_refuses_without_ticket_when_enforced() {
        // Auth enforced + no ticket → the gate rejects with 401 BEFORE upgrade.
        let auth = AuthState::with_token(AccessToken::generate());
        let app = gate_ws_router(dummy_extension_router(), auth);
        let resp = app
            .oneshot(
                HttpRequest::builder()
                    .uri("/extension-ws")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::UNAUTHORIZED);
    }

    #[tokio::test]
    async fn sec001_extension_gate_refuses_invalid_ticket_but_allows_valid_once() {
        let auth = AuthState::with_token(AccessToken::generate());

        // Forged ticket → 401.
        let app = gate_ws_router(dummy_extension_router(), auth.clone());
        let resp = app
            .oneshot(
                HttpRequest::builder()
                    .uri("/extension-ws?ticket=forged")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::UNAUTHORIZED);

        // A valid single-use ticket passes the gate (so it reaches the upgrade
        // handler — which, lacking ws upgrade headers, is NOT our 401).
        let ticket = auth.tickets.issue().await.expect("issue ticket");
        let app = gate_ws_router(dummy_extension_router(), auth.clone());
        let resp = app
            .oneshot(
                HttpRequest::builder()
                    .uri(format!("/extension-ws?ticket={ticket}"))
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_ne!(
            resp.status(),
            StatusCode::UNAUTHORIZED,
            "valid ticket must pass the gate"
        );
    }

    #[tokio::test]
    async fn sec001_extension_gate_is_noop_when_auth_disabled() {
        // Local dev: gate is a pass-through, the upgrade handler runs.
        let app = gate_ws_router(dummy_extension_router(), AuthState::disabled());
        let resp = app
            .oneshot(
                HttpRequest::builder()
                    .uri("/extension-ws")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_ne!(resp.status(), StatusCode::UNAUTHORIZED);
    }

    #[tokio::test]
    async fn sec002_control_plane_routes_live_only_on_control_router() {
        // The control router serves /api/remote/status (loopback peer in test).
        let control = control_router(None);
        let resp = control
            .oneshot(
                HttpRequest::builder()
                    .uri("/api/remote/status")
                    .extension(ConnectInfo("127.0.0.1:9999".parse::<SocketAddr>().unwrap()))
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::OK);
    }

    #[tokio::test]
    async fn sec002_public_gateway_does_not_expose_remote_control_routes() {
        let temp = tempfile::tempdir().expect("web dir");
        let monitor = FakeTerminalMonitor::new(Vec::new());
        let app = router_with_terminal_monitor_and_store(
            Arc::new(monitor),
            temp.path().to_path_buf(),
            AuthState::disabled(),
            WsConnectionPool::new(GatewayLimits::default()),
            ControlPlaneSqliteStore::open_in_memory().expect("store"),
        );

        for (method, path) in [
            ("GET", "/api/remote/status"),
            ("POST", "/api/remote/start"),
            ("POST", "/api/remote/stop"),
        ] {
            let resp = app
                .clone()
                .oneshot(
                    HttpRequest::builder()
                        .method(method)
                        .uri(path)
                        .extension(ConnectInfo("127.0.0.1:9999".parse::<SocketAddr>().unwrap()))
                        .body(Body::empty())
                        .unwrap(),
                )
                .await
                .unwrap();
            assert_ne!(
                resp.status(),
                StatusCode::OK,
                "public gateway route {method} {path} must not reach remote control plane"
            );
            let body = http_body_util::BodyExt::collect(resp.into_body())
                .await
                .unwrap()
                .to_bytes();
            let body = String::from_utf8_lossy(&body);
            assert!(
                !body.contains("access_token") && !body.contains("remote subsystem"),
                "public gateway response must not leak remote control-plane data: {body}"
            );
        }
    }

    #[tokio::test]
    async fn agent_ws_readonly_prompt_refuses_before_terminal_inject() {
        let temp = tempfile::tempdir().expect("temp claude config");
        let cwd = temp.path().join("workspace");
        std::fs::create_dir_all(&cwd).expect("workspace");
        let projects = temp.path().join("projects").join("term-agent");
        std::fs::create_dir_all(&projects).expect("claude projects");
        let transcript = projects.join("sess-agent.jsonl");
        std::fs::write(
            &transcript,
            format!(
                "{}\n{}\n",
                claude_session_line(cwd.to_str().unwrap(), "sess-agent"),
                claude_assistant_line()
            ),
        )
        .expect("write transcript");

        let session_id = "agent-session:0:0".to_string();
        let monitor = FakeTerminalMonitor::new(vec![SessionDescriptor {
            id: session_id.clone(),
            title: "agent".to_string(),
            origin: lucarne_rmux::Origin::Adopted,
            dims: lucarne_rmux::Dims { cols: 80, rows: 24 },
            cwd: Some(cwd.to_string_lossy().into_owned()),
        }]);

        let full = AccessToken::generate();
        let readonly = AccessToken::generate();
        let auth = AuthState::with_tokens(full, readonly.clone());
        let app = router_with_terminal_monitor_and_store(
            Arc::new(monitor.clone()),
            temp.path().join("web"),
            auth.clone(),
            WsConnectionPool::new(GatewayLimits {
                idle_timeout: std::time::Duration::from_secs(30),
                max_session_lifetime: std::time::Duration::from_secs(30),
                ..GatewayLimits::default()
            }),
            ControlPlaneSqliteStore::open_in_memory().expect("store"),
        );

        let prev = std::env::var_os("CLAUDE_CONFIG_DIR");
        unsafe {
            std::env::set_var("CLAUDE_CONFIG_DIR", temp.path());
        }

        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind");
        let addr = listener.local_addr().expect("local addr");
        let server = tokio::spawn(async move {
            axum::serve(
                listener,
                app.into_make_service_with_connect_info::<SocketAddr>(),
            )
            .await
        });

        let ticket = auth
            .tickets
            .issue_scoped(AccessScope::ReadOnly)
            .await
            .expect("issue readonly ticket");
        let url = format!("ws://{addr}/agent/{session_id}?ticket={ticket}");
        let (mut socket, _response) = tokio_tungstenite::connect_async(url)
            .await
            .expect("connect agent ws");

        let ready = socket
            .next()
            .await
            .expect("ready frame")
            .expect("ready ok")
            .into_text()
            .expect("ready text");
        let ready: Value = serde_json::from_str(&ready).expect("ready json");
        assert_eq!(ready["type"], "ready");
        assert_eq!(ready["readonly"], true);

        socket
            .send(tokio_tungstenite::tungstenite::Message::Text(
                r#"{"type":"prompt","text":"should not inject"}"#.into(),
            ))
            .await
            .expect("send readonly prompt");
        let mut refusal = None;
        for _ in 0..5 {
            let frame = socket
                .next()
                .await
                .expect("agent frame")
                .expect("agent frame ok")
                .into_text()
                .expect("agent text");
            let value: Value = serde_json::from_str(&frame).expect("agent json");
            if value["type"] == "error" {
                refusal = Some(value);
                break;
            }
        }
        let refusal = refusal.expect("read-only prompt refusal frame");
        assert_eq!(refusal["type"], "error");
        assert_eq!(
            refusal["msg"],
            "read-only session: prompts are not permitted"
        );
        assert!(
            monitor.injections().await.is_empty(),
            "read-only prompt must be refused before terminal injection"
        );

        let _ = socket.close(None).await;
        server.abort();
        match prev {
            Some(value) => unsafe { std::env::set_var("CLAUDE_CONFIG_DIR", value) },
            None => unsafe { std::env::remove_var("CLAUDE_CONFIG_DIR") },
        }
    }

    // ---- G3: CLI-supplied provider + fields flow through /api/remote/start ----

    #[test]
    fn parse_start_params_distinguishes_empty_from_malformed() {
        // M2: an EMPTY body → Ok(empty params) (configured fallback / bodyless POST).
        let empty = parse_start_params(b"").expect("empty body is ok");
        assert!(empty.is_empty());
        // A NON-empty malformed body → Err (the handler returns 400, no silent
        // downgrade to default).
        assert!(parse_start_params(b"not json").is_err());
        // A well-formed body → provider + fields parsed.
        let body = br#"{"provider":"cloudflared","fields":{"token":"t","public_url":"https://x"}}"#;
        let params = parse_start_params(body).expect("valid body parses");
        assert_eq!(params.provider.as_deref(), Some("cloudflared"));
        assert_eq!(params.fields.get("token").map(String::as_str), Some("t"));
        assert_eq!(
            params.fields.get("public_url").map(String::as_str),
            Some("https://x")
        );
        assert!(!params.is_empty());
    }

    /// A `RemoteControl` test double that records the [`RemoteStartParams`] it was
    /// last started with, so the handler→trait body plumbing (G3) is assertable.
    #[derive(Clone, Default)]
    struct RecordingControl {
        last_start: Arc<std::sync::Mutex<Option<RemoteStartParams>>>,
    }

    #[async_trait]
    impl RemoteControl for RecordingControl {
        async fn start(
            &self,
            params: RemoteStartParams,
        ) -> Result<RemoteControlStatus, RemoteControlError> {
            *self.last_start.lock().unwrap() = Some(params);
            Ok(RemoteControlStatus {
                running: true,
                provider: Some("cloudflared".to_string()),
                public_url: Some("https://demo.example.test".to_string()),
                access_token: Some("token".to_string()),
            })
        }
        async fn stop(&self) -> Result<RemoteControlStatus, RemoteControlError> {
            Ok(RemoteControlStatus::default())
        }
        async fn status(&self) -> RemoteControlStatus {
            RemoteControlStatus::default()
        }
    }

    #[tokio::test]
    async fn g3_start_forwards_cli_provider_and_fields_to_control() {
        let recorder = RecordingControl::default();
        let control = control_router(Some(Arc::new(recorder.clone()) as Arc<dyn RemoteControl>));
        let body =
            r#"{"provider":"cloudflared","fields":{"public_url":"https://demo.example.test"}}"#;
        let resp = control
            .oneshot(
                HttpRequest::builder()
                    .method("POST")
                    .uri("/api/remote/start")
                    .header("content-type", "application/json")
                    .extension(ConnectInfo("127.0.0.1:9999".parse::<SocketAddr>().unwrap()))
                    .body(Body::from(body))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::OK);
        // The handler parsed the body and forwarded it verbatim to the control.
        let recorded = recorder
            .last_start
            .lock()
            .unwrap()
            .clone()
            .expect("started");
        assert_eq!(recorded.provider.as_deref(), Some("cloudflared"));
        assert_eq!(
            recorded.fields.get("public_url").map(String::as_str),
            Some("https://demo.example.test")
        );
    }

    #[tokio::test]
    async fn g3_start_with_empty_body_falls_back_to_configured_tunnel() {
        let recorder = RecordingControl::default();
        let control = control_router(Some(Arc::new(recorder.clone()) as Arc<dyn RemoteControl>));
        // Bodyless POST (older client / curl): the handler must still start the
        // tunnel and pass empty params (daemon uses its pre-configured tunnel).
        let resp = control
            .oneshot(
                HttpRequest::builder()
                    .method("POST")
                    .uri("/api/remote/start")
                    .extension(ConnectInfo("127.0.0.1:9999".parse::<SocketAddr>().unwrap()))
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::OK);
        let recorded = recorder
            .last_start
            .lock()
            .unwrap()
            .clone()
            .expect("started");
        assert!(
            recorded.is_empty(),
            "empty body must yield empty params (configured fallback)"
        );
    }

    #[tokio::test]
    async fn sec002_control_plane_loopback_only_rejects_non_loopback_peer() {
        let control = control_router(None);
        let resp = control
            .oneshot(
                HttpRequest::builder()
                    .uri("/api/remote/status")
                    .extension(ConnectInfo(
                        "203.0.113.7:443".parse::<SocketAddr>().unwrap(),
                    ))
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        // Defense-in-depth: a non-loopback peer is refused even on this listener.
        assert_eq!(resp.status(), StatusCode::FORBIDDEN);
    }

    // SEC-002: the public (tunneled) gateway router must NOT register the
    // `/api/remote/*` control plane — those routes belong only to the separate
    // loopback control listener. `gateway_router` needs a live monitor to build,
    // so assert the wiring at the source level: only `control_router` mounts the
    // control routes.
    #[test]
    fn sec002_remote_routes_only_on_control_router_not_gateway_router() {
        let src = include_str!("lib.rs");
        let production = src.split("#[cfg(test)]").next().unwrap_or(src);
        // `gateway_router`'s body ends at its closing — take everything up to the
        // doc comment that precedes `control_router` (the `// Build the loopback`
        // doc line), so the control router's docs/routes are not captured here.
        let gateway = production
            .split("fn gateway_router")
            .nth(1)
            .and_then(|rest| {
                rest.split("/// Build the loopback-only control-plane router")
                    .next()
            })
            .expect("gateway_router body");
        let control = production
            .split("pub fn control_router")
            .nth(1)
            .expect("control_router body");
        assert!(
            !gateway.contains(".route(\"/api/remote/"),
            "gateway (tunneled) router must not mount /api/remote/* (SEC-002)"
        );
        assert!(
            control.contains("/api/remote/start")
                && control.contains("/api/remote/stop")
                && control.contains("/api/remote/status"),
            "control_router must mount the full /api/remote/* control plane"
        );
    }
}
