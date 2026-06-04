//! Remote-access subsystem wiring for `lucarned`.
//!
//! Owns the public-tunnel lifecycle inside the daemon (Locked decision L6).
//! Remote access is a middle layer: it exposes a selected local capability on a
//! loopback listener, then asks the selected tunnel provider from
//! [`lucarne_remote::builtin`] to publish that listener. The current product
//! bundle exposes the terminal gateway capability, but the tunnel provider and
//! exposure manager do not own rmux semantics.
//!
//! The CLI drives this over the loopback-only `/api/remote/{start,stop,status}`
//! routes on a dedicated off-tunnel control listener. Those routes call
//! [`RemoteExposureManager`] (the daemon's [`lucarne_termgw::RemoteControl`]
//! implementation) without exposing the control plane through the public
//! terminal gateway.

use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::Arc;

use async_trait::async_trait;
use lucarne::control_plane::ControlPlaneSqliteStore;
use lucarne_remote::{ProviderConfig, RemoteRegistry, TunnelHandle, TunnelStatus};
use lucarne_rmux::RmuxMonitor;
use lucarne_termgw::{
    AccessToken, AuthState, ForwardedIdentityPolicy, GatewayLimits, RemoteControl,
    RemoteControlError, RemoteControlStatus, RemoteStartParams, WsConnectionPool,
};
use tokio::sync::{watch, Mutex};
use tracing::{info, warn};

#[cfg(test)]
#[async_trait]
trait GatewayStarter: Send + Sync {
    async fn start_gateway(&self) -> Result<(), RemoteControlError>;
}

/// Local capability selected for remote exposure.
///
/// This keeps RemoteAccess as a tunnel/admission layer. The terminal gateway is
/// one capability that can be published, not the definition of remote access.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ExposedCapability {
    TerminalGateway,
}

/// Web asset directory served by the gateway for local/static clients. Env
/// `LUCARNED_REMOTE_WEB` overrides; defaults to `web` (relative to the daemon's
/// working dir), matching the `termgw-dev` runner.
const DEFAULT_WEB_DIR: &str = "web";

/// How often the H3 health watcher polls the running tunnel and reaps it if the
/// provider reports the child has exited (so `/api/remote/status` reflects
/// reality and a crashed tunnel can be restarted without a manual status call).
const REAPER_INTERVAL: std::time::Duration = std::time::Duration::from_secs(15);

/// Resolved remote-access runtime configuration (produced by
/// `remote_config_from_config` in `main.rs`, after env overrides + the
/// loopback-hardened gateway address parse).
///
/// H6c: the provider's own configuration is a GENERIC, transport-agnostic
/// `provider_fields` map (keyed by `RequiredField::key`) — the daemon no longer
/// owns cloudflare-specific fields. Adding FRP / relay / any future backend is a
/// new provider impl + a `providers.<id>` block in `lucarned.yaml`, with zero
/// daemon-config changes (AGENTS.md provider boundary; ADR L1/L2/L7).
#[derive(Clone, Debug)]
pub struct RemoteRuntimeConfig {
    /// Tunnel backend provider id (e.g. `"cloudflared"`).
    pub provider: String,
    /// Loopback gateway bind address (already validated loopback — L3). This is
    /// the ONLY port the tunnel targets.
    pub gateway_addr: SocketAddr,
    /// Loopback control-plane bind address (SEC-002). A DISTINCT port from
    /// `gateway_addr` that the tunnel never targets; serves `/api/remote/*` and
    /// returns the `access_token`. Already validated loopback.
    pub control_addr: SocketAddr,
    /// Configured gateway access token; `None` → generate at startup (L4).
    pub auth_token: Option<String>,
    /// Optional read-only access token (SEC-013); `None` → no read-only tier.
    pub readonly_token: Option<String>,
    /// Explicit opt-out of auth (loud warning; never the default — L4).
    pub insecure: bool,
    /// H6c: opaque per-provider field map (keyed by the provider's
    /// `RequiredField::key`, e.g. cloudflared's `token` / `public_url` /
    /// `binary_path`). The daemon passes this straight through to
    /// [`ProviderConfig`] without interpreting any field — provider-specific
    /// structure stays at the provider boundary.
    pub provider_fields: std::collections::BTreeMap<String, String>,
    /// Capability published through the remote-access tunnel.
    pub capability: ExposedCapability,
    /// Cold-daemon lazy start (this change): when `true` the daemon auto-starts
    /// the configured tunnel at boot (the historical `remote.enabled:true`
    /// behaviour — gateway + monitor + tunnel come up immediately). When `false`
    /// the control plane is still served from boot but the gateway / rmux monitor
    /// / tunnel stay idle until the first `lucarned remote start` (`/api/remote/start`).
    pub autostart: bool,
}

impl RemoteRuntimeConfig {
    /// Build the opaque per-provider [`ProviderConfig`] for the tunnel backend
    /// (Locked decision L2: providers take a flat key→value map, no daemon
    /// types leak in). H6c: this is now a pure copy of the generic
    /// `provider_fields` map — the daemon maps NO provider-specific field names.
    ///
    /// G3: `overrides` are CLI-supplied field values (from `lucarned remote start`)
    /// merged **over** the daemon's configured fields — a present override wins,
    /// an absent one keeps the configured value. An empty `overrides` map yields
    /// exactly the daemon's pre-configured fields (backward compatible).
    fn provider_config(
        &self,
        overrides: &std::collections::BTreeMap<String, String>,
    ) -> ProviderConfig {
        let mut cfg = ProviderConfig::new();
        // Daemon-configured provider fields (verbatim — no per-field mapping).
        for (key, value) in &self.provider_fields {
            if value.is_empty() {
                continue;
            }
            cfg.fields.insert(key.clone(), value.clone());
        }
        // G3: CLI-supplied fields override / extend the configured ones.
        for (key, value) in overrides {
            if value.is_empty() {
                continue;
            }
            cfg.fields.insert(key.clone(), value.clone());
        }
        cfg
    }
}

/// Handle returned to `run_daemon` after the subsystem is up — carries the
/// fields the daemon logs (`provider`, `public_url`). The live tunnel + control
/// plane live in the spawned tasks / the `Arc<RemoteExposureManager>`.
///
/// `public_url` is `Some` only when the tunnel was actually started (autostart),
/// and `None` when the control plane is up but the tunnel is still idle (cold
/// daemon waiting for `lucarned remote start`).
pub struct RemoteSubsystem {
    pub provider: String,
    pub public_url: Option<String>,
}

/// The daemon's [`RemoteControl`] implementation: it owns the tunnel lifecycle
/// (Locked decision L6) and is driven by the gateway's loopback-only
/// `/api/remote/{start,stop,status}` routes.
///
/// H4: a small [`TunnelState`] state machine behind a `Mutex` lets `start` /
/// `stop` / `status` run the provider's `start` / `stop` / `health` **outside**
/// the lock. The lock is only held to read/transition the state and take out the
/// data needed for the await; the long-running provider await then runs lock-free
/// (so status/stop/start never block each other and shutdown can't deadlock),
/// and the result is written back under a fresh lock.
struct RemoteExposureManager {
    registry: RemoteRegistry,
    config: RemoteRuntimeConfig,
    /// The gateway access token handed to remote clients (`#token=…`). Present
    /// unless running `insecure` with no token.
    access_token: Option<String>,
    /// Tunnel lifecycle state machine (H4). Guards transitions; provider awaits
    /// happen with this lock released.
    state: Mutex<TunnelState>,
    /// Auth/admission state for the selected exposed capability. For the current
    /// terminal capability this feeds the termgw router.
    auth: AuthState,
    ws_pool: WsConnectionPool,
    control_store: ControlPlaneSqliteStore,
    web_dir: PathBuf,
    /// Once-guard for the lazy capability bring-up. For `TerminalGateway`, this
    /// means rmux connect + termgw bind/serve. Future capabilities should add
    /// their own branch in `ensure_capability_ready` instead of pushing tunnel
    /// logic into rmux.
    capability_up: Mutex<bool>,
    #[cfg(test)]
    gateway_starter: Option<Arc<dyn GatewayStarter>>,
}

/// Tunnel lifecycle (H4). `Starting` / `Stopping` are transient markers so a
/// concurrent caller observes work-in-progress instead of racing a second
/// provider start/stop while the first is awaiting lock-free.
enum TunnelState {
    /// No tunnel running and none being started.
    Idle,
    /// A `start` is in flight (provider await running lock-free).
    Starting,
    /// A tunnel is up; the handle is needed to `stop` / `health` it.
    Running(TunnelHandle),
    /// A `stop` is in flight (provider await running lock-free).
    Stopping,
}

impl RemoteExposureManager {
    fn status_from(&self, handle: Option<&TunnelHandle>) -> RemoteControlStatus {
        match handle {
            Some(h) => RemoteControlStatus {
                running: true,
                provider: Some(h.provider_id.clone()),
                public_url: Some(h.public_url.to_string()),
                access_token: self.access_token.clone(),
            },
            None => RemoteControlStatus {
                running: false,
                provider: None,
                public_url: None,
                access_token: self.access_token.clone(),
            },
        }
    }
}

#[async_trait]
impl RemoteControl for RemoteExposureManager {
    /// H4: the `self.state` lock is NEVER held across an `await`. The
    /// already-running idempotent path clones the [`TunnelHandle`] out under the
    /// lock (phase 1a), probes `provider.health(&handle)` with the lock RELEASED
    /// (phase 1b), then re-acquires the lock and re-checks that the SAME handle is
    /// still `Running` (compares `opaque`) before deciding the transition (phase
    /// 1c) — so a concurrent stop/start that ran during the lock-free health await
    /// is never clobbered. The provider `start` spawn (phase 2) and the
    /// write-back of its outcome (phase 3) are likewise split across the lock.
    async fn start(
        &self,
        params: RemoteStartParams,
    ) -> Result<RemoteControlStatus, RemoteControlError> {
        // Phase 1a (locked): inspect state, handle the busy cases, and — for a
        // running tunnel — clone the handle out so the health probe runs OUTSIDE
        // the lock (H4: no state lock is ever held across an `await`). For Idle we
        // claim `Starting` directly here so a concurrent start can't double-spawn.
        let running_handle = {
            let mut guard = self.state.lock().await;
            match &*guard {
                // Already running → clone the handle and verify health lock-free
                // (Phase 1b). We do NOT decide the transition under this lock.
                TunnelState::Running(handle) => Some(handle.clone()),
                TunnelState::Idle => {
                    *guard = TunnelState::Starting;
                    None
                }
                TunnelState::Starting => {
                    return Err(RemoteControlError::Backend(
                        "a tunnel start is already in progress".to_string(),
                    ));
                }
                TunnelState::Stopping => {
                    return Err(RemoteControlError::Backend(
                        "a tunnel stop is in progress; retry shortly".to_string(),
                    ));
                }
            }
        };

        // Phase 1b (lock-free): if a tunnel was running, probe its health with the
        // state lock released. H3: a dead tunnel is restarted; a healthy one is the
        // idempotent success path.
        if let Some(handle) = running_handle {
            let healthy = match self.registry.lookup(&handle.provider_id) {
                Some(p) => !matches!(
                    p.health(&handle).await.unwrap_or(TunnelStatus::Unknown),
                    TunnelStatus::Down
                ),
                None => false,
            };

            // Phase 1c (locked): re-acquire and re-check the state. A concurrent
            // start/stop may have changed it while health awaited lock-free, so we
            // only act when the SAME handle is still Running (compare opaque).
            {
                let mut guard = self.state.lock().await;
                match &*guard {
                    TunnelState::Running(current) if current.opaque == handle.opaque => {
                        if healthy {
                            // Idempotent: still up and healthy.
                            return Ok(self.status_from(Some(current)));
                        }
                        // Dead/unknown: drop the stale handle and claim Starting so
                        // we restart below (Phase 2/3).
                        info!("lucarned remote: tunnel reported Down on start; restarting");
                        *guard = TunnelState::Starting;
                    }
                    // The state moved on under us (a concurrent stop reaped it, or a
                    // start replaced the handle). Report the live status without
                    // racing a second spawn.
                    TunnelState::Running(current) => {
                        return Ok(self.status_from(Some(current)));
                    }
                    TunnelState::Idle => {
                        // A concurrent stop cleared it; claim Starting and restart.
                        *guard = TunnelState::Starting;
                    }
                    TunnelState::Starting => {
                        return Err(RemoteControlError::Backend(
                            "a tunnel start is already in progress".to_string(),
                        ));
                    }
                    TunnelState::Stopping => {
                        return Err(RemoteControlError::Backend(
                            "a tunnel stop is in progress; retry shortly".to_string(),
                        ));
                    }
                }
            }
        }

        // Phase 2 (lock-free): resolve provider + config, then await the spawn.
        let result = self.do_start(params).await;

        // Phase 3 (locked): write the outcome back.
        let mut guard = self.state.lock().await;
        match result {
            Ok(handle) => {
                let status = self.status_from(Some(&handle));
                *guard = TunnelState::Running(handle);
                Ok(status)
            }
            Err(e) => {
                // The start failed; return to Idle so a retry is possible.
                *guard = TunnelState::Idle;
                Err(e)
            }
        }
    }

    async fn stop(&self) -> Result<RemoteControlStatus, RemoteControlError> {
        // Phase 1 (locked): take the running handle (if any) and claim Stopping.
        let handle = {
            let mut guard = self.state.lock().await;
            match &*guard {
                TunnelState::Running(_) => {
                    // Replace with Stopping and extract the handle.
                    match std::mem::replace(&mut *guard, TunnelState::Stopping) {
                        TunnelState::Running(handle) => handle,
                        _ => unreachable!("matched Running above"),
                    }
                }
                // Nothing running (Idle / a transient start/stop): succeed
                // idempotently without touching the in-flight transition.
                _ => return Ok(self.status_from(None)),
            }
        };

        // SEC-011: audit the tunnel stop (provider + public host; no token).
        info!(
            provider = %handle.provider_id,
            public_host = handle.public_url.host_str().unwrap_or(""),
            "lucarned remote: tunnel stopping"
        );

        // Phase 2 (lock-free): run the provider stop await.
        let provider = match self.registry.lookup(&handle.provider_id) {
            Some(p) => p,
            None => {
                // Unknown provider: the handle is unusable, so drop it. Treat as
                // stopped (no process we can reap through this registry).
                let mut guard = self.state.lock().await;
                *guard = TunnelState::Idle;
                return Ok(self.status_from(None));
            }
        };
        let provider_id = handle.provider_id.clone();
        let stop_result = provider.stop(handle.clone()).await;

        // Phase 3 (locked): M1 — only clear the handle on success / NotFound /
        // Down (a tunnel that is genuinely gone). A recoverable error keeps the
        // handle so the caller can retry `stop`.
        let mut guard = self.state.lock().await;
        match stop_result {
            Ok(()) => {
                *guard = TunnelState::Idle;
                Ok(self.status_from(None))
            }
            Err(lucarne_remote::RemoteError::NotFound(_)) => {
                // The provider has no live child for this handle → already gone.
                *guard = TunnelState::Idle;
                Ok(self.status_from(None))
            }
            Err(e) => {
                // M1: recoverable error — keep the handle for a retry.
                warn!(provider = %provider_id, error = %e, "lucarned remote: tunnel stop failed; retaining handle");
                *guard = TunnelState::Running(handle);
                Err(RemoteControlError::Backend(e.to_string()))
            }
        }
    }

    async fn status(&self) -> RemoteControlStatus {
        // Phase 1 (locked): clone the handle out so the health await is lock-free.
        let handle = {
            let guard = self.state.lock().await;
            match &*guard {
                TunnelState::Running(handle) => Some(handle.clone()),
                _ => None,
            }
        };
        let Some(handle) = handle else {
            return self.status_from(None);
        };

        // Phase 2 (lock-free): H3 — ask the provider for live health.
        let health = match self.registry.lookup(&handle.provider_id) {
            Some(p) => p.health(&handle).await.unwrap_or(TunnelStatus::Unknown),
            None => TunnelStatus::Down,
        };

        if matches!(health, TunnelStatus::Down) {
            // H3: a dead tunnel is reaped so a future `start` can relaunch and
            // `/api/remote/status` reflects reality (running=false).
            let mut guard = self.state.lock().await;
            // Only clear if still the same running handle (avoid clobbering a
            // concurrent start/stop transition).
            if let TunnelState::Running(current) = &*guard {
                if current.opaque == handle.opaque {
                    info!("lucarned remote: tunnel health=Down; clearing handle (status)");
                    *guard = TunnelState::Idle;
                }
            }
            return self.status_from(None);
        }

        self.status_from(Some(&handle))
    }
}

impl RemoteExposureManager {
    /// Lazily bring up the gateway exactly once (cold-daemon lazy start).
    ///
    /// On a cold daemon (`autostart:false`) the control plane is served from boot
    /// but the rmux monitor + termgw gateway are NOT — the first `start()` (from
    /// `lucarned remote start`, or autostart) runs this. It connects the system rmux
    /// monitor, builds the termgw router on the shared ws pool, binds the
    /// loopback gateway, and spawns its serve task.
    ///
    /// H4 discipline: the `capability_up` lock is NEVER held across the heavy awaits
    /// (rmux connect / bind / serve). Phase 1 reads the flag under the lock and
    /// returns early if already up; phase 2 does the work lock-free; phase 3
    /// re-acquires the lock, re-checks the flag (a concurrent caller may have
    /// raced us), and only commits the spawn + sets the flag when it is still
    /// down — otherwise it discards the duplicate listener so we never double-bind
    /// or double-serve.
    ///
    /// An rmux connect / bind failure returns [`RemoteControlError::Backend`] so
    /// the CLI gets a clear error WITHOUT crashing the daemon (the control plane
    /// stays up; a later `lucarned remote start` can retry once rmux is available).
    async fn ensure_capability_ready(&self) -> Result<(), RemoteControlError> {
        match self.config.capability {
            ExposedCapability::TerminalGateway => self.ensure_terminal_gateway().await,
        }
    }

    async fn ensure_terminal_gateway(&self) -> Result<(), RemoteControlError> {
        // Phase 1 (locked): already up → nothing to do.
        if *self.capability_up.lock().await {
            return Ok(());
        }

        #[cfg(test)]
        if let Some(starter) = &self.gateway_starter {
            starter.start_gateway().await?;
            let mut guard = self.capability_up.lock().await;
            if !*guard {
                *guard = true;
            }
            return Ok(());
        }

        // Phase 2 (lock-free): connect rmux, build + bind the gateway. None of
        // these awaits hold the `capability_up` lock (H4).
        let monitor = Arc::new(
            RmuxMonitor::connect()
                .await
                .map_err(|e| RemoteControlError::Backend(format!("rmux connect failed: {e}")))?,
        );
        let adopted = monitor
            .adopt_all()
            .await
            .map_err(|e| RemoteControlError::Backend(format!("rmux adopt failed: {e}")))?;
        info!(
            sessions = adopted.len(),
            "lucarned remote: adopted system rmux sessions"
        );

        // H1: build the gateway router on the shared ws pool so `/ws` and
        // `/agent` share one global connection cap. Web apps are external
        // consumers of this gateway API; the daemon no longer owns a separate
        // web-chat runtime crate.
        let app = lucarne_termgw::router_with_pool_and_store(
            monitor,
            self.web_dir.clone(),
            self.auth.clone(),
            self.ws_pool.clone(),
            self.control_store.clone(),
        );
        let listener = tokio::net::TcpListener::bind(self.config.gateway_addr)
            .await
            .map_err(|e| {
                RemoteControlError::Backend(format!(
                    "gateway bind {} failed: {e}",
                    self.config.gateway_addr
                ))
            })?;
        let bound = listener
            .local_addr()
            .map_err(|e| RemoteControlError::Backend(format!("gateway local_addr failed: {e}")))?;

        // Phase 3 (locked): re-check the flag — a concurrent caller may have won
        // the race while we were connecting/binding lock-free. If so, discard this
        // duplicate listener (drop it) so we never double-serve the same port.
        {
            let mut guard = self.capability_up.lock().await;
            if *guard {
                drop(listener);
                return Ok(());
            }
            tokio::spawn(async move {
                if let Err(err) = axum::serve(
                    listener,
                    app.into_make_service_with_connect_info::<SocketAddr>(),
                )
                .await
                {
                    warn!(error = %err, "lucarned remote gateway stopped");
                }
            });
            *guard = true;
        }
        info!(addr = %bound, "lucarned remote gateway listening (loopback)");
        Ok(())
    }

    /// Resolve provider + config and spawn the tunnel (lock-free). H6a: log the
    /// provider's own `warnings(cfg)` instead of special-casing a provider id.
    async fn do_start(
        &self,
        params: RemoteStartParams,
    ) -> Result<TunnelHandle, RemoteControlError> {
        // Cold-daemon lazy start: bring up the selected exposed capability on a
        // loopback listener before the tunnel targets it. For TerminalGateway,
        // this means rmux monitor + termgw router + bind. The tunnel layer
        // itself remains capability-agnostic.
        self.ensure_capability_ready().await?;

        // G3: a CLI-supplied provider id overrides the daemon's configured one;
        // absent → fall back to the pre-configured provider.
        let provider_id = params
            .provider
            .as_deref()
            .filter(|p| !p.is_empty())
            .unwrap_or(self.config.provider.as_str())
            .to_string();
        let provider = self
            .registry
            .lookup(&provider_id)
            .ok_or_else(|| RemoteControlError::UnknownProvider(provider_id.clone()))?;
        // G3: merge CLI fields over the daemon's configured provider fields.
        let cfg = self.config.provider_config(&params.fields);

        // M7: let the provider validate its own config (e.g. cloudflared requires
        // a named-tunnel `public_url` when a `token` is present, and checks the
        // URL is well-formed). The daemon does NOT branch on the provider id; the
        // rule lives in the provider. A violation → typed BadConfig (400).
        if let Err(detail) = provider.validate_config(&cfg) {
            return Err(RemoteControlError::BadConfig(detail));
        }

        // H6a: log any provider-declared warnings about this config (e.g. a
        // cloudflared quick tunnel exposes terminal content at the CF edge). The
        // daemon does NOT special-case the provider id; the provider owns the text.
        for warning in provider.warnings(&cfg) {
            warn!(provider = %provider_id, "lucarned remote: {warning}");
        }

        let handle = provider
            .start(self.config.gateway_addr, &cfg)
            .await
            .map_err(map_remote_error)?;
        // SEC-011: audit the tunnel start (provider + public host only; never the
        // access token).
        info!(
            provider = %handle.provider_id,
            public_host = handle.public_url.host_str().unwrap_or(""),
            "lucarned remote: tunnel started"
        );
        Ok(handle)
    }
}

/// Map a [`lucarne_remote::RemoteError`] to a typed [`RemoteControlError`] (M2):
/// a missing config field → bad config (400), a not-found provider/handle →
/// not-found (404), everything else (spawn/parse/io) → a backend error (502).
fn map_remote_error(err: lucarne_remote::RemoteError) -> RemoteControlError {
    use lucarne_remote::RemoteError;
    match err {
        RemoteError::MissingField(_) | RemoteError::Parse(_) => {
            RemoteControlError::BadConfig(err.to_string())
        }
        RemoteError::NotFound(_) => RemoteControlError::UnknownProvider(err.to_string()),
        RemoteError::Spawn { .. } | RemoteError::Io(_) => {
            RemoteControlError::Backend(err.to_string())
        }
    }
}

/// Build the gateway [`AuthState`] from the full-access token plus the optional
/// read-only token (SEC-013). A configured `readonly_token` is validated
/// (SEC-008: non-whitespace, ≥32 chars) and wired as the read-only credential;
/// when absent the behaviour is the existing single-token all-or-nothing model.
fn build_auth(
    token: AccessToken,
    readonly_token: Option<&str>,
) -> Result<AuthState, Box<dyn std::error::Error>> {
    match readonly_token.filter(|t| !t.is_empty()) {
        Some(ro) => {
            let readonly = AccessToken::from_secret_validated(ro.to_string()).map_err(
                |e| -> Box<dyn std::error::Error> { format!("remote.readonly_token: {e}").into() },
            )?;
            info!("lucarned remote: read-only access token enabled (SEC-013)");
            Ok(AuthState::with_tokens(token, readonly))
        }
        None => Ok(AuthState::with_token(token)),
    }
}

/// Start the remote-access subsystem: build the default-deny auth state, serve
/// the loopback-only control plane (so `lucarned remote start` can reach the daemon from
/// boot), and — only when `config.autostart` — bring up the gateway + tunnel.
///
/// Cold-daemon lazy start (this change): the control plane (SEC-002, port 7801)
/// is ALWAYS served, even when the tunnel is idle, so `/api/remote/status`
/// returns the access token + `running:false` from startup and `lucarned remote start`
/// is never refused with a connection-refused. The rmux monitor + termgw gateway
/// + tunnel are brought up lazily on the first `start()`:
///
/// - from `lucarned remote start`, or
/// - when `autostart` is set (`remote.enabled:true`), from a single
///   `control.start()` here at boot.
///
/// An rmux-less environment therefore no longer crashes the daemon: the control
/// plane stays up and a `start()` returns a clear [`RemoteControlError`] instead.
///
/// Returns the [`RemoteSubsystem`] handle (for daemon logging). Tunnel + gateway
/// run in spawned tasks; the tunnel is stopped when `shutdown` fires.
pub async fn spawn_remote_subsystem(
    config: RemoteRuntimeConfig,
    control_store: ControlPlaneSqliteStore,
    mut shutdown: watch::Receiver<bool>,
) -> Result<RemoteSubsystem, Box<dyn std::error::Error>> {
    // default-deny (L4): require a token. Generate one when absent unless the
    // operator explicitly opted into insecure exposure. SEC-008: an explicit
    // token must be validated (non-whitespace, ≥32 chars) and fail closed.
    let (auth, access_token) = match (&config.auth_token, config.insecure) {
        (Some(token), _) => {
            let token = AccessToken::from_secret_validated(token.clone())
                .map_err(|e| -> Box<dyn std::error::Error> { e.to_string().into() })?;
            let secret = token.as_str().to_string();
            (
                build_auth(token, config.readonly_token.as_deref())?,
                Some(secret),
            )
        }
        (None, false) => {
            let token = AccessToken::generate();
            let secret = token.as_str().to_string();
            warn!(
                "lucarned remote: no auth_token configured — generated an ephemeral \
                 access token for this session (set remote.auth_token to persist it)"
            );
            (
                build_auth(token, config.readonly_token.as_deref())?,
                Some(secret),
            )
        }
        (None, true) => {
            warn!(
                "lucarned remote: INSECURE public exposure with NO access token — anyone \
                 reaching the tunnel can drive your terminals. This is RCE-equivalent."
            );
            (AuthState::disabled(), None)
        }
    };

    // H6b: resolve the trusted forwarded-identity policy from the CONFIGURED
    // provider's own contract (cloudflared → `cf-connecting-ip`), so the gateway
    // never hardcodes a provider header. The gateway trusts the header only
    // behind the loopback tunnel source. An unknown provider / no headers → the
    // safe socket-peer-only default.
    let registry = lucarne_remote::builtin();
    let forwarded_policy = registry
        .lookup(&config.provider)
        .map(|p| ForwardedIdentityPolicy::trusting(p.forwarded_identity_headers().iter().copied()))
        .unwrap_or_default();
    let auth = auth.with_forwarded_identity(forwarded_policy);

    // H1: ONE shared ws-connection pool drives every ws route on the gateway port
    // (`/ws` + `/agent`) so a single `max_ws_connections` cap (plus the same
    // idle/lifetime/inbound-frame-rate limits) governs all of them. Built here
    // so it is shared by the lazy capability bring-up.
    let ws_pool = WsConnectionPool::new(GatewayLimits::default());

    // Web asset dir served by the terminal gateway (resolved here so capability
    // bring-up does not re-read the env per start).
    let web_dir = std::path::PathBuf::from(
        std::env::var("LUCARNED_REMOTE_WEB").unwrap_or_else(|_| DEFAULT_WEB_DIR.to_string()),
    );
    let control_addr = config.control_addr;

    // The daemon owns the exposure lifecycle; a SEPARATE loopback control
    // listener (SEC-002) forwards `/api/remote/*` to it. H4: the control starts
    // in the Idle state; its state machine runs provider awaits lock-free. The
    // selected capability is NOT connected here — it is brought up lazily on the
    // first `start()` (cold-daemon lazy start).
    let control = Arc::new(RemoteExposureManager {
        registry,
        config: config.clone(),
        access_token: access_token.clone(),
        state: Mutex::new(TunnelState::Idle),
        auth,
        ws_pool,
        control_store,
        web_dir,
        capability_up: Mutex::new(false),
        #[cfg(test)]
        gateway_starter: None,
    });

    // SEC-002: serve the loopback-only control plane on its OWN distinct port the
    // tunnel never targets. This separation — not peer-IP — is the trust boundary
    // that keeps `/api/remote/*` (and the `access_token`) off the tunnel. Always
    // served (even on a cold daemon) so `lucarned remote start` can reach the daemon.
    let control_for_plane = control.clone() as Arc<dyn RemoteControl>;
    let control_listener = tokio::net::TcpListener::bind(control_addr).await?;
    let control_bound = control_listener.local_addr()?;
    if !control_bound.ip().is_loopback() {
        return Err("remote control plane must bind a loopback address (SEC-002)".into());
    }
    tokio::spawn(async move {
        let control_app = lucarne_termgw::control_router(Some(control_for_plane));
        if let Err(err) = axum::serve(
            control_listener,
            control_app.into_make_service_with_connect_info::<SocketAddr>(),
        )
        .await
        {
            warn!(error = %err, "lucarned remote control plane stopped");
        }
    });
    info!(addr = %control_bound, "lucarned remote control plane listening (loopback-only, off-tunnel)");

    // Autostart (historical `remote.enabled:true` behaviour): bring up the
    // gateway + tunnel immediately via one `start()` (the SAME lazy path
    // `lucarned remote start` uses — capability readiness then `provider.start`).
    // When `autostart` is false the control plane is ready and the tunnel stays idle
    // until the first `lucarned remote start`. The daemon's auto-start uses its
    // pre-configured provider + fields (empty params — the G3 override path is the
    // CLI's `/api/remote/start` body).
    let public_url = if config.autostart {
        let status = control
            .start(RemoteStartParams::default())
            .await
            .map_err(|e| -> Box<dyn std::error::Error> { e.into() })?;
        Some(
            status
                .public_url
                .clone()
                .ok_or("remote tunnel started without a public URL")?,
        )
    } else {
        info!(
            "lucarned remote: control plane ready, tunnel idle — run `lucarned remote start` to open public access"
        );
        None
    };
    let provider = config.provider.clone();

    // Graceful shutdown: when the daemon signals shutdown, stop the tunnel so the
    // provider process (e.g. cloudflared) is reaped (mirrors the health subsystem
    // shutdown wiring).
    // A second receiver for the H3 health watcher below (the stop task moves the
    // original `shutdown`).
    let shutdown_rx_for_watcher = shutdown.clone();
    let shutdown_control = control.clone();
    tokio::spawn(async move {
        // Wait for the shutdown flag to flip to true.
        loop {
            if *shutdown.borrow() {
                break;
            }
            if shutdown.changed().await.is_err() {
                break;
            }
        }
        if let Err(err) = shutdown_control.stop().await {
            warn!(error = %err, "lucarned remote: tunnel stop on shutdown failed");
        } else {
            info!("lucarned remote tunnel stopped on shutdown");
        }
    });

    // H3: periodic health watcher / reaper. The provider's `health` (via
    // `status()`) detects a child that exited and reaps it (clearing the handle),
    // so a crashed tunnel is noticed and `/api/remote/status` reflects reality
    // even without a client status request. `status()` itself is a no-op when
    // Idle (nothing running), so this is cheap; it stops with the daemon.
    let watcher_control = control.clone();
    let mut watcher_shutdown = shutdown_rx_for_watcher;
    tokio::spawn(async move {
        let mut tick = tokio::time::interval(REAPER_INTERVAL);
        tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        loop {
            tokio::select! {
                _ = tick.tick() => {
                    // Polling status() runs health() and reaps a Down child.
                    let _ = watcher_control.status().await;
                }
                changed = watcher_shutdown.changed() => {
                    if changed.is_err() || *watcher_shutdown.borrow() {
                        break;
                    }
                }
            }
        }
    });

    Ok(RemoteSubsystem {
        provider,
        public_url,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::BTreeMap;
    use std::sync::atomic::{AtomicUsize, Ordering};

    use lucarne_remote::{RemoteAccessProvider, RemoteError, RemoteResult, RequiredField};
    // `TunnelHandle::public_url` is a `url::Url`; lucarned does not depend on the
    // `url` crate directly, but reqwest re-exports the very same type, so build
    // test URLs through `reqwest::Url` (== `url::Url`).
    use reqwest::Url;

    /// A 32+ char token that satisfies SEC-008 (`from_secret_validated`:
    /// non-whitespace, `>= MIN_EXPLICIT_TOKEN_LEN` = 32). Used wherever a test
    /// needs a *valid* operator-supplied token.
    const VALID_TOKEN: &str = "0123456789abcdef0123456789abcdef"; // exactly 32 chars
    const VALID_READONLY: &str = "ro-0123456789abcdef0123456789abcdef"; // > 32 chars

    fn base_config() -> RemoteRuntimeConfig {
        RemoteRuntimeConfig {
            provider: "cloudflared".to_string(),
            gateway_addr: "127.0.0.1:7800".parse().unwrap(),
            control_addr: "127.0.0.1:7801".parse().unwrap(),
            auth_token: None,
            readonly_token: None,
            insecure: false,
            provider_fields: BTreeMap::new(),
            capability: ExposedCapability::TerminalGateway,
            autostart: false,
        }
    }

    fn fields(pairs: &[(&str, &str)]) -> BTreeMap<String, String> {
        pairs
            .iter()
            .map(|(k, v)| (k.to_string(), v.to_string()))
            .collect()
    }

    /// Build a `RemoteExposureManager` for the pure-logic tests (no tunnel/rmux):
    /// it sits in `Idle` with no gateway up, so `status`/`stop`/`status_from` can
    /// be exercised without ever connecting rmux or binding a port.
    fn control(config: RemoteRuntimeConfig, access_token: Option<String>) -> RemoteExposureManager {
        control_with_registry(config, access_token, lucarne_remote::builtin(), None)
    }

    fn control_with_registry(
        config: RemoteRuntimeConfig,
        access_token: Option<String>,
        registry: lucarne_remote::RemoteRegistry,
        gateway_starter: Option<Arc<dyn GatewayStarter>>,
    ) -> RemoteExposureManager {
        RemoteExposureManager {
            registry,
            config,
            access_token,
            state: Mutex::new(TunnelState::Idle),
            auth: AuthState::disabled(),
            ws_pool: WsConnectionPool::new(GatewayLimits::default()),
            control_store: ControlPlaneSqliteStore::open_in_memory()
                .expect("open in-memory control-plane store"),
            web_dir: PathBuf::from("web"),
            capability_up: Mutex::new(false),
            gateway_starter,
        }
    }

    #[derive(Default)]
    struct CountingGatewayStarter {
        calls: AtomicUsize,
        failures: AtomicUsize,
    }

    impl CountingGatewayStarter {
        fn fail_next(&self) {
            self.failures.fetch_add(1, Ordering::SeqCst);
        }

        fn calls(&self) -> usize {
            self.calls.load(Ordering::SeqCst)
        }
    }

    #[async_trait]
    impl GatewayStarter for CountingGatewayStarter {
        async fn start_gateway(&self) -> Result<(), RemoteControlError> {
            self.calls.fetch_add(1, Ordering::SeqCst);
            if self.failures.load(Ordering::SeqCst) > 0 {
                self.failures.fetch_sub(1, Ordering::SeqCst);
                return Err(RemoteControlError::Backend("gateway failed".to_string()));
            }
            Ok(())
        }
    }

    #[derive(Clone, Default)]
    struct FakeProvider {
        state: Arc<FakeProviderState>,
    }

    #[derive(Default)]
    struct FakeProviderState {
        starts: AtomicUsize,
        stops: AtomicUsize,
        health: AtomicUsize,
        fail_starts: AtomicUsize,
        fail_stops: AtomicUsize,
        health_down: AtomicUsize,
    }

    impl FakeProvider {
        fn fail_next_start(&self) {
            self.state.fail_starts.fetch_add(1, Ordering::SeqCst);
        }

        fn fail_next_stop(&self) {
            self.state.fail_stops.fetch_add(1, Ordering::SeqCst);
        }

        fn report_down_once(&self) {
            self.state.health_down.fetch_add(1, Ordering::SeqCst);
        }

        fn starts(&self) -> usize {
            self.state.starts.load(Ordering::SeqCst)
        }

        fn stops(&self) -> usize {
            self.state.stops.load(Ordering::SeqCst)
        }

        fn health_checks(&self) -> usize {
            self.state.health.load(Ordering::SeqCst)
        }
    }

    #[async_trait]
    impl RemoteAccessProvider for FakeProvider {
        fn id(&self) -> &'static str {
            "fake"
        }

        fn name(&self) -> &'static str {
            "Fake"
        }

        fn required_fields(&self) -> &[RequiredField] {
            &[]
        }

        async fn start(
            &self,
            _local: std::net::SocketAddr,
            _cfg: &ProviderConfig,
        ) -> RemoteResult<TunnelHandle> {
            self.state.starts.fetch_add(1, Ordering::SeqCst);
            if self.state.fail_starts.load(Ordering::SeqCst) > 0 {
                self.state.fail_starts.fetch_sub(1, Ordering::SeqCst);
                return Err(RemoteError::Spawn {
                    provider: "fake".to_string(),
                    message: "start failed".to_string(),
                });
            }
            Ok(TunnelHandle {
                provider_id: "fake".to_string(),
                public_url: Url::parse("https://fake.example.test/").unwrap(),
                opaque: format!("fake-{}", self.state.starts.load(Ordering::SeqCst)),
            })
        }

        async fn stop(&self, _handle: TunnelHandle) -> RemoteResult<()> {
            self.state.stops.fetch_add(1, Ordering::SeqCst);
            if self.state.fail_stops.load(Ordering::SeqCst) > 0 {
                self.state.fail_stops.fetch_sub(1, Ordering::SeqCst);
                return Err(RemoteError::Io(std::io::Error::other("stop failed")));
            }
            Ok(())
        }

        async fn health(&self, _handle: &TunnelHandle) -> RemoteResult<TunnelStatus> {
            self.state.health.fetch_add(1, Ordering::SeqCst);
            if self.state.health_down.load(Ordering::SeqCst) > 0 {
                self.state.health_down.fetch_sub(1, Ordering::SeqCst);
                return Ok(TunnelStatus::Down);
            }
            Ok(TunnelStatus::Up)
        }
    }

    fn fake_control(
        provider: FakeProvider,
        gateway: Arc<CountingGatewayStarter>,
    ) -> RemoteExposureManager {
        let mut config = base_config();
        config.provider = "fake".to_string();
        let mut registry = lucarne_remote::RemoteRegistry::new();
        registry.register(provider);
        control_with_registry(
            config,
            Some(VALID_TOKEN.to_string()),
            registry,
            Some(gateway),
        )
    }

    // ---- G3: RemoteRuntimeConfig::provider_config override / merge ----

    #[test]
    fn provider_config_uses_daemon_fields_when_no_overrides() {
        let mut config = base_config();
        config.provider_fields =
            fields(&[("token", "cfg-token"), ("public_url", "https://a.test")]);
        let cfg = config.provider_config(&BTreeMap::new());
        assert_eq!(cfg.get("token"), Some("cfg-token"));
        assert_eq!(cfg.get("public_url"), Some("https://a.test"));
    }

    #[test]
    fn provider_config_cli_overrides_win_over_daemon_fields() {
        let mut config = base_config();
        config.provider_fields =
            fields(&[("token", "cfg-token"), ("public_url", "https://a.test")]);
        // G3: a present override wins; an absent one keeps the configured value.
        let overrides = fields(&[("token", "cli-token")]);
        let cfg = config.provider_config(&overrides);
        assert_eq!(cfg.get("token"), Some("cli-token"), "CLI override must win");
        assert_eq!(
            cfg.get("public_url"),
            Some("https://a.test"),
            "absent override keeps the configured value"
        );
    }

    #[test]
    fn provider_config_skips_empty_values_on_both_sides() {
        let mut config = base_config();
        // An empty configured field is skipped (treated as unset).
        config.provider_fields = fields(&[("token", ""), ("public_url", "https://a.test")]);
        // An empty override is skipped too — it must NOT clear a configured value.
        let overrides = fields(&[("public_url", "")]);
        let cfg = config.provider_config(&overrides);
        assert_eq!(cfg.get("token"), None, "empty configured field is skipped");
        assert_eq!(
            cfg.get("public_url"),
            Some("https://a.test"),
            "empty override must not clear the configured value"
        );
    }

    #[test]
    fn provider_config_override_extends_with_new_field() {
        let config = base_config(); // no configured fields
        let overrides = fields(&[("token", "cli-only")]);
        let cfg = config.provider_config(&overrides);
        assert_eq!(cfg.get("token"), Some("cli-only"));
    }

    // ---- build_auth: read-only token branches (SEC-013) ----

    #[test]
    fn build_auth_without_readonly_token_is_single_token() {
        let token = AccessToken::from_secret_validated(VALID_TOKEN).unwrap();
        // None / empty readonly → single-token model, must succeed.
        assert!(build_auth(token.clone(), None).is_ok());
        let token2 = AccessToken::from_secret_validated(VALID_TOKEN).unwrap();
        assert!(
            build_auth(token2, Some("")).is_ok(),
            "an empty readonly token is treated as absent (single-token model)"
        );
    }

    #[test]
    fn build_auth_with_valid_readonly_token_succeeds() {
        let token = AccessToken::from_secret_validated(VALID_TOKEN).unwrap();
        assert!(
            build_auth(token, Some(VALID_READONLY)).is_ok(),
            "a valid (>=32 char) readonly token enables the read-only tier"
        );
    }

    #[test]
    fn build_auth_rejects_weak_readonly_token() {
        let token = AccessToken::from_secret_validated(VALID_TOKEN).unwrap();
        // SEC-008: a non-empty but too-short readonly token must fail closed.
        // (`AuthState` is not `Debug`, so match the Result rather than `expect_err`.)
        match build_auth(token, Some("too-short")) {
            Ok(_) => panic!("weak readonly token must be rejected"),
            Err(err) => assert!(
                err.to_string().contains("readonly_token"),
                "error must point at the readonly_token field: {err}"
            ),
        }
    }

    // ---- M2: map_remote_error → RemoteControlError ----

    #[test]
    fn map_remote_error_missing_field_is_bad_config() {
        let mapped = map_remote_error(lucarne_remote::RemoteError::MissingField("token".into()));
        assert!(matches!(mapped, RemoteControlError::BadConfig(_)));
        assert_eq!(mapped.status_code(), axum::http::StatusCode::BAD_REQUEST);
    }

    #[test]
    fn map_remote_error_parse_is_bad_config() {
        let mapped = map_remote_error(lucarne_remote::RemoteError::Parse("bad url".into()));
        assert!(matches!(mapped, RemoteControlError::BadConfig(_)));
    }

    #[test]
    fn map_remote_error_not_found_is_unknown_provider() {
        let mapped = map_remote_error(lucarne_remote::RemoteError::NotFound("frp".into()));
        assert!(matches!(mapped, RemoteControlError::UnknownProvider(_)));
        assert_eq!(mapped.status_code(), axum::http::StatusCode::NOT_FOUND);
    }

    #[test]
    fn map_remote_error_spawn_and_io_are_backend() {
        let spawn = map_remote_error(lucarne_remote::RemoteError::Spawn {
            provider: "cloudflared".into(),
            message: "no binary".into(),
        });
        assert!(matches!(spawn, RemoteControlError::Backend(_)));
        assert_eq!(spawn.status_code(), axum::http::StatusCode::BAD_GATEWAY);

        let io = map_remote_error(lucarne_remote::RemoteError::Io(std::io::Error::other(
            "boom",
        )));
        assert!(matches!(io, RemoteControlError::Backend(_)));
    }

    // ---- RemoteExposureManager::status_from: running / idle, token passthrough ----

    #[test]
    fn status_from_running_handle_reports_provider_and_url() {
        let ctl = control(base_config(), Some(VALID_TOKEN.to_string()));
        let handle = TunnelHandle {
            provider_id: "cloudflared".to_string(),
            public_url: Url::parse("https://foo-bar.trycloudflare.com/").unwrap(),
            opaque: "op-1".to_string(),
        };
        let status = ctl.status_from(Some(&handle));
        assert!(status.running);
        assert_eq!(status.provider.as_deref(), Some("cloudflared"));
        assert_eq!(
            status.public_url.as_deref(),
            Some("https://foo-bar.trycloudflare.com/")
        );
        // L4: the access token is passed through regardless of running state.
        assert_eq!(status.access_token.as_deref(), Some(VALID_TOKEN));
    }

    #[test]
    fn status_from_none_reports_idle_but_keeps_token() {
        let ctl = control(base_config(), Some(VALID_TOKEN.to_string()));
        let status = ctl.status_from(None);
        assert!(!status.running);
        assert_eq!(status.provider, None);
        assert_eq!(status.public_url, None);
        // Token still surfaced when idle so `lucarned remote start` can render the QR.
        assert_eq!(status.access_token.as_deref(), Some(VALID_TOKEN));
    }

    #[test]
    fn status_from_passes_through_absent_token() {
        // insecure (no token) → access_token is None in both states.
        let ctl = control(base_config(), None);
        assert_eq!(ctl.status_from(None).access_token, None);
        let handle = TunnelHandle {
            provider_id: "cloudflared".to_string(),
            public_url: Url::parse("https://x.trycloudflare.com/").unwrap(),
            opaque: "op".to_string(),
        };
        assert_eq!(ctl.status_from(Some(&handle)).access_token, None);
    }

    // ---- state machine: the rmux-free reachable branches ----

    #[tokio::test]
    async fn status_when_idle_is_not_running() {
        // `status()` on an Idle control needs no rmux: it short-circuits before
        // any provider health await.
        let ctl = control(base_config(), Some(VALID_TOKEN.to_string()));
        let status = ctl.status().await;
        assert!(!status.running);
        assert_eq!(status.access_token.as_deref(), Some(VALID_TOKEN));
    }

    #[tokio::test]
    async fn stop_when_idle_is_idempotent_success() {
        // `stop()` on an Idle control is the idempotent no-op path (no rmux /
        // provider call): it returns a not-running status without erroring.
        let ctl = control(base_config(), Some(VALID_TOKEN.to_string()));
        let status = ctl.stop().await.expect("idempotent stop must succeed");
        assert!(!status.running);
        assert_eq!(status.provider, None);
    }

    #[tokio::test]
    async fn second_start_is_rejected_while_starting() {
        // Provider-lookup / validate sit behind capability readiness (which
        // connects rmux for TerminalGateway), so the reachable rmux-free
        // state-machine guard is the busy-state rejection: a control already in
        // `Starting` rejects a concurrent start.
        let ctl = control(base_config(), Some(VALID_TOKEN.to_string()));
        *ctl.state.lock().await = TunnelState::Starting;
        let err = ctl
            .start(RemoteStartParams::default())
            .await
            .expect_err("a start already in progress must be rejected");
        assert!(matches!(err, RemoteControlError::Backend(_)));
    }

    #[tokio::test]
    async fn start_success_sets_running_and_reuses_gateway_on_idempotent_start() {
        let provider = FakeProvider::default();
        let gateway = Arc::new(CountingGatewayStarter::default());
        let ctl = fake_control(provider.clone(), gateway.clone());

        let status = ctl
            .start(RemoteStartParams::default())
            .await
            .expect("start succeeds");
        assert!(status.running);
        assert_eq!(status.provider.as_deref(), Some("fake"));
        assert_eq!(
            status.public_url.as_deref(),
            Some("https://fake.example.test/")
        );
        assert_eq!(provider.starts(), 1);
        assert_eq!(gateway.calls(), 1);

        let status = ctl
            .start(RemoteStartParams::default())
            .await
            .expect("idempotent running start succeeds");
        assert!(status.running);
        assert_eq!(
            provider.starts(),
            1,
            "healthy running tunnel must not start a second provider"
        );
        assert_eq!(
            gateway.calls(),
            1,
            "lazy gateway is guarded and must start once"
        );
    }

    #[tokio::test]
    async fn start_provider_failure_returns_to_idle_for_retry() {
        let provider = FakeProvider::default();
        provider.fail_next_start();
        let gateway = Arc::new(CountingGatewayStarter::default());
        let ctl = fake_control(provider.clone(), gateway.clone());

        let err = ctl
            .start(RemoteStartParams::default())
            .await
            .expect_err("first provider start fails");
        assert!(matches!(err, RemoteControlError::Backend(_)));
        assert!(
            !ctl.status().await.running,
            "failed start must return to Idle"
        );

        let status = ctl
            .start(RemoteStartParams::default())
            .await
            .expect("retry after failed start succeeds");
        assert!(status.running);
        assert_eq!(provider.starts(), 2, "retry must call provider.start again");
        assert_eq!(
            gateway.calls(),
            1,
            "gateway already came up before provider failure and is reused"
        );
    }

    #[tokio::test]
    async fn start_gateway_failure_returns_to_idle_and_allows_retry() {
        let provider = FakeProvider::default();
        let gateway = Arc::new(CountingGatewayStarter::default());
        gateway.fail_next();
        let ctl = fake_control(provider.clone(), gateway.clone());

        let err = ctl
            .start(RemoteStartParams::default())
            .await
            .expect_err("first lazy gateway start fails");
        assert!(matches!(err, RemoteControlError::Backend(_)));
        assert!(
            !ctl.status().await.running,
            "gateway failure must return to Idle"
        );
        assert_eq!(
            provider.starts(),
            0,
            "provider must not start until gateway is available"
        );

        let status = ctl
            .start(RemoteStartParams::default())
            .await
            .expect("retry starts gateway and provider");
        assert!(status.running);
        assert_eq!(gateway.calls(), 2);
        assert_eq!(provider.starts(), 1);
    }

    #[tokio::test]
    async fn stop_running_tunnel_transitions_to_idle() {
        let provider = FakeProvider::default();
        let gateway = Arc::new(CountingGatewayStarter::default());
        let ctl = fake_control(provider.clone(), gateway);

        ctl.start(RemoteStartParams::default())
            .await
            .expect("start succeeds");
        let stopped = ctl.stop().await.expect("stop succeeds");
        assert!(!stopped.running);
        assert_eq!(provider.stops(), 1);
        assert!(!ctl.status().await.running);
    }

    #[tokio::test]
    async fn status_reaps_down_tunnel_and_next_start_relaunches() {
        let provider = FakeProvider::default();
        let gateway = Arc::new(CountingGatewayStarter::default());
        let ctl = fake_control(provider.clone(), gateway);

        ctl.start(RemoteStartParams::default())
            .await
            .expect("start succeeds");
        assert_eq!(provider.starts(), 1);

        provider.report_down_once();
        let status = ctl.status().await;
        assert!(!status.running, "health=Down must clear the running handle");
        assert_eq!(
            provider.health_checks(),
            1,
            "status probes provider health once"
        );

        let relaunched = ctl
            .start(RemoteStartParams::default())
            .await
            .expect("start after reap succeeds");
        assert!(relaunched.running);
        assert_eq!(
            provider.starts(),
            2,
            "reaped tunnel must be relaunched on the next start"
        );
    }

    #[tokio::test]
    async fn stop_recoverable_failure_retains_handle_for_retry() {
        let provider = FakeProvider::default();
        let gateway = Arc::new(CountingGatewayStarter::default());
        let ctl = fake_control(provider.clone(), gateway);

        ctl.start(RemoteStartParams::default())
            .await
            .expect("start succeeds");
        provider.fail_next_stop();

        let err = ctl
            .stop()
            .await
            .expect_err("recoverable stop failure is surfaced");
        assert!(matches!(err, RemoteControlError::Backend(_)));
        assert!(
            ctl.status().await.running,
            "recoverable stop failure must retain the running handle"
        );

        let stopped = ctl.stop().await.expect("retry stop succeeds");
        assert!(!stopped.running);
        assert_eq!(provider.stops(), 2, "retry must call provider.stop again");
    }
}
