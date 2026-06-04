//! Cloudflare Tunnel ([`cloudflared`]) remote-access provider.
//!
//! Implements [`RemoteAccessProvider`] over the `cloudflared` binary. Two modes
//! are supported and selected purely by configuration (Free decision F3, quick
//! is the default):
//!
//! * **Quick tunnel** (no `token`): spawns
//!   `cloudflared tunnel --url http://127.0.0.1:<port>`, reads the binary's
//!   stderr, and extracts the first `https://<sub>.trycloudflare.com` URL it
//!   prints. Zero-config first run with a random ephemeral domain.
//! * **Named tunnel** (`token` present): spawns
//!   `cloudflared tunnel run --token <token>`. Named tunnels have a fixed,
//!   pre-configured public hostname, so the public URL is taken from the
//!   `public_url` config field rather than parsed from stderr.
//!
//! The tunnel binary spawning is isolated from the parsing logic
//! ([`parse_quick_url`]) so the latter is unit-testable without `cloudflared`
//! installed. A missing binary is surfaced as [`RemoteError::Spawn`] — never a
//! panic.
//!
//! Per the provider boundary (Locked decision L2) this is a **pure tunnel**: it
//! only knows local addr → public URL, stop, and health. No auth.

use std::collections::HashMap;
use std::net::SocketAddr;
use std::path::{Path, PathBuf};
use std::sync::Mutex;
use std::time::Duration;

use async_trait::async_trait;
use tokio::io::{AsyncBufReadExt, BufReader};
use tokio::process::{Child, Command};
use tracing::{debug, warn};

use crate::{
    ProviderConfig, RemoteAccessProvider, RemoteError, RemoteResult, RequiredField, TunnelHandle,
    TunnelStatus,
};

/// Stable provider id.
const PROVIDER_ID: &str = "cloudflared";
/// Human-readable provider name.
const PROVIDER_NAME: &str = "Cloudflare Tunnel";

/// Config field carrying the named-tunnel token. When present and non-empty the
/// provider runs in named mode; otherwise it runs in quick mode (the default).
const FIELD_TOKEN: &str = "token";
/// Config field carrying the fixed public URL for a named tunnel.
const FIELD_PUBLIC_URL: &str = "public_url";
/// Config field carrying an explicit absolute path to the `cloudflared` binary
/// (SEC-003). When set it is used verbatim (after the same safety checks);
/// otherwise the binary is resolved to an absolute path via `$PATH`.
const FIELD_BINARY_PATH: &str = "binary_path";
/// Deprecated `remote.cloudflare` config section accepted as a provider-owned
/// compatibility alias for `remote.providers.cloudflared`.
const COMPAT_SECTION_CLOUDFLARE: &str = "cloudflare";

/// Bare binary name resolved on `$PATH` when no `binary_path` is configured.
const CLOUDFLARED_BIN: &str = "cloudflared";

/// How long [`start`](Cloudflared::start) waits for a quick tunnel to print its
/// `trycloudflare.com` URL before giving up.
const QUICK_URL_TIMEOUT: Duration = Duration::from_secs(30);

/// The fields this provider advertises for CLI prompting. The token is optional:
/// leaving it empty selects the zero-config quick tunnel. `public_url` is
/// conditionally required (M7): only when a named-tunnel `token` is present (a
/// named tunnel has a fixed hostname, so the URL cannot be parsed from stderr).
/// `binary_path` is an optional absolute path used to pin the `cloudflared`
/// binary (SEC-003); leaving it empty resolves the binary on `$PATH` to an
/// absolute path.
static REQUIRED_FIELDS: &[RequiredField] = &[
    RequiredField {
        key: FIELD_TOKEN,
        label: "Cloudflare Tunnel Token (named tunnel; leave empty for quick)",
        secret: true,
        required: false,
        required_when: None,
    },
    RequiredField {
        key: FIELD_PUBLIC_URL,
        label: "Named-tunnel public URL (e.g. https://term.example.com)",
        secret: false,
        required: false,
        // M7: required only when a (named-tunnel) token is configured.
        required_when: Some((FIELD_TOKEN, crate::ANY_VALUE)),
    },
    RequiredField {
        key: FIELD_BINARY_PATH,
        label: "Absolute path to the cloudflared binary (leave empty to resolve via PATH)",
        secret: false,
        required: false,
        required_when: None,
    },
];

/// Cloudflare Tunnel provider backed by the `cloudflared` binary.
///
/// Holds an internal registry mapping a [`TunnelHandle::opaque`] key to the
/// spawned child process (plus any secret token file written for it, L3) so it
/// can later be stopped and health-checked.
#[derive(Default)]
pub struct Cloudflared {
    /// Spawned children keyed by the opaque handle id.
    children: Mutex<HashMap<String, RunningTunnel>>,
}

/// A running cloudflared child plus the on-disk token file (if any) written for
/// it (L3). Keeping the [`TokenFile`] here ties the secret file's lifetime to the
/// child: it is removed when the tunnel is stopped (handle removed) or the
/// provider is dropped, so the token never lingers on disk after the tunnel ends.
struct RunningTunnel {
    child: Child,
    /// `Some` for a named tunnel whose token was passed via `--token-file` (L3);
    /// `None` for a quick tunnel (no token).
    _token_file: Option<TokenFile>,
}

/// A 0600 temp file holding a cloudflared named-tunnel token, removed on drop (L3).
///
/// `cloudflared tunnel run --token-file <PATH>` reads the token from a file
/// instead of argv, so the token never appears in the process command line
/// (`ps` / `/proc/<pid>/cmdline`). The file is created with `0600` (owner-only)
/// and unlinked when this value drops. NOTE (L3): the file is necessarily
/// readable by the SAME local user while it exists, and cloudflared keeps the
/// token in its own memory/argv-free state after reading; this removes the
/// `ps`-visible argv exposure, not same-user access (a same-user attacker is
/// already inside the trust boundary of the daemon).
struct TokenFile {
    path: PathBuf,
}

impl TokenFile {
    /// Write `token` to a fresh owner-only (0600) temp file next to the system
    /// temp dir. The filename embeds the pid + a nanosecond stamp so concurrent
    /// tunnels never collide.
    fn write(token: &str) -> RemoteResult<Self> {
        let nanos = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_nanos())
            .unwrap_or(0);
        let path =
            std::env::temp_dir().join(format!("lucarne-cf-token-{}-{}", std::process::id(), nanos));
        write_owner_only(&path, token.as_bytes())?;
        Ok(Self { path })
    }
}

impl Drop for TokenFile {
    fn drop(&mut self) {
        // Best-effort unlink; a failure here only leaves a 0600 file behind.
        let _ = std::fs::remove_file(&self.path);
    }
}

/// Write `bytes` to `path` creating it `0600` (owner read/write only) on unix so
/// the secret token file is never group/world readable (L3). On non-unix the
/// file is created with the platform default (no mode bits to set).
#[cfg(unix)]
fn write_owner_only(path: &Path, bytes: &[u8]) -> RemoteResult<()> {
    use std::io::Write;
    use std::os::unix::fs::OpenOptionsExt;
    let mut file = std::fs::OpenOptions::new()
        .write(true)
        .create_new(true)
        .mode(0o600)
        .open(path)
        .map_err(|e| {
            spawn_err(format!(
                "could not create token file {}: {e}",
                path.display()
            ))
        })?;
    file.write_all(bytes).map_err(|e| {
        spawn_err(format!(
            "could not write token file {}: {e}",
            path.display()
        ))
    })?;
    Ok(())
}

#[cfg(not(unix))]
fn write_owner_only(path: &Path, bytes: &[u8]) -> RemoteResult<()> {
    std::fs::write(path, bytes).map_err(|e| {
        spawn_err(format!(
            "could not write token file {}: {e}",
            path.display()
        ))
    })
}

impl Cloudflared {
    /// Create a new Cloudflared provider with an empty child registry.
    pub fn new() -> Self {
        Self::default()
    }

    /// Allocate a fresh opaque handle id used to key the child registry.
    fn new_opaque() -> String {
        // Monotonic-ish unique key; uniqueness only needs to hold within one
        // process so a nanosecond timestamp is sufficient and dependency-free.
        let nanos = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_nanos())
            .unwrap_or(0);
        format!("{PROVIDER_ID}-{nanos}")
    }

    /// Number of children currently tracked in the registry (test-only): lets a
    /// test assert that `health()` reaps a stale (exited) entry so its
    /// `TokenFile` is dropped/unlinked.
    #[cfg(test)]
    fn tracked_children(&self) -> usize {
        self.children
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .len()
    }

    /// Insert a (child, token_file) pair under a fresh opaque handle (test-only).
    /// Mirrors the bookkeeping `start` does after spawning, without needing the
    /// `cloudflared` binary, so the reaping path in `health()` is unit-testable.
    #[cfg(test)]
    fn insert_for_test(&self, child: Child, token_file: Option<TokenFile>) -> TunnelHandle {
        let opaque = Self::new_opaque();
        self.children
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .insert(
                opaque.clone(),
                RunningTunnel {
                    child,
                    _token_file: token_file,
                },
            );
        TunnelHandle {
            provider_id: PROVIDER_ID.to_string(),
            public_url: url::Url::parse("https://test.trycloudflare.com/").unwrap(),
            opaque,
        }
    }
}

/// Scan `stderr` for the first quick-tunnel public URL of the form
/// `https://<sub>.trycloudflare.com` and parse it into a [`url::Url`].
///
/// This is a hand-rolled scan (no regex dependency): it locates each
/// `https://` occurrence, reads the host up to the first delimiter, and accepts
/// it when the host ends with `.trycloudflare.com`. Robust to surrounding log
/// decoration and works on the multi-line banner cloudflared prints.
///
/// Returns `None` when no matching URL is present. Pure and side-effect free so
/// it can be unit-tested without the `cloudflared` binary.
pub fn parse_quick_url(stderr: &str) -> Option<url::Url> {
    const SCHEME: &str = "https://";
    const SUFFIX: &str = ".trycloudflare.com";

    let mut offset = 0;
    while let Some(rel) = stderr[offset..].find(SCHEME) {
        let start = offset + rel;
        let rest = &stderr[start..];

        // The URL token runs from `https://` until the first character that
        // cannot appear in a bare URL as cloudflared prints it: whitespace or
        // the punctuation it decorates the banner with. Path slashes stay part
        // of the token (so `https://x.trycloudflare.com/` is captured whole).
        let token_len = rest
            .find(|c: char| {
                c.is_whitespace()
                    || matches!(
                        c,
                        '"' | '\'' | '|' | '<' | '>' | '(' | ')' | '[' | ']' | ',' | '`'
                    )
            })
            .unwrap_or(rest.len());
        let candidate = &rest[..token_len];

        // Host is everything after the scheme, up to the first path slash.
        let after_scheme = &candidate[SCHEME.len()..];
        let host = after_scheme
            .split(['/', '?', '#'])
            .next()
            .unwrap_or(after_scheme);
        if host.len() > SUFFIX.len() && host.ends_with(SUFFIX) {
            if let Ok(url) = url::Url::parse(candidate) {
                return Some(url);
            }
        }

        // Advance past this `https://` occurrence to find the next.
        offset = start + SCHEME.len();
    }
    None
}

/// Resolve the `cloudflared` binary to a vetted **absolute** path (SEC-003).
///
/// PATH-hijack defense: a daemon must not spawn whatever `cloudflared` happens
/// to be first on `$PATH`. Resolution order:
///
/// 1. A configured `binary_path` (used verbatim) — for pinned/packaged installs.
/// 2. Otherwise an absolute lookup over `$PATH` entries (each candidate must be
///    an absolute path to an existing file).
///
/// The resolved path is then vetted: it must exist, be a regular file, and live
/// in a directory that is **not world-writable** (so an attacker can't drop a
/// replacement binary next to it). Any failure is surfaced as
/// [`RemoteError::Spawn`] — the daemon refuses to spawn rather than running an
/// untrusted binary.
fn resolve_cloudflared_binary(cfg: &ProviderConfig) -> RemoteResult<PathBuf> {
    let resolved = match cfg.get(FIELD_BINARY_PATH).filter(|p| !p.is_empty()) {
        Some(configured) => {
            let path = PathBuf::from(configured);
            if !path.is_absolute() {
                return Err(spawn_err(format!(
                    "configured `{FIELD_BINARY_PATH}` must be an absolute path, got `{configured}`"
                )));
            }
            path
        }
        None => resolve_on_path(CLOUDFLARED_BIN).ok_or_else(|| {
            spawn_err(format!(
                "could not find `{CLOUDFLARED_BIN}` on $PATH (is it installed? \
                 set `{FIELD_BINARY_PATH}` to pin an absolute path)"
            ))
        })?,
    };
    vet_binary(&resolved)?;
    Ok(resolved)
}

/// Build a [`RemoteError::Spawn`] for this provider with `message`.
fn spawn_err(message: impl Into<String>) -> RemoteError {
    RemoteError::Spawn {
        provider: PROVIDER_ID.to_string(),
        message: message.into(),
    }
}

/// Find `name` as an absolute path by scanning `$PATH` entries. Only absolute
/// PATH entries are considered (a relative `.`-style entry is an injection
/// vector and is skipped), and the candidate must be an existing file.
fn resolve_on_path(name: &str) -> Option<PathBuf> {
    let path_var = std::env::var_os("PATH")?;
    for dir in std::env::split_paths(&path_var) {
        if !dir.is_absolute() {
            continue;
        }
        let candidate = dir.join(name);
        if candidate.is_file() {
            return Some(candidate);
        }
    }
    None
}

/// Vet a resolved binary path (SEC-003): it must exist, be a regular file, and
/// sit in a directory that is not world-writable.
fn vet_binary(path: &Path) -> RemoteResult<()> {
    let meta = std::fs::metadata(path)
        .map_err(|err| spawn_err(format!("`{}` is not accessible: {err}", path.display())))?;
    if !meta.is_file() {
        return Err(spawn_err(format!(
            "`{}` is not a regular file",
            path.display()
        )));
    }
    // Reject a binary whose directory is world-writable — an attacker with write
    // access to that directory could swap the binary for a malicious one.
    if let Some(parent) = path.parent() {
        if is_world_writable(parent)? {
            return Err(spawn_err(format!(
                "refusing to spawn `{}`: its directory `{}` is world-writable",
                path.display(),
                parent.display()
            )));
        }
    }
    Ok(())
}

/// True if `dir` is world-writable (the `o+w` mode bit). On non-unix platforms
/// there is no such bit, so this is always `false`.
#[cfg(unix)]
fn is_world_writable(dir: &Path) -> RemoteResult<bool> {
    use std::os::unix::fs::PermissionsExt;
    let meta = std::fs::metadata(dir)
        .map_err(|err| spawn_err(format!("`{}` is not accessible: {err}", dir.display())))?;
    Ok(meta.permissions().mode() & 0o002 != 0)
}

#[cfg(not(unix))]
fn is_world_writable(_dir: &Path) -> RemoteResult<bool> {
    Ok(false)
}

#[async_trait]
impl RemoteAccessProvider for Cloudflared {
    fn id(&self) -> &'static str {
        PROVIDER_ID
    }

    fn name(&self) -> &'static str {
        PROVIDER_NAME
    }

    fn required_fields(&self) -> &[RequiredField] {
        REQUIRED_FIELDS
    }

    fn compat_config_sections(&self) -> &[&'static str] {
        &[COMPAT_SECTION_CLOUDFLARE]
    }

    /// H6a: warn when running a *quick* tunnel (no `token`). A quick tunnel
    /// terminates TLS at the Cloudflare edge, so terminal content is visible in
    /// plaintext there. The daemon logs this (it does NOT special-case the
    /// provider id) so the caveat lives with the provider.
    fn warnings(&self, cfg: &ProviderConfig) -> Vec<String> {
        let quick = cfg.get(FIELD_TOKEN).filter(|t| !t.is_empty()).is_none();
        if quick {
            vec![
                "Cloudflare QUICK tunnel: terminal content is visible in plaintext at the \
                 Cloudflare edge (TLS terminates there). For sensitive sessions configure a \
                 named tunnel (cloudflare.token + public_url) instead."
                    .to_string(),
            ]
        } else {
            Vec::new()
        }
    }

    /// H6b: cloudflared's edge sets `cf-connecting-ip` to the real client IP.
    /// The gateway trusts it only behind the loopback tunnel source. Keeping the
    /// header here (not hardcoded in the gateway) honors the provider boundary.
    fn forwarded_identity_headers(&self) -> &[&'static str] {
        &["cf-connecting-ip"]
    }

    /// M7: validate the named-tunnel rule — a `token` selects a named tunnel,
    /// which has a fixed hostname, so `public_url` must be supplied (it cannot be
    /// parsed from stderr the way a quick tunnel's URL is). The descriptor's
    /// `required_when` already expresses this declaratively; this override adds a
    /// `public_url` well-formedness check so a malformed URL is rejected at
    /// config time rather than mid-`start`. The rule stays inside the provider.
    fn validate_config(&self, cfg: &ProviderConfig) -> Result<(), String> {
        let named = cfg.get(FIELD_TOKEN).filter(|t| !t.is_empty()).is_some();
        if named {
            match cfg.get(FIELD_PUBLIC_URL).filter(|u| !u.is_empty()) {
                None => {
                    return Err(format!(
                        "named tunnel (a `{FIELD_TOKEN}` is set) requires `{FIELD_PUBLIC_URL}`"
                    ));
                }
                Some(url) => {
                    url::Url::parse(url).map_err(|e| {
                        format!("invalid `{FIELD_PUBLIC_URL}` for named tunnel: {e}")
                    })?;
                }
            }
        }
        Ok(())
    }

    async fn start(&self, local: SocketAddr, cfg: &ProviderConfig) -> RemoteResult<TunnelHandle> {
        // Mode is decided by the presence of a non-empty token field. Validate
        // the per-mode config (e.g. named-tunnel `public_url`) BEFORE resolving
        // or spawning the binary, so a config error is reported without touching
        // the filesystem/PATH.
        let token = cfg.get(FIELD_TOKEN).filter(|t| !t.is_empty());
        let named_public_url = match token {
            Some(_) => {
                let public_url = cfg.require(FIELD_PUBLIC_URL)?;
                let public_url = url::Url::parse(public_url).map_err(|err| {
                    RemoteError::Parse(format!(
                        "invalid `{FIELD_PUBLIC_URL}` for named tunnel: {err}"
                    ))
                })?;
                Some(public_url)
            }
            None => None,
        };

        // SEC-003: resolve `cloudflared` to a vetted absolute path before
        // spawning — never rely on a bare name found via `$PATH`.
        let binary = resolve_cloudflared_binary(cfg)?;

        // L3: for a named tunnel, write the token to a 0600 temp file and pass
        // `--token-file <PATH>` instead of `--token <token>`, so the secret never
        // appears in the process argv (`ps` / `/proc/<pid>/cmdline`). The file is
        // unlinked when the tunnel stops (its `TokenFile` is dropped with the
        // child). A quick tunnel has no token → no file.
        let token_file = match token {
            Some(token) => Some(TokenFile::write(token)?),
            None => None,
        };

        let mut command = Command::new(&binary);
        match &token_file {
            Some(tf) => {
                // Named tunnel: fixed domain, public URL comes from config; token
                // is read from the 0600 file (kept off argv — L3).
                command
                    .arg("tunnel")
                    .arg("run")
                    .arg("--token-file")
                    .arg(&tf.path);
            }
            None => {
                // Quick tunnel: random trycloudflare.com domain parsed from stderr.
                command.args([
                    "tunnel",
                    "--url",
                    &format!("http://127.0.0.1:{}", local.port()),
                ]);
            }
        }

        // cloudflared logs (including the quick-tunnel URL) go to stderr.
        //
        // H2: stderr must not block the child. In NAMED mode there is nothing to
        // parse, so discard stderr to `null` — no pipe to ever fill. In QUICK
        // mode we must read stderr to extract the URL, so we pipe it and a
        // background task drains it for the child's WHOLE lifetime (it returns
        // the first URL via a oneshot, then keeps reading so the pipe never
        // fills and back-pressures / blocks cloudflared).
        let quick_mode = named_public_url.is_none();
        command
            .stdout(std::process::Stdio::null())
            .kill_on_drop(true);
        if quick_mode {
            command.stderr(std::process::Stdio::piped());
        } else {
            command.stderr(std::process::Stdio::null());
        }

        let mut child = command
            .spawn()
            .map_err(|err| spawn_err(format!("could not launch `{}`: {err}", binary.display())))?;

        let public_url = match named_public_url {
            Some(url) => url,
            None => {
                // Take the piped stderr and spawn a lifetime-long drain task. It
                // sends the first trycloudflare URL back over a oneshot and then
                // keeps consuming stderr until EOF (child exit), so the pipe is
                // never allowed to fill (H2).
                let stderr = child.stderr.take().ok_or_else(|| RemoteError::Spawn {
                    provider: PROVIDER_ID.to_string(),
                    message: "cloudflared stderr was not captured".to_string(),
                })?;
                let (url_tx, url_rx) = tokio::sync::oneshot::channel::<url::Url>();
                tokio::spawn(drain_quick_stderr(stderr, url_tx));
                match tokio::time::timeout(QUICK_URL_TIMEOUT, url_rx).await {
                    Ok(Ok(url)) => url,
                    // The drain task ended (stream closed) before sending a URL.
                    Ok(Err(_recv)) => {
                        let _ = child.kill().await;
                        let _ = child.wait().await;
                        return Err(RemoteError::Parse(
                            "cloudflared exited before printing a trycloudflare.com URL"
                                .to_string(),
                        ));
                    }
                    Err(_elapsed) => {
                        let _ = child.kill().await;
                        let _ = child.wait().await;
                        return Err(RemoteError::Parse(format!(
                            "timed out after {}s waiting for cloudflared quick-tunnel URL",
                            QUICK_URL_TIMEOUT.as_secs()
                        )));
                    }
                }
            }
        };

        let opaque = Self::new_opaque();
        {
            let mut children = self
                .children
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner());
            children.insert(
                opaque.clone(),
                RunningTunnel {
                    child,
                    _token_file: token_file,
                },
            );
        }

        debug!(
            target: "lucarne_remote",
            provider_id = PROVIDER_ID,
            public_url = %public_url,
            "cloudflared tunnel started"
        );

        Ok(TunnelHandle {
            provider_id: PROVIDER_ID.to_string(),
            public_url,
            opaque,
        })
    }

    async fn stop(&self, handle: TunnelHandle) -> RemoteResult<()> {
        let running = {
            let mut children = self
                .children
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner());
            children.remove(&handle.opaque)
        };

        match running {
            // The token file (if any) is dropped with `running` after the child
            // is reaped, removing the secret from disk (L3).
            Some(mut running) => {
                running.child.kill().await?;
                // Reap the process so it does not linger as a zombie.
                let _ = running.child.wait().await;
                debug!(
                    target: "lucarne_remote",
                    provider_id = PROVIDER_ID,
                    "cloudflared tunnel stopped"
                );
                Ok(())
            }
            None => {
                // Unknown handle: nothing to stop. Treat as not-found rather than
                // silently succeeding so callers notice stale handles.
                warn!(
                    target: "lucarne_remote",
                    provider_id = PROVIDER_ID,
                    opaque = handle.opaque.as_str(),
                    "stop called for unknown cloudflared handle"
                );
                Err(RemoteError::NotFound(handle.opaque))
            }
        }
    }

    async fn health(&self, handle: &TunnelHandle) -> RemoteResult<TunnelStatus> {
        let mut children = self
            .children
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());

        match children.get_mut(&handle.opaque) {
            Some(running) => match running.child.try_wait() {
                // Still running: process has not exited.
                Ok(None) => Ok(TunnelStatus::Up),
                // Exited: the tunnel is down. Reap the stale entry so the dropped
                // `RunningTunnel` runs `TokenFile::drop` and unlinks the 0600
                // named-tunnel token file (L3) — otherwise a crashed named tunnel
                // would leave the secret on disk until the whole provider drops.
                // `health()` is the only place that detects the exit; matching the
                // daemon `status()` Down→Idle reaping with the on-disk cleanup here.
                Ok(Some(_status)) => {
                    children.remove(&handle.opaque);
                    debug!(
                        target: "lucarne_remote",
                        provider_id = PROVIDER_ID,
                        opaque = handle.opaque.as_str(),
                        "cloudflared child exited; reaped stale handle + token file (health)"
                    );
                    Ok(TunnelStatus::Down)
                }
                // Could not determine liveness.
                Err(err) => {
                    warn!(
                        target: "lucarne_remote",
                        provider_id = PROVIDER_ID,
                        error = %err,
                        "cloudflared try_wait failed"
                    );
                    Ok(TunnelStatus::Unknown)
                }
            },
            // No child tracked for this handle.
            None => Ok(TunnelStatus::Down),
        }
    }
}

/// Drain a quick-tunnel's stderr for the child's whole lifetime (H2).
///
/// Reads stderr line by line: the first `trycloudflare.com` URL is sent back
/// over `url_tx` (the caller waits on it to learn the public URL), and the task
/// then keeps reading until EOF so the stderr pipe is never allowed to fill —
/// a full pipe would back-pressure and block cloudflared. The receiver may be
/// dropped (timeout / already got the URL); `send` failing is fine, the loop
/// keeps draining regardless.
async fn drain_quick_stderr(
    stderr: tokio::process::ChildStderr,
    url_tx: tokio::sync::oneshot::Sender<url::Url>,
) {
    let mut reader = BufReader::new(stderr).lines();
    let mut url_tx = Some(url_tx);
    // Reads until EOF (child exited) or a read error — either ends draining.
    while let Ok(Some(line)) = reader.next_line().await {
        if url_tx.is_some() {
            if let Some(url) = parse_quick_url(&line) {
                // Send the first URL; ignore a closed receiver (timeout).
                if let Some(tx) = url_tx.take() {
                    let _ = tx.send(url);
                }
            }
        }
        // Keep reading regardless so the pipe never fills.
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A representative multi-line cloudflared quick-tunnel banner.
    const SAMPLE_STDERR: &str = "\
2026-05-30T10:00:00Z INF Thank you for trying Cloudflare Tunnel.
2026-05-30T10:00:00Z INF Requesting new quick Tunnel on trycloudflare.com...
2026-05-30T10:00:01Z INF +--------------------------------------------------------+
2026-05-30T10:00:01Z INF |  Your quick Tunnel has been created! Visit it at:      |
2026-05-30T10:00:01Z INF |  https://foo-bar.trycloudflare.com                     |
2026-05-30T10:00:01Z INF +--------------------------------------------------------+
2026-05-30T10:00:02Z INF Connection registered connIndex=0";

    #[test]
    fn parse_quick_url_extracts_trycloudflare_url() {
        let url = parse_quick_url(SAMPLE_STDERR).expect("should find trycloudflare url");
        assert_eq!(url.as_str(), "https://foo-bar.trycloudflare.com/");
        assert_eq!(url.host_str(), Some("foo-bar.trycloudflare.com"));
        assert_eq!(url.scheme(), "https");
    }

    #[test]
    fn parse_quick_url_single_line() {
        let url = parse_quick_url("see https://foo-bar.trycloudflare.com here")
            .expect("should find url mid-line");
        assert_eq!(url.host_str(), Some("foo-bar.trycloudflare.com"));
    }

    #[test]
    fn parse_quick_url_returns_first_match() {
        let blob = "https://aaa-111.trycloudflare.com then https://bbb-222.trycloudflare.com";
        let url = parse_quick_url(blob).expect("should find first url");
        assert_eq!(url.host_str(), Some("aaa-111.trycloudflare.com"));
    }

    #[test]
    fn parse_quick_url_ignores_non_trycloudflare() {
        assert!(parse_quick_url("https://example.com/foo").is_none());
        assert!(parse_quick_url("connecting to https://1.1.1.1 ...").is_none());
        assert!(parse_quick_url("no url at all in this line").is_none());
        // Bare suffix with no subdomain must not match.
        assert!(parse_quick_url("https://.trycloudflare.com").is_none());
    }

    #[test]
    fn required_fields_advertises_optional_token() {
        let provider = Cloudflared::new();
        let fields = provider.required_fields();
        // token + public_url + binary_path (M7 added the conditional public_url;
        // SEC-003 added binary_path).
        assert_eq!(fields.len(), 3);
        let token = fields
            .iter()
            .find(|f| f.key == "token")
            .expect("token field");
        assert!(token.secret, "token must be masked");
        assert!(!token.required, "token is optional (quick is default)");
        assert!(
            token.required_when.is_none(),
            "token itself is unconditional"
        );
        // M7: public_url is conditionally required only when a token is set.
        let public_url = fields
            .iter()
            .find(|f| f.key == "public_url")
            .expect("public_url field");
        assert!(
            !public_url.required,
            "public_url is not unconditionally required"
        );
        assert_eq!(
            public_url.required_when,
            Some(("token", crate::ANY_VALUE)),
            "public_url is required-when a token is present (named tunnel)"
        );
        let bin = fields
            .iter()
            .find(|f| f.key == "binary_path")
            .expect("binary_path field");
        assert!(
            !bin.required,
            "binary_path is optional (PATH resolution default)"
        );
        assert!(!bin.secret, "binary_path is not a secret");
    }

    // M7: the provider's `validate_config` enforces the named-tunnel rule
    // (`public_url` required + well-formed when a token is set) so neither the
    // daemon nor the CLI branches on the provider id.
    #[test]
    fn validate_config_enforces_named_tunnel_public_url() {
        let provider = Cloudflared::new();

        // Quick tunnel (no token) → always valid.
        assert!(provider.validate_config(&ProviderConfig::new()).is_ok());

        // Named tunnel (token) without public_url → error mentioning public_url.
        let mut named = ProviderConfig::new();
        named.fields.insert("token".to_string(), "tok".to_string());
        let err = provider
            .validate_config(&named)
            .expect_err("public_url required for named tunnel");
        assert!(err.contains("public_url"), "got: {err}");

        // Named tunnel with a malformed public_url → parse error.
        let mut bad = named.clone();
        bad.fields
            .insert("public_url".to_string(), "not a url".to_string());
        assert!(provider.validate_config(&bad).is_err());

        // Named tunnel with a valid public_url → ok.
        let mut good = named.clone();
        good.fields.insert(
            "public_url".to_string(),
            "https://term.example.com".to_string(),
        );
        assert!(provider.validate_config(&good).is_ok());
    }

    #[test]
    fn id_and_name_are_stable() {
        let provider = Cloudflared::new();
        assert_eq!(provider.id(), "cloudflared");
        assert_eq!(provider.name(), "Cloudflare Tunnel");
    }

    // H6a: a quick tunnel (no token) yields a plaintext-at-edge warning; a named
    // tunnel (token present) yields none. The daemon logs these without
    // special-casing the provider id.
    #[test]
    fn quick_tunnel_warns_named_does_not() {
        let provider = Cloudflared::new();
        // Quick mode (no token).
        let quick = ProviderConfig::new();
        let warnings = provider.warnings(&quick);
        assert_eq!(warnings.len(), 1, "quick tunnel must warn");
        assert!(
            warnings[0].contains("plaintext") && warnings[0].contains("Cloudflare edge"),
            "warning must mention plaintext at the edge: {warnings:?}"
        );
        // Empty token is treated as quick → still warns.
        let mut empty = ProviderConfig::new();
        empty.fields.insert("token".to_string(), String::new());
        assert_eq!(provider.warnings(&empty).len(), 1);
        // Named mode (non-empty token) → no warning.
        let mut named = ProviderConfig::new();
        named
            .fields
            .insert("token".to_string(), "eyJhbGc...".to_string());
        assert!(
            provider.warnings(&named).is_empty(),
            "named tunnel must not warn"
        );
    }

    // H6b: cloudflared advertises `cf-connecting-ip` as its trusted forwarded
    // header (the gateway trusts it only behind the loopback tunnel source).
    #[test]
    fn cloudflared_advertises_cf_connecting_ip_forwarded_header() {
        let provider = Cloudflared::new();
        assert_eq!(provider.forwarded_identity_headers(), &["cf-connecting-ip"]);
    }

    #[test]
    fn cloudflared_declares_legacy_config_section_alias() {
        let provider = Cloudflared::new();
        assert_eq!(provider.compat_config_sections(), &["cloudflare"]);
    }

    #[test]
    fn mode_selection_by_token_presence() {
        // Quick mode: no token field at all.
        let quick = ProviderConfig::new();
        assert!(quick.get("token").filter(|t| !t.is_empty()).is_none());

        // Quick mode: empty token is treated as absent.
        let mut empty = ProviderConfig::new();
        empty.fields.insert("token".to_string(), String::new());
        assert!(empty.get("token").filter(|t| !t.is_empty()).is_none());

        // Named mode: non-empty token present.
        let mut named = ProviderConfig::new();
        named
            .fields
            .insert("token".to_string(), "eyJhbGc...".to_string());
        assert!(named.get("token").filter(|t| !t.is_empty()).is_some());
    }

    #[tokio::test]
    async fn named_mode_requires_public_url() {
        let provider = Cloudflared::new();
        let mut cfg = ProviderConfig::new();
        cfg.fields
            .insert("token".to_string(), "a-token".to_string());
        // No public_url configured -> MissingField, without ever spawning.
        let local: SocketAddr = "127.0.0.1:7800".parse().unwrap();
        let err = provider.start(local, &cfg).await.expect_err("must error");
        assert!(
            matches!(err, RemoteError::MissingField(ref key) if key == "public_url"),
            "expected MissingField(public_url), got {err:?}"
        );
    }

    #[tokio::test]
    async fn health_of_unknown_handle_is_down() {
        let provider = Cloudflared::new();
        let handle = TunnelHandle {
            provider_id: PROVIDER_ID.to_string(),
            public_url: url::Url::parse("https://foo-bar.trycloudflare.com/").unwrap(),
            opaque: "not-tracked".to_string(),
        };
        assert_eq!(provider.health(&handle).await.unwrap(), TunnelStatus::Down);
    }

    // R3-3: when the cloudflared child has exited, `health()` reports Down AND
    // reaps the stale registry entry, so the dropped `RunningTunnel` runs
    // `TokenFile::drop` and unlinks the 0600 named-tunnel token file (L3) rather
    // than leaving the secret on disk until the whole provider drops.
    #[tokio::test]
    async fn health_reaps_exited_child_and_removes_token_file() {
        let provider = Cloudflared::new();

        // A real (named-tunnel) token file on disk, tied to the child's lifetime.
        let token_file = TokenFile::write("secret-named-tunnel-token").expect("token file");
        let token_path = token_file.path.clone();
        assert!(
            token_path.exists(),
            "token file exists while the tunnel is tracked"
        );

        // A short-lived child that exits immediately (stands in for cloudflared
        // crashing). `true` exits 0 on every supported platform.
        let mut child = Command::new("true")
            .stdout(std::process::Stdio::null())
            .stderr(std::process::Stdio::null())
            .spawn()
            .expect("spawn `true`");
        // Ensure it has actually exited before we probe health (so `try_wait`
        // observes the exit deterministically).
        let _ = child.wait().await;

        let handle = provider.insert_for_test(child, Some(token_file));
        assert_eq!(
            provider.tracked_children(),
            1,
            "child tracked before health"
        );

        // Health observes the exit → Down, AND reaps the entry.
        assert_eq!(provider.health(&handle).await.unwrap(), TunnelStatus::Down);
        assert_eq!(
            provider.tracked_children(),
            0,
            "exited child must be reaped from the registry"
        );
        // Reaping dropped the `RunningTunnel` → its `TokenFile` was unlinked.
        assert!(
            !token_path.exists(),
            "named-tunnel token file must be removed when health reaps the dead tunnel"
        );

        // A second health() on the now-untracked handle is a safe Down (no
        // double-remove panic; matches the daemon stop/status NotFound handling).
        assert_eq!(provider.health(&handle).await.unwrap(), TunnelStatus::Down);
        assert_eq!(provider.tracked_children(), 0);
    }

    // ---- SEC-003: cloudflared binary pinning / vetting ----

    #[test]
    fn configured_binary_path_must_be_absolute() {
        let mut cfg = ProviderConfig::new();
        cfg.fields
            .insert("binary_path".to_string(), "cloudflared".to_string());
        let err = resolve_cloudflared_binary(&cfg).expect_err("relative path must be rejected");
        assert!(
            matches!(err, RemoteError::Spawn { ref message, .. } if message.contains("absolute")),
            "expected absolute-path Spawn error, got {err:?}"
        );
    }

    #[test]
    fn configured_binary_path_rejected_when_missing() {
        let mut cfg = ProviderConfig::new();
        // An absolute path that does not exist must fail closed (no spawn).
        cfg.fields.insert(
            "binary_path".to_string(),
            "/nonexistent/definitely/not/here/cloudflared".to_string(),
        );
        let err = resolve_cloudflared_binary(&cfg).expect_err("missing binary must be rejected");
        assert!(
            matches!(err, RemoteError::Spawn { .. }),
            "expected Spawn error for missing binary, got {err:?}"
        );
    }

    #[test]
    fn configured_binary_path_accepts_vetted_absolute_file() {
        // A real existing file in a non-world-writable dir resolves cleanly. Use
        // the test binary's own dir as a representative vetted location.
        let dir = tempfile::tempdir().expect("tempdir");
        let bin = dir.path().join("cloudflared");
        std::fs::write(&bin, b"#!/bin/sh\n").expect("write fake binary");
        // tempdir is created 0700 by default — not world-writable.
        let mut cfg = ProviderConfig::new();
        cfg.fields.insert(
            "binary_path".to_string(),
            bin.to_string_lossy().into_owned(),
        );
        let resolved = resolve_cloudflared_binary(&cfg).expect("vetted absolute file accepted");
        assert_eq!(resolved, bin);
    }

    #[cfg(unix)]
    #[test]
    fn world_writable_binary_directory_is_refused() {
        use std::os::unix::fs::PermissionsExt;
        let dir = tempfile::tempdir().expect("tempdir");
        // Make the directory world-writable (o+w).
        let mut perms = std::fs::metadata(dir.path()).unwrap().permissions();
        perms.set_mode(0o777);
        std::fs::set_permissions(dir.path(), perms).expect("chmod 777");
        let bin = dir.path().join("cloudflared");
        std::fs::write(&bin, b"#!/bin/sh\n").expect("write fake binary");

        let mut cfg = ProviderConfig::new();
        cfg.fields.insert(
            "binary_path".to_string(),
            bin.to_string_lossy().into_owned(),
        );
        let err = resolve_cloudflared_binary(&cfg)
            .expect_err("world-writable binary dir must be refused");
        assert!(
            matches!(err, RemoteError::Spawn { ref message, .. } if message.contains("world-writable")),
            "expected world-writable Spawn error, got {err:?}"
        );
    }

    // L3: the named-tunnel token is written to a 0600 file (so it stays off the
    // process argv) and the file is removed when the `TokenFile` drops.
    #[cfg(unix)]
    #[test]
    fn token_file_is_owner_only_and_removed_on_drop() {
        use std::os::unix::fs::PermissionsExt;
        let path = {
            let tf = TokenFile::write("super-secret-tunnel-token").expect("write token file");
            // Exists while held, with 0600 permissions (owner read/write only).
            let meta = std::fs::metadata(&tf.path).expect("token file exists");
            assert_eq!(
                meta.permissions().mode() & 0o777,
                0o600,
                "token file must be owner-only (0600)"
            );
            let content = std::fs::read_to_string(&tf.path).expect("read token file");
            assert_eq!(content, "super-secret-tunnel-token");
            tf.path.clone()
        };
        // Dropped → unlinked, so the secret does not linger on disk.
        assert!(!path.exists(), "token file must be removed on drop");
    }
}
