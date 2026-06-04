//! Public-exposure auth layer for the terminal gateway.
//!
//! The gateway mirrors a live terminal with input re-injection: an unauthenticated
//! reader is effectively remote code execution, so this is the highest-risk surface
//! in the project. Everything here exists to make public exposure safe-by-default:
//!
//! - [`AccessToken`] — a ≥256-bit CSPRNG secret (url-safe base64). The long-lived
//!   credential. `/api/*` requires it via `Authorization: Bearer <token>`, compared
//!   in **constant time** ([`ct_eq`], `subtle::ConstantTimeEq`) to deny timing side
//!   channels.
//! - [`TicketStore`] — browsers can't set a `Authorization` header on a WebSocket,
//!   and putting the long-lived token in the ws URL leaks it into logs. So the ws
//!   path uses a two-step exchange: the client posts its Bearer token to
//!   `/auth/ticket`, gets a **single-use, ~30s TTL** ticket, and passes it as
//!   `?ticket=`. The ws handler [`consume`](TicketStore::consume)s it *before*
//!   `.on_upgrade()` — a used or expired ticket is rejected, defeating replay.
//! - [`RateLimiter`] — consecutive auth failures per key lock the key out for a
//!   cooldown (seed: 5 failures → 60s), defeating brute force.
//! - [`AuthMode`] / [`require_auth_or_refuse`] — **default-deny**: public mode with
//!   no token configured refuses to start unless an explicit insecure override is set.
//! - [`parse_gateway_addr`] — loopback hardening: in remote mode the gateway bind
//!   address must be `127.0.0.1`/`::1`. Even if auth had a bug, there is no public
//!   socket to reach (defense in depth; mirrors `lucarned::health::parse_health_addr`).

use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::{Duration, Instant};

use rand::rngs::OsRng;
use rand::RngCore;
use subtle::ConstantTimeEq;
use tokio::sync::Mutex;

/// A single ws ticket is valid for ~30s and may be consumed exactly once.
pub const TICKET_TTL: Duration = Duration::from_secs(30);
/// Consecutive auth failures from one key before it is locked out.
pub const MAX_FAILURES: u32 = 5;
/// How long a key stays locked out after hitting [`MAX_FAILURES`].
pub const LOCKOUT: Duration = Duration::from_secs(60);

/// Hard cap on outstanding (issued, not-yet-consumed/expired) ws tickets the
/// [`TicketStore`] holds at once (M3). A bearer is already authenticated before
/// it can mint a ticket, so this only bounds churn from a misbehaving/compromised
/// authenticated client; over the cap new ticket issuance is refused so existing
/// fresh tickets are never invalidated by a mint flood.
pub const MAX_OUTSTANDING_TICKETS: usize = 1024;

/// Max ws tickets minted per [`TICKET_RATE_WINDOW`] across the store (M3). A
/// run of `issue` calls beyond this within the window is throttled by refusing
/// new tickets. Existing tickets remain valid until consumed or expired.
pub const MAX_TICKETS_PER_WINDOW: u32 = 256;
/// Rolling window for the [`MAX_TICKETS_PER_WINDOW`] issue-rate accounting.
pub const TICKET_RATE_WINDOW: Duration = Duration::from_secs(1);

/// Hard cap on distinct keys the [`RateLimiter`] tracks at once (SEC-009).
///
/// Behind a same-host tunnel the limiter is dominated by the single shared
/// loopback key, so this only bites if the gateway is ever bound to a real
/// interface (where each attacker IP is a fresh key). The cap + opportunistic
/// eviction keep the failure map from growing without bound under that load.
pub const MAX_RATE_LIMIT_ENTRIES: usize = 4096;

/// Number of random bytes behind a token / ticket (256-bit = 32 bytes).
const SECRET_BYTES: usize = 32;

/// Minimum length for an explicitly-configured access token (SEC-008). A
/// generated token is 256-bit (≥43 url-safe base64 chars); we require any
/// operator-supplied token to carry comparable entropy so a weak/typo'd token
/// can't silently become the live credential while auth stays "enforced".
pub const MIN_EXPLICIT_TOKEN_LEN: usize = 32;

/// Fill `out` with CSPRNG bytes from the OS entropy source.
fn fill_random(out: &mut [u8]) {
    OsRng.fill_bytes(out);
}

/// Generate a url-safe base64 secret backed by `SECRET_BYTES` of OS entropy.
fn random_secret() -> String {
    let mut bytes = [0_u8; SECRET_BYTES];
    fill_random(&mut bytes);
    base64url(&bytes)
}

/// Minimal url-safe base64 (no padding) — avoids pulling a base64 crate for a
/// single encode and keeps the secret safe to place in headers and ws query
/// strings.
fn base64url(bytes: &[u8]) -> String {
    const ALPHABET: &[u8; 64] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789-_";
    let mut out = String::with_capacity(bytes.len().div_ceil(3) * 4);
    for chunk in bytes.chunks(3) {
        let b0 = chunk[0] as u32;
        let b1 = *chunk.get(1).unwrap_or(&0) as u32;
        let b2 = *chunk.get(2).unwrap_or(&0) as u32;
        let n = (b0 << 16) | (b1 << 8) | b2;
        out.push(ALPHABET[(n >> 18 & 0x3f) as usize] as char);
        out.push(ALPHABET[(n >> 12 & 0x3f) as usize] as char);
        if chunk.len() > 1 {
            out.push(ALPHABET[(n >> 6 & 0x3f) as usize] as char);
        }
        if chunk.len() > 2 {
            out.push(ALPHABET[(n & 0x3f) as usize] as char);
        }
    }
    out
}

/// Constant-time byte comparison (timing-attack resistant) via `subtle`.
pub fn ct_eq(a: &[u8], b: &[u8]) -> bool {
    a.ct_eq(b).into()
}

/// The long-lived gateway access token (≥256-bit CSPRNG secret).
///
/// Presented over `Authorization: Bearer <token>` on `/api/*`. Comparison is
/// constant-time; never compare with `==`.
#[derive(Clone)]
pub struct AccessToken {
    secret: String,
}

/// Rejection reason for a configured access token (SEC-008).
///
/// An explicit `auth_token` that is whitespace-only or too short to carry
/// meaningful entropy must fail closed rather than silently become the live
/// credential while [`AuthMode::is_enforced`] stays true.
#[derive(Debug, PartialEq, Eq)]
pub enum TokenError {
    /// The configured token is empty or contained only whitespace.
    Empty,
    /// The configured token is shorter than [`MIN_EXPLICIT_TOKEN_LEN`].
    TooShort { len: usize, min: usize },
}

impl std::fmt::Display for TokenError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            TokenError::Empty => write!(
                f,
                "configured access token is empty or whitespace-only (fail-closed)"
            ),
            TokenError::TooShort { len, min } => write!(
                f,
                "configured access token is too weak: {len} chars, need at least {min} (fail-closed)"
            ),
        }
    }
}

impl std::error::Error for TokenError {}

impl AccessToken {
    /// Mint a fresh token from OS entropy.
    pub fn generate() -> Self {
        Self {
            secret: random_secret(),
        }
    }

    /// Reconstruct a token from a persisted/configured secret string.
    ///
    /// Note: this does **not** validate strength — callers handling
    /// operator-supplied tokens must use [`from_secret_validated`](Self::from_secret_validated)
    /// (SEC-008) so a weak/whitespace token fails closed instead of silently
    /// becoming the live credential.
    pub fn from_secret(secret: impl Into<String>) -> Self {
        Self {
            secret: secret.into(),
        }
    }

    /// Reconstruct a token from an operator-supplied secret, rejecting weak
    /// values (SEC-008): a whitespace-only token is [`TokenError::Empty`] and a
    /// token shorter than [`MIN_EXPLICIT_TOKEN_LEN`] is [`TokenError::TooShort`].
    /// The secret itself is taken verbatim (no trimming) so it round-trips with
    /// what the client must present.
    pub fn from_secret_validated(secret: impl Into<String>) -> Result<Self, TokenError> {
        let secret = secret.into();
        if secret.trim().is_empty() {
            return Err(TokenError::Empty);
        }
        if secret.len() < MIN_EXPLICIT_TOKEN_LEN {
            return Err(TokenError::TooShort {
                len: secret.len(),
                min: MIN_EXPLICIT_TOKEN_LEN,
            });
        }
        Ok(Self { secret })
    }

    /// The url-safe secret string (for QR / config persistence / `Bearer`).
    pub fn as_str(&self) -> &str {
        &self.secret
    }

    /// Constant-time check that `candidate` matches this token's secret.
    pub fn verify(&self, candidate: &str) -> bool {
        ct_eq(self.secret.as_bytes(), candidate.as_bytes())
    }
}

/// Single-use, short-TTL WebSocket ticket store.
///
/// `issue()` mints a random ticket recorded with its issue instant; `consume()`
/// validates the ticket exists, is not expired, and removes it (so a second
/// `consume()` of the same ticket fails — replay protection).
///
/// SEC-013: each ticket also carries the access scope it was issued under
/// ([`AccessScope`]). A ticket minted from the read-only credential is recorded
/// `ReadOnly`; the ws handler reads the consumed scope and refuses write frames
/// for a read-only session. Full-access tickets are unchanged.
///
/// M3 hardening: the outstanding set is bounded by [`MAX_OUTSTANDING_TICKETS`]
/// and the mint rate by [`MAX_TICKETS_PER_WINDOW`] per [`TICKET_RATE_WINDOW`].
/// `issue_scoped` is a fast path — it does NOT scan the whole map on every call;
/// expiry is lazy (`consume_scoped` rejects an expired entry) plus an
/// opportunistic front-prune that runs ONLY when the map is near the cap. Over
/// the cap or rate window, issuance returns [`TicketIssueError`] instead of
/// evicting unrelated fresh tickets.
#[derive(Clone)]
pub struct TicketStore {
    ttl: Duration,
    max_outstanding: usize,
    max_per_window: u32,
    rate_window: Duration,
    inner: Arc<Mutex<TicketStoreInner>>,
}

/// The locked interior of a [`TicketStore`].
struct TicketStoreInner {
    /// Live tickets keyed by their secret string.
    entries: HashMap<String, TicketEntry>,
    /// FIFO of `(seq, ticket)` in issue order, used to evict the oldest in O(1)
    /// amortized without scanning the map. Stale front entries (already consumed
    /// or expired) are skipped lazily.
    order: std::collections::VecDeque<(u64, String)>,
    /// Monotonic issue sequence (also stored per entry to detect a re-used key).
    next_seq: u64,
    /// Issue-rate accounting (M3): count within the current rolling window.
    window_start: Instant,
    window_count: u32,
}

/// Ticket issuance refusal reason.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TicketIssueError {
    /// Too many not-yet-consumed/unexpired tickets are already outstanding.
    OutstandingLimit,
    /// Too many tickets were issued in the current rate window.
    RateLimited,
}

impl std::fmt::Display for TicketIssueError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            TicketIssueError::OutstandingLimit => write!(f, "too many outstanding tickets"),
            TicketIssueError::RateLimited => write!(f, "ticket issuance rate limit exceeded"),
        }
    }
}

impl std::error::Error for TicketIssueError {}

/// The access scope a credential (and the ws ticket minted from it) grants.
///
/// SEC-013: read-only sessions mirror terminals (snapshots/deltas) but may not
/// inject input or create/close/drive sessions. `Full` is the existing
/// all-or-nothing behaviour and stays the default for backward compatibility.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum AccessScope {
    /// Full read+write access (input, session lifecycle, agent prompts).
    Full,
    /// Mirror-only: snapshots/deltas allowed, write frames rejected.
    ReadOnly,
}

impl AccessScope {
    /// True for a read-only scope (write frames must be refused).
    pub fn is_readonly(self) -> bool {
        matches!(self, AccessScope::ReadOnly)
    }
}

/// One recorded ticket: its issue instant (for TTL), its access scope, and the
/// issue sequence (so the FIFO can verify a popped entry is still the live one).
#[derive(Clone, Copy)]
struct TicketEntry {
    issued: Instant,
    scope: AccessScope,
    seq: u64,
}

impl Default for TicketStore {
    fn default() -> Self {
        Self::new(TICKET_TTL)
    }
}

impl TicketStore {
    pub fn new(ttl: Duration) -> Self {
        Self::with_limits(
            ttl,
            MAX_OUTSTANDING_TICKETS,
            MAX_TICKETS_PER_WINDOW,
            TICKET_RATE_WINDOW,
        )
    }

    /// Construct a store with explicit M3 limits (used by tests; production goes
    /// through [`new`](Self::new) / [`default`](Self::default)).
    pub fn with_limits(
        ttl: Duration,
        max_outstanding: usize,
        max_per_window: u32,
        rate_window: Duration,
    ) -> Self {
        Self {
            ttl,
            max_outstanding: max_outstanding.max(1),
            max_per_window: max_per_window.max(1),
            rate_window,
            inner: Arc::new(Mutex::new(TicketStoreInner {
                entries: HashMap::new(),
                order: std::collections::VecDeque::new(),
                next_seq: 0,
                window_start: Instant::now(),
                window_count: 0,
            })),
        }
    }

    /// Mint a new single-use full-access ticket valid for the store TTL.
    pub async fn issue(&self) -> Result<String, TicketIssueError> {
        self.issue_scoped(AccessScope::Full).await
    }

    /// Mint a new single-use ticket carrying `scope` (SEC-013). A `ReadOnly`
    /// ticket later consumes to a read-only ws session.
    ///
    /// M3: a fast path — no full-map scan. It updates the issue-rate window and
    /// refuses issuance when at the outstanding cap or over the rate cap. It only
    /// prunes stale/expired FIFO heads, never an unrelated fresh live ticket.
    pub async fn issue_scoped(&self, scope: AccessScope) -> Result<String, TicketIssueError> {
        let now = Instant::now();
        let mut inner = self.inner.lock().await;

        if inner.entries.len() >= self.max_outstanding {
            inner.prune_expired_and_stale_front(self.ttl, now);
        }
        if inner.entries.len() >= self.max_outstanding {
            return Err(TicketIssueError::OutstandingLimit);
        }

        // M3 issue-rate window accounting.
        if now.duration_since(inner.window_start) >= self.rate_window {
            inner.window_start = now;
            inner.window_count = 0;
        }
        if inner.window_count >= self.max_per_window {
            return Err(TicketIssueError::RateLimited);
        }
        inner.window_count = inner.window_count.saturating_add(1);

        let ticket = random_secret();
        let seq = inner.next_seq;
        inner.next_seq = inner.next_seq.wrapping_add(1);
        inner.entries.insert(
            ticket.clone(),
            TicketEntry {
                issued: now,
                scope,
                seq,
            },
        );
        inner.order.push_back((seq, ticket.clone()));
        Ok(ticket)
    }

    /// Validate and consume `ticket`: `true` iff it existed, was not expired, and
    /// was removed by this call. A second consume of the same ticket is false.
    /// (Backward-compatible boolean view of [`consume_scoped`](Self::consume_scoped).)
    pub async fn consume(&self, ticket: &str) -> bool {
        self.consume_scoped(ticket).await.is_some()
    }

    /// Validate and consume `ticket`, returning its [`AccessScope`] on success
    /// (SEC-013) or `None` when the ticket is empty/expired/already-used.
    ///
    /// Expiry is enforced lazily here (the M3 fast-path `issue` avoids a full
    /// sweep), so an expired entry is rejected and removed on consume.
    pub async fn consume_scoped(&self, ticket: &str) -> Option<AccessScope> {
        if ticket.is_empty() {
            return None;
        }
        let now = Instant::now();
        let mut inner = self.inner.lock().await;
        match inner.entries.remove(ticket) {
            Some(entry) if now.duration_since(entry.issued) < self.ttl => Some(entry.scope),
            // Present but expired (or absent) → not consumable. The FIFO entry is
            // left to be skipped lazily when evicted.
            _ => None,
        }
    }

    /// Number of outstanding (issued, not-yet-consumed) tickets (test-only).
    #[cfg(test)]
    async fn outstanding(&self) -> usize {
        self.inner.lock().await.entries.len()
    }
}

impl TicketStoreInner {
    /// Drop consumed/stale FIFO heads and expired oldest entries. This never
    /// removes a fresh live ticket; once the oldest live ticket is still within
    /// TTL, all later live tickets are fresh too.
    fn prune_expired_and_stale_front(&mut self, ttl: Duration, now: Instant) {
        while let Some((seq, ticket)) = self.order.pop_front() {
            match self.entries.get(&ticket) {
                Some(entry) if entry.seq == seq => {
                    let expired = now.duration_since(entry.issued) >= ttl;
                    if expired {
                        self.entries.remove(&ticket);
                    } else {
                        self.order.push_front((seq, ticket));
                        return;
                    }
                }
                // Stale FIFO head (already consumed, or a re-issued key with a
                // newer seq): skip it.
                _ => {}
            }
        }
    }
}

/// Per-key failed-attempt lockout (brute-force defense).
///
/// `record_failure` bumps a per-key counter; once it reaches `max_failures` the
/// key is locked out for `lockout`. `is_locked` reports current lockout state;
/// `record_success` clears the counter. Keyed by client identity (IP) or global.
///
/// SEC-005: behind a same-host tunnel every request's socket peer is the
/// constant loopback `127.0.0.1`, so a blanket per-key hard lock would let 5 bad
/// bearers lock out *all* legitimate users for the whole cooldown (trivial
/// self-DoS). To avoid that, callers pass a `lockable` flag: a **real** client
/// identity (a forwarded `Cf-Connecting-Ip`, or a genuine non-loopback peer) is
/// lockable (per-attacker brute-force protection preserved), while the **shared**
/// loopback key is *not* lockable — failures are still recorded (for an
/// incremental soft delay + audit) but never produce a blanket 429 that denies
/// everyone.
///
/// SEC-009: the failure map is swept opportunistically on every `record_failure`
/// / `is_locked` — entries whose lockout window has fully elapsed are dropped —
/// and a hard `max_entries` cap evicts the oldest unlocked entries so the map
/// cannot grow without bound if the gateway is ever bound to a real interface.
#[derive(Clone)]
pub struct RateLimiter {
    max_failures: u32,
    lockout: Duration,
    max_entries: usize,
    inner: Arc<Mutex<HashMap<String, Attempts>>>,
}

#[derive(Clone, Copy)]
struct Attempts {
    failures: u32,
    locked_until: Option<Instant>,
    /// When this entry was last touched — drives expired-entry sweeping and
    /// oldest-first eviction once the entry cap is hit (SEC-009).
    last_seen: Instant,
}

impl Attempts {
    /// True once an entry carries no useful state: it is not (or no longer)
    /// hard-locked and its idle window has elapsed, so it is safe to drop.
    /// Such an entry is indistinguishable from an absent key, so sweeping it
    /// never changes a future decision.
    fn is_expired(&self, now: Instant, ttl: Duration) -> bool {
        let locked = matches!(self.locked_until, Some(until) if now < until);
        !locked && now.duration_since(self.last_seen) >= ttl
    }
}

impl Default for RateLimiter {
    fn default() -> Self {
        Self::new(MAX_FAILURES, LOCKOUT)
    }
}

impl RateLimiter {
    pub fn new(max_failures: u32, lockout: Duration) -> Self {
        Self::with_capacity(max_failures, lockout, MAX_RATE_LIMIT_ENTRIES)
    }

    /// Construct a limiter with an explicit `max_entries` cap (SEC-009). Used by
    /// tests; production goes through [`new`](Self::new) / [`default`](Self::default).
    pub fn with_capacity(max_failures: u32, lockout: Duration, max_entries: usize) -> Self {
        Self {
            max_failures,
            lockout,
            max_entries: max_entries.max(1),
            inner: Arc::new(Mutex::new(HashMap::new())),
        }
    }

    /// True if `key` is currently hard-locked-out. A non-lockable (shared) key is
    /// never hard-locked (SEC-005) — see [`record_failure`](Self::record_failure).
    pub async fn is_locked(&self, key: &str) -> bool {
        let now = Instant::now();
        let mut map = self.inner.lock().await;
        // SEC-009: opportunistic sweep of fully-elapsed entries.
        Self::sweep_expired(&mut map, now, self.lockout);
        match map.get_mut(key) {
            Some(a) => match a.locked_until {
                Some(until) if now < until => true,
                Some(_) => {
                    // Lockout elapsed — reset the window.
                    a.failures = 0;
                    a.locked_until = None;
                    a.last_seen = now;
                    false
                }
                None => false,
            },
            None => false,
        }
    }

    /// Record a failed auth attempt for `key`. When `lockable` is true (a real
    /// client identity) a run of `max_failures` arms a hard lockout — returns
    /// true on the transition. When `lockable` is false (the shared loopback key
    /// behind a tunnel) the failure is counted but **never** arms a hard lock, so
    /// one attacker cannot deny everyone (SEC-005); the count still drives the
    /// incremental [`soft_delay`](Self::soft_delay).
    pub async fn record_failure(&self, key: &str, lockable: bool) -> bool {
        let now = Instant::now();
        let mut map = self.inner.lock().await;
        // SEC-009: sweep elapsed entries, then enforce the entry cap before
        // inserting a brand-new key so the map cannot grow without bound.
        Self::sweep_expired(&mut map, now, self.lockout);
        if !map.contains_key(key) {
            let has_room = Self::evict_to_capacity(&mut map, self.max_entries, now);
            // M4: absolute hard cap. If the map is full and every entry is a live
            // hard-lock (nothing evictable), refuse to track a brand-new key
            // rather than inserting past `max_entries`. The active hard-locks are
            // load-bearing and preserved; the new failing key simply isn't
            // recorded this round (it still gets a 401 from the caller, and the
            // attacker cannot inflate the map without bound — SEC-009/M4).
            if !has_room {
                tracing::warn!(
                    cap = self.max_entries,
                    "termgw: rate-limiter at hard cap with all entries locked; \
                     refusing to track a new key (M4)"
                );
                return false;
            }
        }
        let a = map.entry(key.to_string()).or_insert(Attempts {
            failures: 0,
            locked_until: None,
            last_seen: now,
        });
        a.last_seen = now;
        // If a prior lockout has elapsed, start a fresh window.
        if matches!(a.locked_until, Some(until) if now >= until) {
            a.failures = 0;
            a.locked_until = None;
        }
        a.failures = a.failures.saturating_add(1);
        if lockable && a.failures >= self.max_failures {
            a.locked_until = Some(now + self.lockout);
            true
        } else {
            false
        }
    }

    /// Incremental soft delay for `key` based on its current failure count
    /// (SEC-005). Used for the shared loopback key — where a hard lock is unsafe —
    /// to still slow a sustained bearer-guessing flood without denying anyone.
    /// Capped so a legitimate user retrying after a few failures is not stalled
    /// for long.
    pub async fn soft_delay(&self, key: &str) -> Duration {
        const PER_FAILURE_MS: u64 = 200;
        const MAX_MS: u64 = 2_000;
        let map = self.inner.lock().await;
        let failures = map.get(key).map(|a| a.failures).unwrap_or(0);
        let ms = (failures as u64).saturating_mul(PER_FAILURE_MS).min(MAX_MS);
        Duration::from_millis(ms)
    }

    /// Clear the failure window for `key` after a successful auth.
    pub async fn record_success(&self, key: &str) {
        let mut map = self.inner.lock().await;
        map.remove(key);
    }

    /// Number of distinct keys currently tracked (test-only introspection for
    /// the SEC-009 sweep/eviction behaviour).
    #[cfg(test)]
    async fn entry_count(&self) -> usize {
        self.inner.lock().await.len()
    }

    /// Whether a `key` is currently present in the failure map (test-only).
    #[cfg(test)]
    async fn has_entry(&self, key: &str) -> bool {
        self.inner.lock().await.contains_key(key)
    }

    /// Drop every entry whose lockout window has fully elapsed (SEC-009). An
    /// expired entry is indistinguishable from an absent key, so removing it
    /// never changes a future decision.
    fn sweep_expired(map: &mut HashMap<String, Attempts>, now: Instant, ttl: Duration) {
        map.retain(|_, a| !a.is_expired(now, ttl));
    }

    /// Enforce the `max_entries` cap (SEC-009 / M4): while at/over capacity,
    /// evict the oldest entry that is not currently hard-locked (a live lockout
    /// is load-bearing and must be preserved). Sweeping runs first, so this only
    /// fires under genuine pressure from many distinct live keys.
    ///
    /// Returns `true` when there is room for a new key afterwards (`len <
    /// max_entries`), `false` when the map is full and every remaining entry is a
    /// live hard-lock so nothing can be evicted — the M4 absolute cap: the caller
    /// must then refuse to insert a new key rather than exceed `max_entries`.
    fn evict_to_capacity(
        map: &mut HashMap<String, Attempts>,
        max_entries: usize,
        now: Instant,
    ) -> bool {
        while map.len() >= max_entries {
            let victim = map
                .iter()
                .filter(|(_, a)| !matches!(a.locked_until, Some(until) if now < until))
                .min_by_key(|(_, a)| a.last_seen)
                .map(|(k, _)| k.clone());
            match victim {
                Some(key) => {
                    map.remove(&key);
                }
                // Everything left is hard-locked — never evict a live lockout.
                // M4: no room; the caller must not insert past the cap.
                None => return false,
            }
        }
        true
    }
}

/// Whether the gateway enforces auth.
///
/// `Disabled` is local-dev only (loopback); `Token` carries the configured
/// long-lived credential for public exposure.
#[derive(Clone)]
pub enum AuthMode {
    /// No auth — local development on loopback only.
    Disabled,
    /// Auth enforced with this access token.
    Token(AccessToken),
}

impl AuthMode {
    /// True when auth is enforced (a token is configured).
    pub fn is_enforced(&self) -> bool {
        matches!(self, AuthMode::Token(_))
    }

    /// The configured token, if any.
    pub fn token(&self) -> Option<&AccessToken> {
        match self {
            AuthMode::Token(t) => Some(t),
            AuthMode::Disabled => None,
        }
    }
}

/// Trusted forwarded-identity policy (SEC-005 / H6b).
///
/// Behind a same-host tunnel every socket peer is the constant loopback address,
/// so the gateway resolves a client's real identity from a forwarded header. But
/// a forwarded header is attacker-controllable unless the request actually
/// arrived from the trusted local tunnel source, so it is trusted **only** when
/// the socket peer is loopback.
///
/// The set of header names that carry the forwarded client IP is provider-
/// specific (cloudflared uses `cf-connecting-ip`); the gateway must not hardcode
/// it (AGENTS.md provider boundary). Instead the daemon injects the trusted
/// header list for whichever provider it started (e.g. `["cf-connecting-ip"]`
/// for cloudflared). An empty list (the default) means **never** trust a
/// forwarded header — identity is the socket peer only.
#[derive(Clone, Debug, Default)]
pub struct ForwardedIdentityPolicy {
    /// Header names (lowercase) trusted to carry the client IP, consulted in
    /// order. Empty → only the socket peer is used.
    trusted_headers: Vec<String>,
}

impl ForwardedIdentityPolicy {
    /// A policy that never trusts a forwarded header (socket peer only). This is
    /// the safe default for a directly-bound gateway with no tunnel in front.
    pub fn socket_peer_only() -> Self {
        Self {
            trusted_headers: Vec::new(),
        }
    }

    /// A policy trusting `headers` (case-insensitive) to carry the forwarded
    /// client IP — but only when the socket peer is loopback (the trusted tunnel
    /// source). The daemon builds this from the started provider's contract
    /// (e.g. `["cf-connecting-ip"]` for cloudflared).
    pub fn trusting<I, S>(headers: I) -> Self
    where
        I: IntoIterator<Item = S>,
        S: AsRef<str>,
    {
        Self {
            trusted_headers: headers
                .into_iter()
                .map(|h| h.as_ref().to_ascii_lowercase())
                .filter(|h| !h.is_empty())
                .collect(),
        }
    }

    /// The trusted forwarded header names (lowercase), in lookup order.
    pub fn trusted_headers(&self) -> &[String] {
        &self.trusted_headers
    }

    /// Resolve the forwarded client IP from `lookup` (a `header name → value`
    /// resolver) using the first trusted header that yields a value. Returns
    /// `None` when no trusted header is configured or none is present.
    pub fn forwarded_ip<'a>(&self, lookup: impl Fn(&str) -> Option<&'a str>) -> Option<&'a str> {
        self.trusted_headers
            .iter()
            .find_map(|name| lookup(name.as_str()))
    }
}

/// Shared, cheaply-clonable auth state threaded into the gateway router.
#[derive(Clone)]
pub struct AuthState {
    pub mode: AuthMode,
    pub tickets: TicketStore,
    pub limiter: RateLimiter,
    /// Optional read-only credential (SEC-013). When set, a bearer matching this
    /// token authenticates with [`AccessScope::ReadOnly`]: it may mirror sessions
    /// but its ws tickets are minted read-only so write frames are refused.
    /// `None` → no read-only tier (unchanged all-or-nothing behaviour).
    pub readonly_token: Option<AccessToken>,
    /// Trusted forwarded-identity policy (H6b): which header(s) the gateway may
    /// believe for the real client IP, and only behind a loopback tunnel source.
    /// Default → socket-peer-only (never trust a forwarded header).
    pub forwarded_identity: ForwardedIdentityPolicy,
}

impl AuthState {
    /// Auth-disabled state for local dev (loopback).
    pub fn disabled() -> Self {
        Self {
            mode: AuthMode::Disabled,
            tickets: TicketStore::default(),
            limiter: RateLimiter::default(),
            readonly_token: None,
            forwarded_identity: ForwardedIdentityPolicy::socket_peer_only(),
        }
    }

    /// Auth-enforced state with a configured token.
    pub fn with_token(token: AccessToken) -> Self {
        Self {
            mode: AuthMode::Token(token),
            tickets: TicketStore::default(),
            limiter: RateLimiter::default(),
            readonly_token: None,
            forwarded_identity: ForwardedIdentityPolicy::socket_peer_only(),
        }
    }

    /// Auth-enforced state with a full-access token plus a read-only token
    /// (SEC-013). A bearer matching `readonly` authenticates with
    /// [`AccessScope::ReadOnly`]; a bearer matching `token` keeps full access.
    pub fn with_tokens(token: AccessToken, readonly: AccessToken) -> Self {
        Self {
            mode: AuthMode::Token(token),
            tickets: TicketStore::default(),
            limiter: RateLimiter::default(),
            readonly_token: Some(readonly),
            forwarded_identity: ForwardedIdentityPolicy::socket_peer_only(),
        }
    }

    /// Set the trusted forwarded-identity policy (H6b). The daemon calls this
    /// with the started provider's trusted header list (e.g. `cf-connecting-ip`
    /// for cloudflared); the gateway crate never hardcodes a provider header.
    pub fn with_forwarded_identity(mut self, policy: ForwardedIdentityPolicy) -> Self {
        self.forwarded_identity = policy;
        self
    }

    /// Extract the `Bearer` credential from an `Authorization` header value.
    pub fn bearer(authorization: Option<&str>) -> Option<&str> {
        authorization?.strip_prefix("Bearer ").map(str::trim)
    }

    /// Constant-time check that `candidate` matches the configured token. When
    /// auth is disabled this is always true (local dev).
    pub fn verify_token(&self, candidate: &str) -> bool {
        self.scope_for(candidate).is_some()
    }

    /// Resolve the [`AccessScope`] a presented bearer authenticates with, or
    /// `None` when it matches no configured credential (SEC-013).
    ///
    /// - auth disabled → always full (local dev).
    /// - matches the full token → [`AccessScope::Full`].
    /// - matches the read-only token (if configured) → [`AccessScope::ReadOnly`].
    ///
    /// The full token is checked first so it always wins if both happened to be
    /// equal; both comparisons are constant-time.
    pub fn scope_for(&self, candidate: &str) -> Option<AccessScope> {
        match &self.mode {
            AuthMode::Disabled => Some(AccessScope::Full),
            AuthMode::Token(t) => {
                if t.verify(candidate) {
                    Some(AccessScope::Full)
                } else if self
                    .readonly_token
                    .as_ref()
                    .is_some_and(|ro| ro.verify(candidate))
                {
                    Some(AccessScope::ReadOnly)
                } else {
                    None
                }
            }
        }
    }

    /// Resolve the rate-limiter key + lockability for a request (SEC-005).
    ///
    /// Behind a same-host tunnel (cloudflared) the socket `peer` is always the
    /// constant loopback address, so keying the limiter on it would let a handful
    /// of bad bearers hard-lock *every* user. We therefore prefer a forwarded
    /// client identity, but **only trust it when the socket peer is loopback**
    /// (i.e. the request actually arrived via the trusted local tunnel source —
    /// a direct remote peer could otherwise spoof the header):
    ///
    /// - loopback peer + a parseable, non-loopback `forwarded_ip`
    ///   → key = that real IP, `lockable = true` (per-attacker brute-force lock).
    /// - loopback peer, no usable forwarded IP
    ///   → the **shared** loopback key, `lockable = false` (never hard-lock; only
    ///   an incremental soft delay so one attacker can't deny everyone).
    /// - non-loopback peer (a genuine direct client)
    ///   → key = peer IP, `lockable = true`.
    pub fn limiter_key(peer: SocketAddr, forwarded_ip: Option<&str>) -> (String, bool) {
        if peer.ip().is_loopback() {
            if let Some(real) = forwarded_ip
                .map(str::trim)
                .filter(|s| !s.is_empty())
                .and_then(|s| s.parse::<std::net::IpAddr>().ok())
                .filter(|ip| !ip.is_loopback())
            {
                return (real.to_string(), true);
            }
            return (SHARED_LOOPBACK_KEY.to_string(), false);
        }
        (peer.ip().to_string(), true)
    }
}

/// Limiter key for requests that reach the gateway over the loopback tunnel with
/// no trustworthy forwarded client IP. Deliberately *not* an IP so it never
/// hard-locks (SEC-005) — see [`AuthState::limiter_key`].
pub const SHARED_LOOPBACK_KEY: &str = "shared-loopback";

/// Refusal reasons surfaced by [`require_auth_or_refuse`].
#[derive(Debug)]
pub enum AuthRefusal {
    /// Remote/public mode requested but no token configured and no insecure override.
    PublicWithoutToken,
}

impl std::fmt::Display for AuthRefusal {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            AuthRefusal::PublicWithoutToken => write!(
                f,
                "refusing to expose the terminal gateway publicly without an access token \
                 (default-deny); configure a token or pass an explicit insecure override"
            ),
        }
    }
}

impl std::error::Error for AuthRefusal {}

/// Default-deny gate for public exposure.
///
/// When `remote` (public) mode is on, the gateway must have a token configured.
/// With no token and no explicit `insecure` override this returns
/// [`AuthRefusal::PublicWithoutToken`] so the daemon refuses to start publicly.
/// `insecure` is the explicit opt-out (caller is expected to log a loud warning).
pub fn require_auth_or_refuse(
    remote: bool,
    mode: &AuthMode,
    insecure: bool,
) -> Result<(), AuthRefusal> {
    if remote && !mode.is_enforced() && !insecure {
        return Err(AuthRefusal::PublicWithoutToken);
    }
    Ok(())
}

/// Address parsing errors for the gateway bind address.
#[derive(Debug)]
pub enum GatewayAddrError {
    /// Not a valid `SocketAddr`.
    Invalid(String),
    /// Valid address but not a loopback IP (rejected in remote mode).
    NonLoopback(SocketAddr),
}

impl std::fmt::Display for GatewayAddrError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            GatewayAddrError::Invalid(v) => write!(f, "invalid gateway address: {v}"),
            GatewayAddrError::NonLoopback(addr) => write!(
                f,
                "gateway address must be loopback-only when exposed via tunnel: {addr}"
            ),
        }
    }
}

impl std::error::Error for GatewayAddrError {}

/// Parse + harden the gateway bind address.
///
/// Mirrors `lucarned::health::parse_health_addr`: the gateway must bind a
/// loopback address so it never listens on a public interface (`0.0.0.0`). The
/// tunnel connects *outbound* to the edge and back to this loopback socket, so
/// the only public entry point is the authenticated tunnel ingress.
pub fn parse_gateway_addr(raw: &str) -> Result<SocketAddr, GatewayAddrError> {
    let addr = raw
        .parse::<SocketAddr>()
        .map_err(|_| GatewayAddrError::Invalid(raw.to_string()))?;
    if !addr.ip().is_loopback() {
        return Err(GatewayAddrError::NonLoopback(addr));
    }
    Ok(addr)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn access_token_is_at_least_256_bit_and_constant_time() {
        let a = AccessToken::generate();
        let b = AccessToken::generate();
        // url-safe base64 of 32 bytes is well over 32 chars; two tokens differ.
        assert!(a.as_str().len() >= 32, "token too short: {}", a.as_str());
        assert_ne!(a.as_str(), b.as_str(), "tokens must be unique");
        // verify() round-trips and rejects others via constant-time compare.
        assert!(a.verify(a.as_str()));
        assert!(!a.verify(b.as_str()));
        assert!(!a.verify("wrong"));
    }

    #[test]
    fn ct_eq_matches_only_equal_bytes() {
        assert!(ct_eq(b"hunter2", b"hunter2"));
        assert!(!ct_eq(b"hunter2", b"hunter3"));
        assert!(!ct_eq(b"short", b"longerstring"));
    }

    #[tokio::test]
    async fn ticket_is_single_use() {
        let store = TicketStore::default();
        let ticket = store.issue().await.expect("issue ticket");
        assert!(store.consume(&ticket).await, "first consume must succeed");
        assert!(
            !store.consume(&ticket).await,
            "replay: second consume must fail"
        );
        assert!(!store.consume("never-issued").await);
        assert!(!store.consume("").await);
    }

    #[tokio::test]
    async fn ticket_expires_after_ttl() {
        // Tiny TTL so the test is fast and deterministic.
        let store = TicketStore::new(Duration::from_millis(20));
        let ticket = store.issue().await.expect("issue ticket");
        tokio::time::sleep(Duration::from_millis(40)).await;
        assert!(
            !store.consume(&ticket).await,
            "expired ticket must not consume"
        );
    }

    // SEC-013: a ticket carries the scope it was minted under, surfaced on
    // consume; a default `issue()` ticket is Full (backward-compatible).
    #[tokio::test]
    async fn ticket_carries_scope_through_consume() {
        let store = TicketStore::default();
        let full = store.issue().await.expect("issue ticket"); // == issue_scoped(Full)
        assert_eq!(store.consume_scoped(&full).await, Some(AccessScope::Full));

        let ro = store
            .issue_scoped(AccessScope::ReadOnly)
            .await
            .expect("issue read-only ticket");
        assert_eq!(store.consume_scoped(&ro).await, Some(AccessScope::ReadOnly));
        // Single-use still holds for scoped tickets.
        assert_eq!(store.consume_scoped(&ro).await, None);
        // Empty / never-issued → None.
        assert_eq!(store.consume_scoped("").await, None);
        assert_eq!(store.consume_scoped("never-issued").await, None);
    }

    // M3: the outstanding ticket set is hard-capped. Minting beyond the cap is
    // refused and never evicts another client's still-fresh ticket.
    #[tokio::test]
    async fn ticket_store_caps_outstanding_without_evicting_fresh_tickets() {
        // Long TTL (nothing self-expires), small cap, generous rate so the rate
        // path doesn't interfere with the pure-cap assertion.
        let store =
            TicketStore::with_limits(Duration::from_secs(3600), 3, 1000, Duration::from_secs(1));
        let t1 = store.issue().await.expect("issue ticket");
        let t2 = store.issue().await.expect("issue ticket");
        let t3 = store.issue().await.expect("issue ticket");
        assert_eq!(store.outstanding().await, 3, "at cap");
        // A 4th mint is refused instead of evicting the oldest fresh ticket.
        assert_eq!(
            store.issue().await.expect_err("cap must reject new ticket"),
            TicketIssueError::OutstandingLimit
        );
        assert_eq!(store.outstanding().await, 3, "still at cap after refusal");
        // All previously issued fresh tickets survive the mint flood.
        assert!(store.consume(&t1).await);
        assert!(store.consume(&t2).await);
        assert!(store.consume(&t3).await);
    }

    // M3: the issue-rate cap rejects a mint flood without evicting previously
    // issued fresh tickets.
    #[tokio::test]
    async fn ticket_store_rate_cap_refuses_flood_without_evicting_fresh_tickets() {
        let cap = 8;
        let store = TicketStore::with_limits(
            Duration::from_secs(3600),
            cap,
            4,                         // 4 mints per window
            Duration::from_secs(3600), // window won't roll during the test
        );
        let mut issued = Vec::new();
        for _ in 0..4 {
            issued.push(store.issue().await.expect("issue ticket"));
        }
        assert_eq!(
            store
                .issue()
                .await
                .expect_err("rate cap must reject new ticket"),
            TicketIssueError::RateLimited
        );
        assert_eq!(store.outstanding().await, 4);
        for ticket in issued {
            assert!(
                store.consume(&ticket).await,
                "fresh ticket must survive rate refusal"
            );
        }
    }

    // M3: expiry is enforced lazily on consume even though issue does not sweep.
    #[tokio::test]
    async fn ticket_expiry_is_lazy_on_consume() {
        let store = TicketStore::with_limits(
            Duration::from_millis(20),
            1024,
            1000,
            Duration::from_secs(1),
        );
        let ticket = store.issue().await.expect("issue ticket");
        tokio::time::sleep(Duration::from_millis(40)).await;
        assert!(
            !store.consume(&ticket).await,
            "expired ticket must not consume"
        );
    }

    // SEC-013: the readonly token authenticates as ReadOnly; the full token as
    // Full; an unrelated token as nothing. Both comparisons are constant-time.
    #[test]
    fn readonly_token_resolves_distinct_scope() {
        let full = AccessToken::generate();
        let readonly = AccessToken::generate();
        let auth = AuthState::with_tokens(full.clone(), readonly.clone());

        assert_eq!(auth.scope_for(full.as_str()), Some(AccessScope::Full));
        assert_eq!(
            auth.scope_for(readonly.as_str()),
            Some(AccessScope::ReadOnly)
        );
        assert_eq!(auth.scope_for("unrelated"), None);

        // Backward compatibility: no readonly token configured → only the full
        // token authenticates, always as Full.
        let only_full = AuthState::with_token(full.clone());
        assert_eq!(only_full.scope_for(full.as_str()), Some(AccessScope::Full));
        assert_eq!(only_full.scope_for(readonly.as_str()), None);
        assert!(only_full.readonly_token.is_none());

        // Auth disabled (local dev) → always Full.
        let disabled = AuthState::disabled();
        assert_eq!(disabled.scope_for("anything"), Some(AccessScope::Full));
    }

    #[tokio::test]
    async fn rate_limiter_locks_out_after_max_failures() {
        let limiter = RateLimiter::new(MAX_FAILURES, LOCKOUT);
        let key = "203.0.113.7";
        assert!(!limiter.is_locked(key).await);
        let mut locked = false;
        for _ in 0..MAX_FAILURES {
            locked = limiter.record_failure(key, true).await;
        }
        assert!(locked, "reaching MAX_FAILURES must lock out");
        assert!(limiter.is_locked(key).await, "key must be locked");
        // A success clears the window.
        limiter.record_success(key).await;
        assert!(!limiter.is_locked(key).await);
    }

    #[tokio::test]
    async fn rate_limiter_lockout_expires() {
        let limiter = RateLimiter::new(2, Duration::from_millis(20));
        let key = "lock";
        assert!(limiter.record_failure(key, true).await || true);
        assert!(
            limiter.record_failure(key, true).await,
            "second failure locks"
        );
        assert!(limiter.is_locked(key).await);
        tokio::time::sleep(Duration::from_millis(40)).await;
        assert!(!limiter.is_locked(key).await, "lockout must elapse");
    }

    // SEC-009: an entry whose lockout window has fully elapsed is swept on the
    // next limiter touch, so the failure map cannot accumulate dead entries.
    #[tokio::test]
    async fn rate_limiter_sweeps_expired_entries() {
        let limiter = RateLimiter::with_capacity(2, Duration::from_millis(20), 64);
        // Arm a lockout for an attacker key, then let the window elapse.
        limiter.record_failure("203.0.113.1", true).await;
        assert!(limiter.record_failure("203.0.113.1", true).await);
        assert_eq!(limiter.entry_count().await, 1);
        tokio::time::sleep(Duration::from_millis(40)).await;
        // Touching the limiter with an unrelated failure sweeps the now-expired
        // entry along with itself if it too is past its window.
        limiter.record_failure("203.0.113.2", true).await;
        // The expired attacker entry is gone; only the fresh one remains.
        assert!(!limiter.has_entry("203.0.113.1").await);
        assert!(limiter.has_entry("203.0.113.2").await);
    }

    // SEC-009: over the entry cap, the oldest unlocked entry is evicted so the
    // map size stays bounded, but a live hard-lock is never evicted.
    #[tokio::test]
    async fn rate_limiter_evicts_oldest_when_over_capacity() {
        // Cap of 3 distinct keys; a long lockout so nothing self-expires mid-test.
        let limiter = RateLimiter::with_capacity(100, Duration::from_secs(3600), 3);
        // Three unlocked keys (one failure each, below max_failures so no lock).
        limiter.record_failure("a", true).await;
        tokio::time::sleep(Duration::from_millis(2)).await;
        limiter.record_failure("b", true).await;
        tokio::time::sleep(Duration::from_millis(2)).await;
        limiter.record_failure("c", true).await;
        assert_eq!(limiter.entry_count().await, 3);
        // A fourth distinct key must evict the oldest ("a") to stay at the cap.
        limiter.record_failure("d", true).await;
        assert_eq!(limiter.entry_count().await, 3);
        assert!(
            !limiter.has_entry("a").await,
            "oldest entry must be evicted"
        );
        assert!(limiter.has_entry("d").await, "newest entry must be present");
    }

    // SEC-009: a live hard-lock is load-bearing and must survive eviction
    // pressure even when it is the oldest entry.
    #[tokio::test]
    async fn rate_limiter_never_evicts_a_live_hard_lock() {
        let limiter = RateLimiter::with_capacity(2, Duration::from_secs(3600), 2);
        // "locked" hits max_failures → hard-locked (and is the oldest entry).
        limiter.record_failure("locked", true).await;
        assert!(limiter.record_failure("locked", true).await);
        assert!(limiter.is_locked("locked").await);
        // Insert more distinct keys; the locked entry must be preserved.
        limiter.record_failure("x", true).await;
        limiter.record_failure("y", true).await;
        assert!(
            limiter.is_locked("locked").await,
            "a live lockout must never be evicted"
        );
    }

    // M4: absolute hard cap. When the map is full and EVERY entry is a live
    // hard-lock (nothing evictable), a brand-new failing key is NOT inserted —
    // the cap is never exceeded, and the existing hard-locks are preserved.
    #[tokio::test]
    async fn rate_limiter_never_exceeds_cap_when_all_locked() {
        // Cap of 2, lock after 1 failure, long lockout so both stay locked.
        let limiter = RateLimiter::with_capacity(1, Duration::from_secs(3600), 2);
        // Two distinct keys each hard-lock immediately → map full of live locks.
        assert!(limiter.record_failure("a", true).await, "a locks");
        assert!(limiter.record_failure("b", true).await, "b locks");
        assert!(limiter.is_locked("a").await);
        assert!(limiter.is_locked("b").await);
        assert_eq!(limiter.entry_count().await, 2, "at cap, all locked");

        // A third distinct key cannot be tracked (no victim) → not locked, and
        // the map does NOT grow past the cap.
        let locked_new = limiter.record_failure("c", true).await;
        assert!(
            !locked_new,
            "a new key under a full all-locked cap is not locked"
        );
        assert_eq!(
            limiter.entry_count().await,
            2,
            "hard cap: the map must not exceed max_entries even under flood"
        );
        assert!(
            !limiter.has_entry("c").await,
            "new key was refused tracking (M4)"
        );
        // The existing live hard-locks are untouched.
        assert!(limiter.is_locked("a").await);
        assert!(limiter.is_locked("b").await);
    }

    // SEC-005: the shared loopback key (no real client identity behind the
    // tunnel) must NEVER hard-lock, so one attacker can't deny everyone. It only
    // accrues an incremental soft delay.
    #[tokio::test]
    async fn rate_limiter_shared_key_never_hard_locks_and_soft_delays() {
        let limiter = RateLimiter::new(MAX_FAILURES, LOCKOUT);
        let key = SHARED_LOOPBACK_KEY;
        let mut ever_locked = false;
        for _ in 0..(MAX_FAILURES * 4) {
            ever_locked |= limiter.record_failure(key, false).await;
        }
        assert!(!ever_locked, "shared key must never arm a hard lock");
        assert!(
            !limiter.is_locked(key).await,
            "shared key must never report locked"
        );
        // But it does accrue a (capped) incremental soft delay.
        assert!(
            limiter.soft_delay(key).await > Duration::ZERO,
            "repeated failures must produce a soft delay"
        );
        assert!(
            limiter.soft_delay(key).await <= Duration::from_millis(2_000),
            "soft delay must stay capped"
        );
    }

    // SEC-005: a real forwarded client IP (trusted only behind a loopback peer)
    // is lockable; the shared key is not; a direct non-loopback peer is keyed by
    // its own IP.
    #[test]
    fn limiter_key_prefers_trusted_forwarded_ip() {
        let loopback: SocketAddr = "127.0.0.1:55000".parse().unwrap();
        let direct: SocketAddr = "203.0.113.9:55000".parse().unwrap();

        // Loopback peer + real forwarded IP → key = that IP, lockable.
        let (key, lockable) = AuthState::limiter_key(loopback, Some("198.51.100.7"));
        assert_eq!(key, "198.51.100.7");
        assert!(lockable);

        // Loopback peer + no forwarded IP → shared key, NOT lockable.
        let (key, lockable) = AuthState::limiter_key(loopback, None);
        assert_eq!(key, SHARED_LOOPBACK_KEY);
        assert!(!lockable);

        // Loopback peer + a forwarded *loopback* IP is not trusted as a real
        // client → shared key, NOT lockable.
        let (key, lockable) = AuthState::limiter_key(loopback, Some("127.0.0.1"));
        assert_eq!(key, SHARED_LOOPBACK_KEY);
        assert!(!lockable);

        // Garbage forwarded value → shared key, NOT lockable.
        let (key, lockable) = AuthState::limiter_key(loopback, Some("not-an-ip"));
        assert_eq!(key, SHARED_LOOPBACK_KEY);
        assert!(!lockable);

        // Direct non-loopback peer → keyed by its own IP, lockable; a spoofed
        // forwarded header is ignored (peer is not the trusted tunnel source).
        let (key, lockable) = AuthState::limiter_key(direct, Some("10.0.0.1"));
        assert_eq!(key, "203.0.113.9");
        assert!(lockable);
    }

    // SEC-008: explicit tokens that are empty/whitespace or too short fail closed.
    #[test]
    fn explicit_token_validation_rejects_weak_tokens() {
        assert_eq!(
            AccessToken::from_secret_validated("").err(),
            Some(TokenError::Empty)
        );
        assert_eq!(
            AccessToken::from_secret_validated("   \t ").err(),
            Some(TokenError::Empty)
        );
        // Shorter than MIN_EXPLICIT_TOKEN_LEN → TooShort.
        let short = "abc123";
        assert!(matches!(
            AccessToken::from_secret_validated(short),
            Err(TokenError::TooShort { .. })
        ));
        // A long-enough token is accepted verbatim (no trimming of the secret).
        let strong = "0123456789abcdef0123456789abcdef"; // 32 chars
        let token = AccessToken::from_secret_validated(strong).expect("strong token accepted");
        assert_eq!(token.as_str(), strong);
        assert!(token.verify(strong));
        // A generated token always passes validation.
        let generated = AccessToken::generate();
        assert!(AccessToken::from_secret_validated(generated.as_str().to_string()).is_ok());
    }

    #[test]
    fn gateway_addr_must_be_loopback() {
        assert!(parse_gateway_addr("127.0.0.1:7800").is_ok());
        assert!(parse_gateway_addr("[::1]:7800").is_ok());
        assert!(matches!(
            parse_gateway_addr("0.0.0.0:7800"),
            Err(GatewayAddrError::NonLoopback(_))
        ));
        assert!(matches!(
            parse_gateway_addr("203.0.113.5:7800"),
            Err(GatewayAddrError::NonLoopback(_))
        ));
        assert!(matches!(
            parse_gateway_addr("not-an-addr"),
            Err(GatewayAddrError::Invalid(_))
        ));
    }

    #[test]
    fn default_deny_refuses_public_without_token() {
        // Public mode, no token, no override → refused.
        assert!(matches!(
            require_auth_or_refuse(true, &AuthMode::Disabled, false),
            Err(AuthRefusal::PublicWithoutToken)
        ));
        // Public mode, token configured → allowed.
        assert!(
            require_auth_or_refuse(true, &AuthMode::Token(AccessToken::generate()), false).is_ok()
        );
        // Public mode, no token but explicit insecure override → allowed (loud warn elsewhere).
        assert!(require_auth_or_refuse(true, &AuthMode::Disabled, true).is_ok());
        // Local (non-remote) mode never refuses.
        assert!(require_auth_or_refuse(false, &AuthMode::Disabled, false).is_ok());
    }

    #[test]
    fn bearer_extraction() {
        assert_eq!(AuthState::bearer(Some("Bearer abc123")), Some("abc123"));
        assert_eq!(AuthState::bearer(Some("bearer abc123")), None);
        assert_eq!(AuthState::bearer(Some("abc123")), None);
        assert_eq!(AuthState::bearer(None), None);
    }

    // H6b: the gateway never hardcodes a provider's forwarded header; the policy
    // is injected by the daemon. The default trusts nothing (socket-peer only),
    // a `trusting` policy consults its headers case-insensitively in order, and
    // an `AuthState` carries the policy so `bearer_guard` can use it.
    #[test]
    fn forwarded_identity_policy_resolution() {
        // Default: never trust a forwarded header (socket peer only).
        let none = ForwardedIdentityPolicy::socket_peer_only();
        assert!(none.trusted_headers().is_empty());
        assert_eq!(none.forwarded_ip(|_| Some("198.51.100.7")), None);

        // A trusting policy normalizes header names to lowercase and consults
        // them in order, returning the first present value.
        let policy = ForwardedIdentityPolicy::trusting(["CF-Connecting-IP"]);
        assert_eq!(policy.trusted_headers(), &["cf-connecting-ip".to_string()]);
        // Lookup keyed by the normalized name yields the value.
        let headers = std::collections::HashMap::from([("cf-connecting-ip", "198.51.100.7")]);
        assert_eq!(
            policy.forwarded_ip(|name| headers.get(name).copied()),
            Some("198.51.100.7")
        );
        // A header that isn't trusted is ignored.
        let other = std::collections::HashMap::from([("x-real-ip", "10.0.0.1")]);
        assert_eq!(policy.forwarded_ip(|name| other.get(name).copied()), None);

        // Empty header names are filtered out at construction.
        let filtered = ForwardedIdentityPolicy::trusting(["", "X-Forwarded-For"]);
        assert_eq!(filtered.trusted_headers(), &["x-forwarded-for".to_string()]);

        // The default AuthState carries a socket-peer-only policy; `with_*`
        // builders can attach a trusting one.
        let auth = AuthState::with_token(AccessToken::generate());
        assert!(auth.forwarded_identity.trusted_headers().is_empty());
        let trusting = AuthState::with_token(AccessToken::generate())
            .with_forwarded_identity(ForwardedIdentityPolicy::trusting(["cf-connecting-ip"]));
        assert_eq!(
            trusting.forwarded_identity.trusted_headers(),
            &["cf-connecting-ip".to_string()]
        );
    }
}
