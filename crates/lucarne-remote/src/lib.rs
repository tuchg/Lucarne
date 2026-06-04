//! Pluggable remote-access tunnel adapter base for Lucarne.
//!
//! This crate defines a transport-agnostic provider abstraction that lets the
//! Lucarne daemon expose a loopback-bound gateway to the public internet via
//! pluggable tunnel backends. It mirrors the proven
//! [`AdapterPlugin`]/[`AdapterRegistry`] pattern from `lucarne-adapter`
//! (`lucarne-adapter/src/lib.rs:575-603`): a `Send + Sync` async trait plus a
//! registry that registers, looks up, and enumerates implementations.
//!
//! # Boundary (Locked decision L2)
//!
//! A [`RemoteAccessProvider`] is a **pure tunnel**: it only knows how to
//! `start` a tunnel from a local [`SocketAddr`](std::net::SocketAddr) and hand
//! back a public URL, `stop` it, and report `health`. Authentication,
//! authorization, and any token/ticket exchange live in the gateway/web layer
//! and never leak into a provider. The gateway always binds loopback; the
//! tunnel connects outbound and dials back in (Locked decision L3).
//!
//! # Reserved future backends (Locked decision L7)
//!
//! This crate is rmux-free by design. The trait is the only seam additional
//! backends need: FRP, a self-hosted lightweight relay, and other NAT-traversal
//! tunnels are **reserved** — they will implement [`RemoteAccessProvider`] in
//! their own modules later and register through [`RemoteRegistry`] with zero
//! changes to this core. **None of those reserved backends are implemented this
//! release** (see ADR `docs/decisions/2026-05-30-remote-access-tunnel-adapter.md`,
//! decision L7); only the trait seam + commented `lucarned.yaml` `provider`
//! placeholders exist for them. The first and only concrete backend this release,
//! [`Cloudflared`], lives in the [`cloudflared`] module and is wired into
//! [`builtin`].

use std::collections::BTreeMap;
use std::net::SocketAddr;
use std::sync::Arc;

use async_trait::async_trait;
use tracing::info;

pub mod cloudflared;

pub use cloudflared::Cloudflared;

/// Result alias for remote-access operations.
pub type RemoteResult<T> = Result<T, RemoteError>;

/// Errors produced while managing a remote-access tunnel.
#[derive(Debug, thiserror::Error)]
pub enum RemoteError {
    /// Failed to spawn or launch the underlying tunnel process/backend.
    #[error("failed to spawn tunnel for provider `{provider}`: {message}")]
    Spawn {
        /// Provider id that failed to spawn.
        provider: String,
        /// Human-readable failure detail.
        message: String,
    },

    /// Failed to parse backend output (e.g. the public URL from logs).
    #[error("failed to parse tunnel output: {0}")]
    Parse(String),

    /// A required configuration field was missing.
    #[error("missing required config field `{0}`")]
    MissingField(String),

    /// The referenced provider/tunnel could not be found.
    #[error("remote-access provider `{0}` not found")]
    NotFound(String),

    /// An underlying I/O error.
    #[error("remote-access io error: {0}")]
    Io(#[from] std::io::Error),
}

/// Health of a live tunnel as reported by its provider.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TunnelStatus {
    /// The tunnel is up and the public URL should be reachable.
    Up,
    /// The tunnel is down / no longer serving.
    Down,
    /// The provider could not determine the tunnel state.
    Unknown,
}

/// Describes one configuration field a provider needs, used to drive CLI
/// prompting (label/secret/required) without the CLI knowing provider details.
///
/// M7: a field can also be *conditionally* required via [`required_when`]: it
/// becomes required only when another field already has a given value (e.g.
/// cloudflared's `public_url` is required only when a named-tunnel `token` is
/// present). This lets the CLI drive an interactive prompt for the conditional
/// field from the descriptor alone — no provider-specific branching in the CLI.
#[derive(Debug, Clone)]
pub struct RequiredField {
    /// Stable machine key looked up in [`ProviderConfig`].
    pub key: &'static str,
    /// Human-readable prompt label.
    pub label: &'static str,
    /// Whether the value is sensitive (mask input, never log).
    pub secret: bool,
    /// Whether the value must ALWAYS be supplied for `start` to succeed.
    pub required: bool,
    /// M7: conditional requirement — `Some((key, value))` makes this field
    /// required only when field `key` is present with exactly `value` (e.g.
    /// `Some(("token", _))`-style "required when a token is set"). The match is
    /// "field `key` present and equal to `value`"; use the sentinel value
    /// [`ANY_VALUE`] to mean "required when `key` is present with ANY non-empty
    /// value". `None` → no conditional rule (only [`required`](Self::required)
    /// applies).
    pub required_when: Option<(&'static str, &'static str)>,
}

/// Sentinel for [`RequiredField::required_when`] meaning "the gating field is
/// present with ANY non-empty value" (vs. an exact value match).
pub const ANY_VALUE: &str = "*";

impl RequiredField {
    /// Whether this field is required given the rest of `cfg` (M7): always-required
    /// fields are required unconditionally; a `required_when` field is required
    /// only when its gating field is present (matching the configured value, or
    /// any non-empty value for the [`ANY_VALUE`] sentinel).
    pub fn is_required(&self, cfg: &ProviderConfig) -> bool {
        if self.required {
            return true;
        }
        match self.required_when {
            Some((gate_key, ANY_VALUE)) => cfg.get(gate_key).is_some_and(|v| !v.is_empty()),
            Some((gate_key, gate_value)) => cfg.get(gate_key) == Some(gate_value),
            None => false,
        }
    }
}

/// Opaque, provider-agnostic configuration: a flat key→value field map.
///
/// Keys correspond to [`RequiredField::key`] entries advertised by a provider.
/// Backend-specific structure is intentionally kept out of this base type.
#[derive(Debug, Clone, Default)]
pub struct ProviderConfig {
    /// Field values keyed by [`RequiredField::key`].
    pub fields: BTreeMap<String, String>,
}

impl ProviderConfig {
    /// Create an empty config.
    pub fn new() -> Self {
        Self::default()
    }

    /// Look up a configured field value by key.
    pub fn get(&self, key: &str) -> Option<&str> {
        self.fields.get(key).map(String::as_str)
    }

    /// Look up a required field, returning [`RemoteError::MissingField`] when
    /// absent. Convenience for providers validating their inputs in `start`.
    pub fn require(&self, key: &str) -> RemoteResult<&str> {
        self.get(key)
            .ok_or_else(|| RemoteError::MissingField(key.to_string()))
    }
}

/// Handle to a running tunnel.
///
/// Field set is intentionally minimal (`provider_id` + `public_url`) so the
/// base type does not grow backend-specific fields. The opaque handle the
/// provider needs to later `stop`/`health` the tunnel (e.g. a child pid or
/// async task id) is carried in [`opaque`](Self::opaque) as a provider-owned
/// string the provider alone interprets.
#[derive(Debug, Clone)]
pub struct TunnelHandle {
    /// Id of the provider that owns this tunnel.
    pub provider_id: String,
    /// Public URL the tunnel exposes.
    pub public_url: url::Url,
    /// Provider-owned opaque handle (e.g. child pid / task id) the provider
    /// interprets when stopping or health-checking the tunnel.
    pub opaque: String,
}

/// A transport-agnostic remote-access tunnel backend.
///
/// Mirrors `lucarne-adapter`'s `AdapterPlugin`: `Send + Sync`, async, with a
/// stable `id`/`name`. A provider is a **pure tunnel** (Locked decision L2):
/// `start` a local addr → public URL, `stop`, and report `health`. No auth.
///
/// # Reserved backends (Locked decision L7)
///
/// Additional backends — FRP, a self-hosted lightweight relay, and other
/// NAT-traversal tunnels — are **reserved seams only and are not implemented
/// this release**. They will implement this trait in their own modules and
/// register via [`RemoteRegistry`] with zero core changes. The only concrete
/// implementation shipped here is [`Cloudflared`]. See ADR
/// `docs/decisions/2026-05-30-remote-access-tunnel-adapter.md` (decision L7) for
/// the reserved-backend rationale and the path to add one.
#[async_trait]
pub trait RemoteAccessProvider: Send + Sync {
    /// Stable, unique provider id (e.g. `"cloudflared"`).
    fn id(&self) -> &'static str;

    /// Human-readable provider name.
    fn name(&self) -> &'static str;

    /// Fields this provider needs, used to drive CLI prompting.
    fn required_fields(&self) -> &[RequiredField];

    /// Deprecated provider-owned config section names accepted as compatibility
    /// aliases for `remote.providers.<provider-id>`.
    ///
    /// The daemon treats these as opaque section names supplied by the provider
    /// descriptor. Concrete alias names, field keys, and compatibility policy
    /// stay at the provider boundary instead of becoming daemon/common schema.
    fn compat_config_sections(&self) -> &[&'static str] {
        &[]
    }

    /// Operator-facing warnings about a given configuration (H6a).
    ///
    /// Lets a provider surface security/operational caveats about the config it
    /// is about to run with — e.g. a Cloudflare *quick* tunnel terminates TLS at
    /// the Cloudflare edge, so terminal content is visible there. The daemon
    /// logs these instead of branching on a concrete provider id, keeping
    /// provider-specific warnings inside the provider (AGENTS.md boundary).
    /// Default: no warnings.
    fn warnings(&self, _cfg: &ProviderConfig) -> Vec<String> {
        Vec::new()
    }

    /// Validate a configuration before `start` (M7).
    ///
    /// Lets a provider enforce cross-field / conditional rules that a flat
    /// per-field `required` flag cannot express — e.g. cloudflared requires
    /// `public_url` only when a named-tunnel `token` is present. The daemon (and
    /// the CLI's interactive prompt) call this so the rule lives with the
    /// provider, never as a CLI/daemon branch on a concrete provider id
    /// (AGENTS.md boundary).
    ///
    /// The default implementation enforces the descriptor's own
    /// [`RequiredField::is_required`] rules (always-required + `required_when`),
    /// so a provider that only needs declarative conditional requirements gets
    /// them for free; override to add richer checks (e.g. value formats).
    /// Returns `Err(message)` describing the first violation.
    fn validate_config(&self, cfg: &ProviderConfig) -> Result<(), String> {
        for field in self.required_fields() {
            if field.is_required(cfg) && cfg.get(field.key).filter(|v| !v.is_empty()).is_none() {
                return Err(format!("missing required config field `{}`", field.key));
            }
        }
        Ok(())
    }

    /// HTTP header name(s) this provider's edge sets to carry the real client IP
    /// (H6b). The gateway trusts these to resolve client identity — but ONLY when
    /// the socket peer is the loopback tunnel source (a direct remote peer could
    /// otherwise spoof them). Keeping the header name in the provider (rather than
    /// hardcoded in the gateway) honors the provider boundary (AGENTS.md): e.g.
    /// cloudflared returns `["cf-connecting-ip"]`. Default: none → the gateway
    /// uses the socket peer only.
    fn forwarded_identity_headers(&self) -> &[&'static str] {
        &[]
    }

    /// Start a tunnel from `local` and return a handle carrying the public URL.
    async fn start(&self, local: SocketAddr, cfg: &ProviderConfig) -> RemoteResult<TunnelHandle>;

    /// Stop a previously started tunnel, consuming its handle.
    async fn stop(&self, handle: TunnelHandle) -> RemoteResult<()>;

    /// Report the current health of a running tunnel.
    async fn health(&self, handle: &TunnelHandle) -> RemoteResult<TunnelStatus>;
}

/// Registry of remote-access providers.
///
/// Mirrors `lucarne-adapter`'s `AdapterRegistry`: register implementations,
/// then look one up by id or enumerate the registered ids. Backends are stored
/// as `Arc<dyn RemoteAccessProvider>` so they can be shared with the daemon
/// task holding the tunnel lifecycle (Locked decision L6).
///
/// Reserved backends (FRP / lightweight relay / other NAT-traversal tunnels —
/// Locked decision L7) are **not implemented this release**; when added they
/// join here via [`register`](Self::register) with zero core changes. See ADR
/// `docs/decisions/2026-05-30-remote-access-tunnel-adapter.md`.
#[derive(Default, Clone)]
pub struct RemoteRegistry {
    providers: Vec<Arc<dyn RemoteAccessProvider>>,
}

impl RemoteRegistry {
    /// Create an empty registry.
    pub fn new() -> Self {
        Self::default()
    }

    /// Register a provider implementation.
    pub fn register<P>(&mut self, provider: P)
    where
        P: RemoteAccessProvider + 'static,
    {
        info!(
            target: "lucarne_remote",
            provider_id = provider.id(),
            provider_name = provider.name(),
            "remote-access provider registered"
        );
        self.providers.push(Arc::new(provider));
    }

    /// Look up a registered provider by id.
    pub fn get(&self, id: &str) -> Option<Arc<dyn RemoteAccessProvider>> {
        self.providers
            .iter()
            .find(|provider| provider.id() == id)
            .cloned()
    }

    /// Alias for [`get`](Self::get) — mirrors `AdapterRegistry` lookup naming.
    pub fn lookup(&self, id: &str) -> Option<Arc<dyn RemoteAccessProvider>> {
        self.get(id)
    }

    /// Ids of all registered providers, in registration order.
    pub fn ids(&self) -> Vec<&'static str> {
        self.providers
            .iter()
            .map(|provider| provider.id())
            .collect()
    }

    /// Alias for [`ids`](Self::ids) — mirrors `AdapterRegistry` enumerate naming.
    pub fn enumerate(&self) -> Vec<&'static str> {
        self.ids()
    }
}

/// Built-in provider registry.
///
/// Ships the concrete [`Cloudflared`] backend (Free decision F3: quick tunnel by
/// default, named tunnel via token). Reserved backends (FRP / lightweight relay
/// / other tunnels — Locked decision L7) join the same way later by registering
/// through [`RemoteRegistry::register`].
pub fn builtin() -> RemoteRegistry {
    let mut registry = RemoteRegistry::new();
    registry.register(Cloudflared::new());
    registry
}

#[cfg(test)]
mod tests {
    use super::*;

    struct DummyProvider {
        id: &'static str,
        fields: Vec<RequiredField>,
    }

    #[async_trait]
    impl RemoteAccessProvider for DummyProvider {
        fn id(&self) -> &'static str {
            self.id
        }

        fn name(&self) -> &'static str {
            "Dummy"
        }

        fn required_fields(&self) -> &[RequiredField] {
            &self.fields
        }

        async fn start(
            &self,
            _local: SocketAddr,
            _cfg: &ProviderConfig,
        ) -> RemoteResult<TunnelHandle> {
            Ok(TunnelHandle {
                provider_id: self.id.to_string(),
                public_url: url::Url::parse("https://example.test/").unwrap(),
                opaque: String::new(),
            })
        }

        async fn stop(&self, _handle: TunnelHandle) -> RemoteResult<()> {
            Ok(())
        }

        async fn health(&self, _handle: &TunnelHandle) -> RemoteResult<TunnelStatus> {
            Ok(TunnelStatus::Up)
        }
    }

    fn dummy(id: &'static str) -> DummyProvider {
        DummyProvider {
            id,
            fields: vec![RequiredField {
                key: "token",
                label: "Access Token",
                secret: true,
                required: true,
                required_when: None,
            }],
        }
    }

    #[test]
    fn register_get_ids_round_trip() {
        let mut registry = RemoteRegistry::new();
        assert!(registry.ids().is_empty());

        registry.register(dummy("alpha"));
        registry.register(dummy("beta"));

        assert_eq!(registry.ids(), vec!["alpha", "beta"]);
        assert_eq!(registry.enumerate(), vec!["alpha", "beta"]);

        let found = registry.get("beta").expect("beta registered");
        assert_eq!(found.id(), "beta");
        // lookup is an alias of get.
        assert_eq!(registry.lookup("beta").unwrap().id(), "beta");

        assert!(registry.get("missing").is_none());
    }

    #[test]
    fn required_field_construction() {
        let provider = dummy("alpha");
        let fields = provider.required_fields();
        assert_eq!(fields.len(), 1);
        assert_eq!(fields[0].key, "token");
        assert_eq!(fields[0].label, "Access Token");
        assert!(fields[0].secret);
        assert!(fields[0].required);
    }

    #[test]
    fn provider_config_get_and_require() {
        let mut cfg = ProviderConfig::new();
        cfg.fields.insert("token".to_string(), "secret".to_string());

        assert_eq!(cfg.get("token"), Some("secret"));
        assert_eq!(cfg.require("token").unwrap(), "secret");
        assert!(cfg.get("missing").is_none());
        assert!(matches!(
            cfg.require("missing"),
            Err(RemoteError::MissingField(key)) if key == "missing"
        ));
    }

    #[test]
    fn builtin_registry_contains_cloudflared() {
        let registry = builtin();
        assert!(registry.ids().contains(&"cloudflared"));
        assert!(registry.get("cloudflared").is_some());
    }

    // M7: a `required_when` field is required only when its gating field is
    // present, and the default `validate_config` enforces both always-required
    // and conditional fields.
    #[test]
    fn required_when_drives_conditional_requirement() {
        struct CondProvider {
            fields: Vec<RequiredField>,
        }
        #[async_trait]
        impl RemoteAccessProvider for CondProvider {
            fn id(&self) -> &'static str {
                "cond"
            }
            fn name(&self) -> &'static str {
                "Cond"
            }
            fn required_fields(&self) -> &[RequiredField] {
                &self.fields
            }
            async fn start(
                &self,
                _local: SocketAddr,
                _cfg: &ProviderConfig,
            ) -> RemoteResult<TunnelHandle> {
                unreachable!("not started in this test")
            }
            async fn stop(&self, _handle: TunnelHandle) -> RemoteResult<()> {
                Ok(())
            }
            async fn health(&self, _handle: &TunnelHandle) -> RemoteResult<TunnelStatus> {
                Ok(TunnelStatus::Up)
            }
        }

        let provider = CondProvider {
            fields: vec![
                RequiredField {
                    key: "token",
                    label: "Token",
                    secret: true,
                    required: false,
                    required_when: None,
                },
                RequiredField {
                    key: "public_url",
                    label: "Public URL",
                    secret: false,
                    required: false,
                    // Required only when a token is present (any non-empty value).
                    required_when: Some(("token", ANY_VALUE)),
                },
            ],
        };

        // No token → public_url is not required → config validates.
        let empty = ProviderConfig::new();
        assert!(!provider.required_fields()[1].is_required(&empty));
        assert!(provider.validate_config(&empty).is_ok());

        // Token present but no public_url → public_url becomes required → error.
        let mut with_token = ProviderConfig::new();
        with_token
            .fields
            .insert("token".to_string(), "abc".to_string());
        assert!(provider.required_fields()[1].is_required(&with_token));
        let err = provider
            .validate_config(&with_token)
            .expect_err("public_url required when token set");
        assert!(err.contains("public_url"), "got: {err}");

        // Token + public_url → validates.
        let mut both = with_token.clone();
        both.fields
            .insert("public_url".to_string(), "https://x".to_string());
        assert!(provider.validate_config(&both).is_ok());

        // Exact-value match form.
        let exact = RequiredField {
            key: "extra",
            label: "Extra",
            secret: false,
            required: false,
            required_when: Some(("mode", "named")),
        };
        let mut mode_named = ProviderConfig::new();
        mode_named
            .fields
            .insert("mode".to_string(), "named".to_string());
        assert!(exact.is_required(&mode_named));
        let mut mode_quick = ProviderConfig::new();
        mode_quick
            .fields
            .insert("mode".to_string(), "quick".to_string());
        assert!(!exact.is_required(&mode_quick));
    }
}
