//! Typed remote-access config resolution shared by the daemon and tests.
//!
//! Keep the YAML shape, env precedence, token validation, provider-field merge,
//! and loopback/control-port hardening out of `main.rs` so future UI/CLI changes
//! do not grow a second interpretation of `remote:`.

use std::collections::BTreeMap;
use std::net::SocketAddr;

use serde::Deserialize;
use serde_json::Value;
use tracing::{debug, warn};

use crate::remote;

/// Default loopback gateway port.
pub const DEFAULT_REMOTE_GATEWAY_PORT: u16 = 7800;
/// Default loopback control-plane port.
pub const DEFAULT_REMOTE_CONTROL_PORT: u16 = 7801;
/// Default loopback bind for the remote-access gateway. This is the ONLY port
/// the public tunnel targets.
pub const DEFAULT_REMOTE_GATEWAY_ADDR: &str = "127.0.0.1:7800";
/// Default loopback bind for the remote-access control plane. It must stay on a
/// distinct port the public tunnel never targets.
pub const DEFAULT_REMOTE_CONTROL_ADDR: &str = "127.0.0.1:7801";
/// Default remote-access tunnel provider id.
pub const DEFAULT_REMOTE_PROVIDER: &str = "cloudflared";

/// Remote-access (public tunnel) configuration — the `remote:` section of
/// `lucarned.yaml`. Mirrors the public documented schema while leaving provider
/// fields as opaque key/value maps.
#[derive(Clone, Debug, Default, Deserialize)]
pub(crate) struct RemoteFileConfig {
    pub enabled: Option<bool>,
    pub provider: Option<String>,
    pub gateway_addr: Option<String>,
    pub control_addr: Option<String>,
    pub auth_token: Option<String>,
    pub readonly_token: Option<String>,
    pub insecure: Option<bool>,
    #[serde(default)]
    pub providers: BTreeMap<String, BTreeMap<String, Option<String>>>,
    /// Opaque extra YAML sections. Provider-owned compatibility aliases are
    /// discovered through [`lucarne_remote::RemoteAccessProvider`] descriptors.
    /// Daemon/common config must not own concrete provider-specific structs.
    #[serde(default, flatten)]
    pub extra_sections: BTreeMap<String, Value>,
}

/// Environment read seam so tests and future callers can resolve config without
/// mutating process-global env.
pub(crate) trait RemoteConfigEnv {
    fn var(&self, key: &str) -> Option<String>;
}

#[derive(Clone, Copy, Debug)]
pub(crate) struct ProcessEnv;

impl RemoteConfigEnv for ProcessEnv {
    fn var(&self, key: &str) -> Option<String> {
        std::env::var(key).ok()
    }
}

/// Resolve the remote-access runtime config from file config + process env.
pub(crate) fn remote_config_from_config(
    remote: &RemoteFileConfig,
) -> Result<Option<remote::RemoteRuntimeConfig>, Box<dyn std::error::Error>> {
    remote_config_from_config_with_env(remote, ProcessEnv)
}

/// Resolve the remote-access runtime config from file config + injected env.
///
/// Cold-daemon lazy start: this always resolves to `Some`. `remote.enabled`
/// means autostart, not whether the loopback control plane exists.
pub(crate) fn remote_config_from_config_with_env<E: RemoteConfigEnv>(
    remote: &RemoteFileConfig,
    env: E,
) -> Result<Option<remote::RemoteRuntimeConfig>, Box<dyn std::error::Error>> {
    let autostart = env
        .var("LUCARNED_REMOTE_ENABLED")
        .as_deref()
        .and_then(parse_bool)
        .or(remote.enabled)
        .unwrap_or(false);

    let provider = env
        .var("LUCARNED_REMOTE_PROVIDER")
        .or_else(|| remote.provider.clone())
        .unwrap_or_else(|| DEFAULT_REMOTE_PROVIDER.to_string());

    let gateway_addr_raw = env
        .var("LUCARNED_REMOTE_GATEWAY_ADDR")
        .or_else(|| remote.gateway_addr.clone())
        .unwrap_or_else(default_remote_gateway_addr);
    let gateway_addr = lucarne_termgw::parse_gateway_addr(&gateway_addr_raw)?;

    let control_addr_raw = env
        .var("LUCARNED_REMOTE_CONTROL_ADDR")
        .or_else(|| remote.control_addr.clone())
        .map(Ok)
        .unwrap_or_else(|| derive_control_addr(&gateway_addr_raw, gateway_addr))?;
    let control_addr = lucarne_termgw::parse_gateway_addr(&control_addr_raw)?;
    ensure_control_plane_off_tunnel(gateway_addr, control_addr)?;

    let auth_token = env
        .var("LUCARNED_REMOTE_AUTH_TOKEN")
        .filter(|t| !t.is_empty())
        .or_else(|| remote.auth_token.clone().filter(|t| !t.is_empty()));
    let readonly_token = env
        .var("LUCARNED_REMOTE_READONLY_TOKEN")
        .filter(|t| !t.is_empty())
        .or_else(|| remote.readonly_token.clone().filter(|t| !t.is_empty()));
    let insecure = env
        .var("LUCARNED_REMOTE_INSECURE")
        .as_deref()
        .and_then(parse_bool)
        .or(remote.insecure)
        .unwrap_or(false);

    validate_tokens(auth_token.as_deref(), readonly_token.as_deref())?;
    let provider_fields = resolve_provider_fields(remote, &provider);

    if insecure {
        warn!(
            target: "lucarned::remote_config",
            "remote config explicitly disables gateway auth"
        );
    }
    debug!(
        target: "lucarned::remote_config",
        provider = %provider,
        autostart,
        gateway_addr = %gateway_addr,
        control_addr = %control_addr,
        auth_configured = auth_token.is_some(),
        readonly_configured = readonly_token.is_some(),
        insecure,
        provider_field_count = provider_fields.len(),
        "resolved remote config"
    );

    Ok(Some(remote::RemoteRuntimeConfig {
        provider: provider.clone(),
        gateway_addr,
        control_addr,
        auth_token,
        readonly_token,
        insecure,
        provider_fields,
        capability: remote::ExposedCapability::TerminalGateway,
        autostart,
    }))
}

fn derive_control_addr(
    gateway_addr_raw: &str,
    gateway_addr: SocketAddr,
) -> Result<String, Box<dyn std::error::Error>> {
    if gateway_addr_raw == DEFAULT_REMOTE_GATEWAY_ADDR {
        return Ok(default_remote_control_addr());
    }
    match gateway_addr.port().checked_add(1) {
        Some(port) => Ok(SocketAddr::new(gateway_addr.ip(), port).to_string()),
        None => Err(format!(
            "remote.gateway_addr port {} leaves no room for a derived control port; \
             set remote.control_addr / --control-port explicitly (L1)",
            gateway_addr.port()
        )
        .into()),
    }
}

fn default_remote_gateway_addr() -> String {
    format!("127.0.0.1:{DEFAULT_REMOTE_GATEWAY_PORT}")
}

fn default_remote_control_addr() -> String {
    debug_assert_eq!(
        DEFAULT_REMOTE_CONTROL_ADDR,
        format!("127.0.0.1:{DEFAULT_REMOTE_CONTROL_PORT}")
    );
    DEFAULT_REMOTE_CONTROL_ADDR.to_string()
}

fn ensure_control_plane_off_tunnel(
    gateway_addr: SocketAddr,
    control_addr: SocketAddr,
) -> Result<(), Box<dyn std::error::Error>> {
    if control_addr.port() == gateway_addr.port() {
        return Err(format!(
            "remote.control_addr ({control_addr}) must use a different port than \
             remote.gateway_addr ({gateway_addr}) so the control plane is off the tunnel (SEC-002)"
        )
        .into());
    }
    Ok(())
}

fn validate_tokens(
    auth_token: Option<&str>,
    readonly_token: Option<&str>,
) -> Result<(), Box<dyn std::error::Error>> {
    if let Some(token) = auth_token {
        lucarne_termgw::AccessToken::from_secret_validated(token.to_string())
            .map_err(|e| -> Box<dyn std::error::Error> { e.to_string().into() })?;
    }
    if let Some(token) = readonly_token {
        lucarne_termgw::AccessToken::from_secret_validated(token.to_string()).map_err(
            |e| -> Box<dyn std::error::Error> { format!("remote.readonly_token: {e}").into() },
        )?;
    }
    Ok(())
}

/// Assemble the generic provider field map for `provider` (H6c).
///
/// Precedence: provider-declared compatibility sections first, then
/// `remote.providers.<provider>` over them. Empty/null values are absent.
pub(crate) fn resolve_provider_fields(
    remote: &RemoteFileConfig,
    provider: &str,
) -> BTreeMap<String, String> {
    let mut fields = BTreeMap::new();
    if let Some(provider_descriptor) = lucarne_remote::builtin().lookup(provider) {
        for section in provider_descriptor.compat_config_sections() {
            if let Some(section_fields) = remote.extra_sections.get(*section) {
                merge_provider_section(&mut fields, section_fields);
            }
        }
    }
    if let Some(map) = remote.providers.get(provider) {
        for (key, value) in map {
            if let Some(v) = value.as_deref().filter(|v| !v.is_empty()) {
                fields.insert(key.clone(), v.to_string());
            }
        }
    }
    fields
}

fn merge_provider_section(fields: &mut BTreeMap<String, String>, section: &Value) {
    let Some(map) = section.as_object() else {
        return;
    };
    for (key, value) in map {
        match value {
            Value::String(value) if !value.is_empty() => {
                fields.insert(key.clone(), value.clone());
            }
            _ => {}
        }
    }
}

pub(crate) fn parse_bool(value: &str) -> Option<bool> {
    match value.trim().to_ascii_lowercase().as_str() {
        "1" | "true" | "yes" | "on" => Some(true),
        "0" | "false" | "no" | "off" => Some(false),
        _ => None,
    }
}
