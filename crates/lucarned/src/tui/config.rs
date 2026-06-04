//! Config panel — a remote-access provider editor driven ENTIRELY by the
//! built-in provider descriptors (`lucarne_remote::builtin()`), with edits saved
//! back to `lucarned.yaml` via the in-crate
//! [`write_config_with_backup`](crate::onboarding::config::write_config_with_backup)
//! (backup + atomic tmp+rename — Decision 5).
//!
//! Provider boundary (AGENTS.md): the panel NEVER enumerates concrete provider
//! field names. It lists providers from the registry and, for the selected
//! provider, builds a form from `RemoteAccessProvider::required_fields()`
//! (`RequiredField { key, label, secret, required }`). Secret fields are masked
//! on display AND while editing (the secret is never echoed). Only top-level
//! remote daemon config keys and provider descriptor fields are editable here.

use std::collections::BTreeMap;
use std::path::PathBuf;

use crossterm::event::KeyCode;
use ratatui::widgets::ListState;

use lucarne_remote::ProviderConfig;

use crate::remote_config;

/// Default gateway/control ports come from the daemon's typed remote config
/// service, not duplicated literals in the TUI.
const DEFAULT_GATEWAY_PORT: u16 = remote_config::DEFAULT_REMOTE_GATEWAY_PORT;
const DEFAULT_CONTROL_PORT: u16 = remote_config::DEFAULT_REMOTE_CONTROL_PORT;

// ---- Config panel (TASK-004): descriptor-driven form + YAML write-back ----

/// One editable row in the config form. Top-level rows are panel-owned daemon
/// config; [`Field`](Row::Field) rows are built one-per-descriptor-entry from the
/// selected provider's `required_fields()` — the panel never names a concrete
/// provider field itself.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Row {
    /// `remote.enabled`: true autostarts the tunnel at daemon boot; false keeps
    /// the loopback control plane ready and starts lazily.
    Enabled,
    /// The selected `remote.provider` top-level key. Editing cycles through the
    /// registered provider ids (a closed set — never free-typed).
    Provider,
    /// The `remote.gateway_addr` loopback port.
    GatewayPort,
    /// The `remote.control_addr` loopback port.
    ControlPort,
    /// `remote.auth_token`, optional full-access bearer token.
    AuthToken,
    /// `remote.readonly_token`, optional read-only bearer token.
    ReadonlyToken,
    /// `remote.insecure`, explicit no-auth opt-out.
    Insecure,
    /// One provider field from the descriptor: `remote.providers.<id>.<key>`.
    Field {
        /// Stable machine key from [`RequiredField::key`].
        key: &'static str,
        /// Human-readable label from [`RequiredField::label`].
        label: &'static str,
        /// Whether the value is sensitive (masked on display + while editing).
        secret: bool,
        /// Whether the provider requires this field.
        required: bool,
    },
}

/// Render one config row's current display value. Secret field values are MASKED
/// (`••••` when set, `(unset)` when empty) so a token is never echoed; non-secret
/// values are shown verbatim. The `provider`/port rows are always plain.
fn display_value(row: &Row, edits: &ConfigEdits) -> String {
    match row {
        Row::Enabled => edits.enabled.to_string(),
        Row::Provider => edits.provider.clone(),
        Row::GatewayPort => edits.gateway_port.to_string(),
        Row::ControlPort => edits.control_port.to_string(),
        Row::AuthToken => mask_value(edits.auth_token.as_deref().unwrap_or(""), true),
        Row::ReadonlyToken => mask_value(edits.readonly_token.as_deref().unwrap_or(""), true),
        Row::Insecure => edits.insecure.to_string(),
        Row::Field { key, secret, .. } => {
            let value = edits.fields.get(*key).map(String::as_str).unwrap_or("");
            mask_value(value, *secret)
        }
    }
}

/// Mask a value for display: secret + non-empty → `••••` (fixed length, never the
/// real length so width is not a hint); secret + empty → `(unset)`; non-secret →
/// the value verbatim (or `(unset)` when empty).
fn mask_value(value: &str, secret: bool) -> String {
    if value.is_empty() {
        return "(unset)".to_string();
    }
    if secret {
        "••••".to_string()
    } else {
        value.to_string()
    }
}

/// The panel-owned edit buffer: the values the user is editing, kept separate
/// from the parsed-YAML source so a save merges these over the file (preserving
/// unrelated keys). Top-level remote keys are generic daemon config; `fields`
/// are the selected provider's opaque field map (`key` → value), keyed by
/// descriptor [`RequiredField::key`].
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct ConfigEdits {
    /// `remote.enabled`: autostart flag. Lazy start still works when false.
    pub enabled: bool,
    /// Selected `remote.provider` id.
    pub provider: String,
    /// `remote.gateway_addr` loopback port.
    pub gateway_port: u16,
    /// `remote.control_addr` loopback port.
    pub control_port: u16,
    /// The selected provider's field values (`key` → value); empty values are
    /// dropped on save so they never shadow a "leave blank" default.
    pub fields: BTreeMap<String, String>,
    /// Optional full-access bearer token.
    pub auth_token: Option<String>,
    /// Optional read-only bearer token.
    pub readonly_token: Option<String>,
    /// Explicit no-auth opt-out.
    pub insecure: bool,
}

/// The Config panel: a descriptor-driven provider-field editor.
///
/// Holds the registered provider ids, the in-progress [`ConfigEdits`], the form
/// rows for the selected provider, list/cursor state, an inline edit buffer, and
/// a status line. All provider specifics flow through `lucarne_remote` descriptors
/// — the panel only routes opaque ids + descriptor-supplied keys (AGENTS.md).
pub struct ConfigPanel {
    /// Registered provider ids (registration order), from `builtin().ids()`.
    pub providers: Vec<&'static str>,
    /// Index into [`providers`](Self::providers) of the selected provider.
    pub provider_index: usize,
    /// The current edit buffer merged onto `lucarned.yaml` on save.
    pub edits: ConfigEdits,
    /// The form rows for the selected provider (top-level keys + descriptor fields).
    pub rows: Vec<Row>,
    /// Cursor over [`rows`](Self::rows).
    pub list: ListState,
    /// When editing a row, the in-progress input buffer (raw, even for secrets).
    pub editing: Option<String>,
    /// Last status / error / save-result line.
    pub status: Option<String>,
    /// Resolved `lucarned.yaml` path (the daemon's resolution); `None` if it could
    /// not be resolved (no `$LUCARNE_CONFIG` and no home dir).
    pub config_path: Option<PathBuf>,
}

impl Default for ConfigPanel {
    fn default() -> Self {
        let providers = lucarne_remote::builtin().ids();
        let provider_index = 0;
        let provider = providers.first().copied().unwrap_or("").to_string();
        let edits = ConfigEdits {
            enabled: false,
            provider,
            gateway_port: DEFAULT_GATEWAY_PORT,
            control_port: DEFAULT_CONTROL_PORT,
            fields: BTreeMap::new(),
            auth_token: None,
            readonly_token: None,
            insecure: false,
        };
        let rows = build_rows(edits.provider.as_str());
        let mut list = ListState::default();
        if !rows.is_empty() {
            list.select(Some(0));
        }
        Self {
            providers,
            provider_index,
            edits,
            rows,
            list,
            editing: None,
            status: None,
            config_path: None,
        }
    }
}

impl ConfigPanel {
    /// Build a fresh panel. Construction is I/O-free + test-friendly; call
    /// [`Self::load`] once to resolve the config path and seed values from
    /// `lucarned.yaml`.
    pub fn new() -> Self {
        Self::default()
    }

    /// The live start parameters the Go-Public panel's `s` key should use (PART 1):
    /// the currently-selected provider id and its NON-EMPTY field values, taken
    /// straight from the in-TUI edit buffer (no `lucarned.yaml` save required). The
    /// daemon merges these over its pre-config (G3), so configuring here and then
    /// pressing `s` in Go Public "configures + goes public" entirely inside the TUI.
    ///
    /// Pure + I/O-free: empty field values are skipped so a blank optional field
    /// never shadows a daemon default. An empty `provider` (the degenerate
    /// no-registered-providers case) yields empty params → the daemon falls back to
    /// its pre-configured tunnel.
    pub fn start_params(&self) -> (String, BTreeMap<String, String>) {
        let fields = self
            .edits
            .fields
            .iter()
            .filter(|(_, v)| !v.is_empty())
            .map(|(k, v)| (k.clone(), v.clone()))
            .collect();
        (self.edits.provider.clone(), fields)
    }

    /// Resolve the daemon config path (`$LUCARNE_CONFIG`/`$LUCARNED_CONFIG` or
    /// `~/.lucarned/lucarned.yaml`) and seed the edit buffer from its current
    /// `remote:` section when the file exists. Never panics on a missing file —
    /// the form just opens with defaults and the status offers to create it.
    pub fn load(&mut self) {
        let path = crate::onboarding::resolve_init_config_path();
        self.config_path = path.clone();
        match path.as_ref().and_then(|p| std::fs::read_to_string(p).ok()) {
            Some(raw) => {
                self.seed_from_yaml(&raw);
                self.status = Some(format!(
                    "loaded {}",
                    self.config_path
                        .as_ref()
                        .map(|p| p.display().to_string())
                        .unwrap_or_default()
                ));
            }
            None => {
                self.status = Some(match &self.config_path {
                    Some(p) => format!("{} not found — Enter to edit, S to create", p.display()),
                    None => "could not resolve lucarned.yaml path (set LUCARNE_CONFIG)".to_string(),
                });
            }
        }
        self.reload_rows();
    }

    /// Seed the edit buffer from a parsed `lucarned.yaml` string: the selected
    /// provider, the gateway/control ports, and the selected provider's field
    /// values. Unknown / malformed values fall back to the existing defaults.
    fn seed_from_yaml(&mut self, raw: &str) {
        let Ok(value) = serde_yaml::from_str::<serde_yaml::Value>(raw) else {
            return;
        };
        #[derive(serde::Deserialize)]
        struct RemoteSeed {
            #[serde(default)]
            remote: remote_config::RemoteFileConfig,
        }
        if let Ok(parsed) = serde_yaml::from_str::<RemoteSeed>(raw).map(|seed| seed.remote) {
            // Only adopt a provider we actually know about (opaque routing); an
            // unknown id keeps the default selection.
            if let Some(provider) = parsed.provider.as_deref() {
                if let Some(idx) = self.providers.iter().position(|p| *p == provider) {
                    self.provider_index = idx;
                    self.edits.provider = provider.to_string();
                }
            }
            self.edits.enabled = parsed.enabled.unwrap_or(self.edits.enabled);
            self.edits.auth_token = parsed.auth_token.filter(|v| !v.is_empty());
            self.edits.readonly_token = parsed.readonly_token.filter(|v| !v.is_empty());
            self.edits.insecure = parsed.insecure.unwrap_or(false);
        }
        let remote = mapping_get(&value, "remote");
        if let Some(port) = remote
            .and_then(|r| mapping_get(r, "gateway_addr"))
            .and_then(serde_yaml::Value::as_str)
            .and_then(port_of_addr)
        {
            self.edits.gateway_port = port;
        }
        if let Some(port) = remote
            .and_then(|r| mapping_get(r, "control_addr"))
            .and_then(serde_yaml::Value::as_str)
            .and_then(port_of_addr)
        {
            self.edits.control_port = port;
        }
        // Provider field values come from remote.providers.<selected>.* — opaque
        // key→value pairs, never interpreted here.
        seed_fields_for_provider(&value, self.edits.provider.as_str(), &mut self.edits.fields);
    }

    /// Re-seed the edit buffer's `fields` for the CURRENTLY-selected provider from
    /// the on-disk `lucarned.yaml` (COR-001). Called on a provider change so
    /// switching providers — and switching back — restores a provider's persisted
    /// `remote.providers.<id>.*` values instead of leaving them blanked (which a
    /// subsequent save would otherwise wipe via `merge_config_yaml`). A missing /
    /// unreadable / malformed file just leaves the fields empty (no panic).
    fn reseed_fields_from_file(&mut self) {
        self.edits.fields.clear();
        let Some(raw) = self
            .config_path
            .as_ref()
            .and_then(|p| std::fs::read_to_string(p).ok())
        else {
            return;
        };
        let Ok(value) = serde_yaml::from_str::<serde_yaml::Value>(&raw) else {
            return;
        };
        seed_fields_for_provider(&value, self.edits.provider.as_str(), &mut self.edits.fields);
    }

    /// Rebuild the form rows for the currently-selected provider and clamp the
    /// cursor back into range via the shared [`super::nav::clamp`]
    /// ([`super::nav::EmptyPolicy::SelectFirst`]: a non-empty form always keeps a
    /// focused row; an empty form clears the selection).
    fn reload_rows(&mut self) {
        self.rows = build_rows(self.edits.provider.as_str());
        super::nav::clamp(
            &mut self.list,
            self.rows.len(),
            super::nav::EmptyPolicy::SelectFirst,
        );
    }

    /// The display string for the row under the cursor (used by the renderer).
    pub fn row_display(&self, row: &Row) -> String {
        display_value(row, &self.edits)
    }

    /// Move the cursor down one row (clamped; ignored while editing). Delegates the
    /// clamp/step to the shared [`super::nav::step`].
    pub fn select_next(&mut self) {
        if self.editing.is_some() {
            return;
        }
        super::nav::step(&mut self.list, self.rows.len(), true);
    }

    /// Move the cursor up one row (clamped; ignored while editing). Delegates the
    /// clamp/step to the shared [`super::nav::step`].
    pub fn select_previous(&mut self) {
        if self.editing.is_some() {
            return;
        }
        super::nav::step(&mut self.list, self.rows.len(), false);
    }

    /// The row currently under the cursor, if any.
    fn selected_row(&self) -> Option<Row> {
        self.list.selected().and_then(|i| self.rows.get(i).cloned())
    }

    /// Handle a key for the Config panel.
    ///
    /// Not editing: `Enter` begins editing the selected row (the `Provider` row
    /// instead CYCLES to the next registered id — a closed set), `s` saves the
    /// config. While editing: printable chars append, `Backspace` deletes, `Enter`
    /// commits the buffer, `Esc` cancels. Secret rows capture chars but the
    /// renderer masks them — the typed value is never echoed.
    pub fn handle_key(&mut self, code: KeyCode) {
        if self.editing.is_some() {
            self.handle_edit_key(code);
            return;
        }
        match code {
            KeyCode::Enter => self.begin_edit(),
            KeyCode::Char('s') | KeyCode::Char('S') => self.save(),
            _ => {}
        }
    }

    /// Begin editing the selected row. The `Provider` row is a closed-set cycle
    /// (no free text), so `Enter` there advances to the next registered provider
    /// and boolean rows toggle instead of opening a buffer.
    /// The edit buffer starts EMPTY (typing replaces, not appends); committing an
    /// empty buffer clears a field / cancels a port edit.
    fn begin_edit(&mut self) {
        match self.selected_row() {
            Some(Row::Enabled) => {
                self.edits.enabled = !self.edits.enabled;
                self.status = Some(format!("autostart → {}", self.edits.enabled));
            }
            Some(Row::Provider) => self.cycle_provider(),
            Some(Row::Insecure) => {
                self.edits.insecure = !self.edits.insecure;
                self.status = Some(format!("insecure → {}", self.edits.insecure));
            }
            Some(_) => self.editing = Some(String::new()),
            None => {}
        }
    }

    /// Advance the selected provider to the next registered id (wraps) and rebuild
    /// the form from the new provider's descriptor.
    fn cycle_provider(&mut self) {
        if self.providers.is_empty() {
            return;
        }
        self.provider_index = (self.provider_index + 1) % self.providers.len();
        self.edits.provider = self.providers[self.provider_index].to_string();
        // A different provider has a different field set. Re-seed the new
        // provider's persisted values from `lucarned.yaml` (COR-001) so visiting a
        // provider — and a later save — never wipes its on-disk fields; then
        // rebuild the rows from the new descriptor.
        self.reseed_fields_from_file();
        self.reload_rows();
        self.status = Some(format!("provider → {}", self.edits.provider));
    }

    /// Apply one key while an inline edit buffer is open.
    fn handle_edit_key(&mut self, code: KeyCode) {
        match code {
            KeyCode::Char(c) => {
                if let Some(buf) = self.editing.as_mut() {
                    buf.push(c);
                }
            }
            KeyCode::Backspace => {
                if let Some(buf) = self.editing.as_mut() {
                    buf.pop();
                }
            }
            KeyCode::Esc => {
                self.editing = None;
                self.status = Some("edit cancelled".to_string());
            }
            KeyCode::Enter => self.commit_edit(),
            _ => {}
        }
    }

    /// Commit the inline edit buffer into the edit state for the selected row.
    /// An empty buffer on a port row keeps the current value (a no-op cancel);
    /// an empty buffer on a field row clears that field.
    fn commit_edit(&mut self) {
        let Some(buf) = self.editing.take() else {
            return;
        };
        let buf = buf.trim().to_string();
        match self.selected_row() {
            Some(Row::Enabled) => {}
            Some(Row::GatewayPort) if buf.is_empty() => {}
            Some(Row::GatewayPort) => match buf.parse::<u16>() {
                Ok(p) => self.edits.gateway_port = p,
                Err(_) => self.status = Some(format!("invalid port `{buf}`")),
            },
            Some(Row::ControlPort) if buf.is_empty() => {}
            Some(Row::ControlPort) => match buf.parse::<u16>() {
                Ok(p) => self.edits.control_port = p,
                Err(_) => self.status = Some(format!("invalid port `{buf}`")),
            },
            Some(Row::AuthToken) => {
                self.edits.auth_token = if buf.is_empty() { None } else { Some(buf) };
            }
            Some(Row::ReadonlyToken) => {
                self.edits.readonly_token = if buf.is_empty() { None } else { Some(buf) };
            }
            Some(Row::Insecure) => {}
            Some(Row::Field { key, .. }) => {
                if buf.is_empty() {
                    self.edits.fields.remove(key);
                } else {
                    self.edits.fields.insert(key.to_string(), buf);
                }
            }
            _ => {}
        }
    }

    /// Validate (via the provider descriptor) then save the edits back to
    /// `lucarned.yaml`: merge over the existing file (preserving unrelated keys)
    /// and write via the in-crate [`write_config_with_backup`]
    /// (crate::onboarding::config::write_config_with_backup) — backup + atomic
    /// tmp+rename. The status line reports the backup path / validation error /
    /// failure; never panics.
    pub fn save(&mut self) {
        // Provider-side validation lives in the descriptor (AGENTS.md): build the
        // opaque ProviderConfig and let the provider enforce its own rules.
        if let Err(e) = self.validate() {
            self.status = Some(format!("validation failed: {e}"));
            return;
        }
        let Some(path) = self.config_path.clone() else {
            self.status = Some("no config path resolved (set LUCARNE_CONFIG)".to_string());
            return;
        };
        // Merge over the existing file when present; otherwise start from an empty
        // mapping so a missing file is created with just the remote section.
        let existing = std::fs::read_to_string(&path).unwrap_or_default();
        let merged = match merge_config_yaml(&existing, &self.edits) {
            Ok(m) => m,
            Err(e) => {
                self.status = Some(format!("failed to render config: {e}"));
                return;
            }
        };
        match crate::onboarding::config::write_config_with_backup(&path, &merged) {
            Ok(()) => {
                self.status = Some(format!("saved {} (previous backed up)", path.display()));
            }
            Err(e) => {
                self.status = Some(format!("save failed: {e}"));
            }
        }
    }

    /// Validate the current edits against the selected provider's descriptor.
    fn validate(&self) -> Result<(), String> {
        // COR-007 / SEC-002: mirror the daemon's `remote_config_from_config`
        // check — the control plane must bind a DISTINCT loopback port the tunnel
        // never targets, so a shared gateway/control port is rejected before any
        // write (otherwise the panel could persist a config the daemon refuses).
        if self.edits.control_port == self.edits.gateway_port {
            return Err(format!(
                "control port ({}) must differ from the gateway port ({}) so the control plane \
                 stays off the tunnel (SEC-002)",
                self.edits.control_port, self.edits.gateway_port
            ));
        }
        if let Some(token) = self.edits.auth_token.as_ref() {
            lucarne_termgw::AccessToken::from_secret_validated(token.clone())
                .map_err(|e| format!("auth_token: {e}"))?;
        }
        if let Some(token) = self.edits.readonly_token.as_ref() {
            lucarne_termgw::AccessToken::from_secret_validated(token.clone())
                .map_err(|e| format!("readonly_token: {e}"))?;
        }
        let registry = lucarne_remote::builtin();
        let provider = registry
            .get(&self.edits.provider)
            .ok_or_else(|| format!("unknown provider `{}`", self.edits.provider))?;
        let mut cfg = ProviderConfig::new();
        for (k, v) in &self.edits.fields {
            if !v.is_empty() {
                cfg.fields.insert(k.clone(), v.clone());
            }
        }
        provider.validate_config(&cfg)
    }
}

/// Build the form rows for `provider`: the panel-owned top-level keys followed by
/// one row per descriptor [`RequiredField`]. The field rows come ENTIRELY from
/// `required_fields()` — the panel never names a concrete provider field.
pub fn build_rows(provider: &str) -> Vec<Row> {
    let mut rows = vec![
        Row::Enabled,
        Row::Provider,
        Row::GatewayPort,
        Row::ControlPort,
        Row::AuthToken,
        Row::ReadonlyToken,
        Row::Insecure,
    ];
    let registry = lucarne_remote::builtin();
    if let Some(p) = registry.get(provider) {
        for field in p.required_fields() {
            rows.push(Row::Field {
                key: field.key,
                label: field.label,
                secret: field.secret,
                required: field.required,
            });
        }
    }
    rows
}

/// Merge `edits` into the parsed `lucarned.yaml` `raw`, PRESERVING every unrelated
/// key/section, and return the re-serialized YAML. Only the `remote` top-level
/// fields owned by the panel and the selected provider's
/// `remote.providers.<id>.*` field map are touched; empty field values are
/// removed so a blanked secret never lingers. An empty/blank `raw` starts from
/// an empty mapping (so a missing file is created with only the remote section).
pub fn merge_config_yaml(raw: &str, edits: &ConfigEdits) -> Result<String, serde_yaml::Error> {
    let mut value: serde_yaml::Value = if raw.trim().is_empty() {
        serde_yaml::Value::Mapping(serde_yaml::Mapping::new())
    } else {
        serde_yaml::from_str(raw)?
    };
    let root = ensure_mapping(&mut value);
    let remote = ensure_child_mapping(root, "remote");
    remote.insert(
        serde_yaml::Value::String("enabled".to_string()),
        serde_yaml::Value::Bool(edits.enabled),
    );
    remote.insert(
        serde_yaml::Value::String("provider".to_string()),
        serde_yaml::Value::String(edits.provider.clone()),
    );
    remote.insert(
        serde_yaml::Value::String("gateway_addr".to_string()),
        serde_yaml::Value::String(format!("127.0.0.1:{}", edits.gateway_port)),
    );
    remote.insert(
        serde_yaml::Value::String("control_addr".to_string()),
        serde_yaml::Value::String(format!("127.0.0.1:{}", edits.control_port)),
    );
    set_optional_string(remote, "auth_token", edits.auth_token.as_deref());
    set_optional_string(remote, "readonly_token", edits.readonly_token.as_deref());
    remote.insert(
        serde_yaml::Value::String("insecure".to_string()),
        serde_yaml::Value::Bool(edits.insecure),
    );
    let providers = ensure_child_mapping(remote, "providers");
    let provider_map = ensure_child_mapping(providers, edits.provider.as_str());
    // For every key the selected provider's descriptor advertises, the edit state
    // is the source of truth: present (non-empty) → upsert; absent/empty → remove
    // (so a cleared secret disappears rather than lingering). Non-descriptor keys
    // the user may have set manually are left untouched.
    let descriptor_keys: Vec<&'static str> = lucarne_remote::builtin()
        .get(edits.provider.as_str())
        .map(|p| p.required_fields().iter().map(|f| f.key).collect())
        .unwrap_or_default();
    for key in descriptor_keys {
        let yaml_key = serde_yaml::Value::String(key.to_string());
        match edits.fields.get(key) {
            Some(val) if !val.is_empty() => {
                provider_map.insert(yaml_key, serde_yaml::Value::String(val.clone()));
            }
            _ => {
                provider_map.remove(&yaml_key);
            }
        }
    }
    // Also upsert any extra edited fields that are not descriptor keys (defensive;
    // the panel only ever edits descriptor-driven rows, but keep edits authoritative).
    for (key, val) in &edits.fields {
        if val.is_empty() {
            continue;
        }
        let yaml_key = serde_yaml::Value::String(key.clone());
        provider_map.insert(yaml_key, serde_yaml::Value::String(val.clone()));
    }
    serde_yaml::to_string(&value)
}

fn set_optional_string(mapping: &mut serde_yaml::Mapping, key: &str, value: Option<&str>) {
    let yaml_key = serde_yaml::Value::String(key.to_string());
    match value.filter(|v| !v.is_empty()) {
        Some(value) => {
            mapping.insert(yaml_key, serde_yaml::Value::String(value.to_string()));
        }
        None => {
            mapping.remove(&yaml_key);
        }
    }
}

/// Look up a child of a YAML mapping by string key.
fn mapping_get<'a>(value: &'a serde_yaml::Value, key: &str) -> Option<&'a serde_yaml::Value> {
    value
        .as_mapping()?
        .get(serde_yaml::Value::String(key.to_string()))
}

/// Clear `fields` and re-seed it from `remote.providers.<provider>.*` of a parsed
/// `lucarned.yaml` `value`: opaque `key → value` pairs, never interpreted here.
/// Empty values are dropped. Shared by the initial seed and the provider-change
/// re-seed (COR-001) so both restore the same persisted field set.
fn seed_fields_for_provider(
    value: &serde_yaml::Value,
    provider: &str,
    fields: &mut BTreeMap<String, String>,
) {
    fields.clear();
    if let Some(map) = mapping_get(value, "remote")
        .and_then(|r| mapping_get(r, "providers"))
        .and_then(|p| mapping_get(p, provider))
        .and_then(serde_yaml::Value::as_mapping)
    {
        for (k, v) in map {
            if let (Some(k), Some(v)) = (k.as_str(), v.as_str()) {
                if !v.is_empty() {
                    fields.insert(k.to_string(), v.to_string());
                }
            }
        }
    }
}

/// Parse the port out of a `host:port` address string.
fn port_of_addr(addr: &str) -> Option<u16> {
    addr.rsplit_once(':').and_then(|(_, p)| p.parse().ok())
}

/// Force `value` to be a mapping (replacing a non-mapping), returning it.
fn ensure_mapping(value: &mut serde_yaml::Value) -> &mut serde_yaml::Mapping {
    if !matches!(value, serde_yaml::Value::Mapping(_)) {
        *value = serde_yaml::Value::Mapping(serde_yaml::Mapping::new());
    }
    match value {
        serde_yaml::Value::Mapping(mapping) => mapping,
        _ => unreachable!("value was forced to mapping"),
    }
}

/// Ensure `mapping[key]` is a child mapping (creating/replacing as needed),
/// returning it. Preserves an existing child mapping's contents.
fn ensure_child_mapping<'a>(
    mapping: &'a mut serde_yaml::Mapping,
    key: &str,
) -> &'a mut serde_yaml::Mapping {
    let yaml_key = serde_yaml::Value::String(key.to_string());
    if !matches!(mapping.get(&yaml_key), Some(serde_yaml::Value::Mapping(_))) {
        mapping.insert(
            yaml_key.clone(),
            serde_yaml::Value::Mapping(serde_yaml::Mapping::new()),
        );
    }
    match mapping.get_mut(&yaml_key) {
        Some(serde_yaml::Value::Mapping(child)) => child,
        _ => unreachable!("child was forced to mapping"),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn base_edits() -> ConfigEdits {
        ConfigEdits {
            enabled: false,
            provider: "cloudflared".to_string(),
            gateway_port: 7800,
            control_port: 7801,
            fields: BTreeMap::new(),
            auth_token: None,
            readonly_token: None,
            insecure: false,
        }
    }

    #[test]
    fn rows_built_from_provider_required_fields() {
        // The form is driven ENTIRELY by the descriptor: top-level keys + one row
        // per required_fields() entry. We assert the descriptor's keys appear as
        // Field rows without the panel naming any concrete provider field itself.
        let provider = "cloudflared";
        let rows = build_rows(provider);
        // First rows are panel-owned daemon-config rows.
        assert_eq!(rows[0], Row::Enabled);
        assert_eq!(rows[1], Row::Provider);
        assert_eq!(rows[2], Row::GatewayPort);
        assert_eq!(rows[3], Row::ControlPort);
        assert_eq!(rows[4], Row::AuthToken);
        assert_eq!(rows[5], Row::ReadonlyToken);
        assert_eq!(rows[6], Row::Insecure);

        // The remaining rows must match the provider descriptor exactly (count +
        // each key/label/secret/required), proving they come from required_fields().
        let registry = lucarne_remote::builtin();
        let descriptor = registry.get(provider).expect("cloudflared registered");
        let field_rows: Vec<&Row> = rows.iter().skip(7).collect();
        assert_eq!(field_rows.len(), descriptor.required_fields().len());
        for (row, field) in field_rows.iter().zip(descriptor.required_fields()) {
            match row {
                Row::Field {
                    key,
                    label,
                    secret,
                    required,
                } => {
                    assert_eq!(*key, field.key);
                    assert_eq!(*label, field.label);
                    assert_eq!(*secret, field.secret);
                    assert_eq!(*required, field.required);
                }
                other => panic!("expected a Field row, got {other:?}"),
            }
        }
        // At least one secret field exists in the descriptor (the token) so the
        // masking test below is meaningful.
        assert!(
            descriptor.required_fields().iter().any(|f| f.secret),
            "descriptor should advertise a secret field"
        );
    }

    #[test]
    fn secret_field_value_is_masked_never_echoed() {
        // A set secret renders masked (fixed token, not the value, not its length).
        let secret_value = "super-secret-tunnel-token";
        assert_eq!(mask_value(secret_value, true), "••••");
        assert!(!mask_value(secret_value, true).contains("secret"));
        // An empty secret shows the unset marker, not an empty masked string.
        assert_eq!(mask_value("", true), "(unset)");
        // Non-secret values are shown verbatim (and unset when empty).
        assert_eq!(
            mask_value("https://t.example.com", false),
            "https://t.example.com"
        );
        assert_eq!(mask_value("", false), "(unset)");

        // End-to-end through display_value: a secret field's display must not leak
        // the raw value.
        let mut edits = base_edits();
        edits
            .fields
            .insert("token".to_string(), secret_value.to_string());
        edits.auth_token = Some(secret_value.to_string());
        let secret_row = Row::Field {
            key: "token",
            label: "Cloudflare Tunnel Token",
            secret: true,
            required: false,
        };
        let shown = display_value(&secret_row, &edits);
        assert_eq!(shown, "••••");
        assert!(!shown.contains(secret_value));
        assert_eq!(display_value(&Row::AuthToken, &edits), "••••");
    }

    #[test]
    fn merge_preserves_unrelated_keys_and_writes_edited_provider_field() {
        // A config with unrelated channels + state sections that must survive a
        // merge untouched, plus a pre-existing remote provider field that should
        // be updated in place.
        let raw = r#"
agents:
  - codex
  - pi
state:
  db: ~/.lucarned/state.sqlite3
remote:
  enabled: true
  provider: cloudflared
  auth_token: keep-this-token
  providers:
    cloudflared:
      token: old-token
      binary_path: /usr/local/bin/cloudflared
channels:
  telegram:
    enabled: true
    token: keep-telegram-token
"#;

        let mut edits = base_edits();
        edits.enabled = false;
        edits.gateway_port = 7900;
        edits.control_port = 7901;
        edits.auth_token = Some("0123456789abcdef0123456789abcdef".to_string());
        edits.readonly_token = Some("readonly-0123456789abcdef0123456789".to_string());
        edits.insecure = true;
        edits
            .fields
            .insert("token".to_string(), "new-token".to_string());
        edits.fields.insert(
            "public_url".to_string(),
            "https://tunnel.example.com".to_string(),
        );
        // binary_path was seeded from the file and left unchanged → it stays in the
        // edit set (the panel always saves the full edited field set), so it must be
        // preserved on merge.
        edits.fields.insert(
            "binary_path".to_string(),
            "/usr/local/bin/cloudflared".to_string(),
        );

        let merged = merge_config_yaml(raw, &edits).expect("merge yaml");
        let value: serde_json::Value = serde_yaml::from_str(&merged).expect("parse merged");

        // Unrelated sections survive verbatim.
        assert_eq!(
            value.pointer("/state/db").and_then(|v| v.as_str()),
            Some("~/.lucarned/state.sqlite3")
        );
        assert_eq!(
            value
                .pointer("/channels/telegram/token")
                .and_then(|v| v.as_str()),
            Some("keep-telegram-token")
        );
        assert_eq!(
            value.pointer("/remote/auth_token").and_then(|v| v.as_str()),
            Some("0123456789abcdef0123456789abcdef")
        );
        assert_eq!(
            value
                .pointer("/remote/readonly_token")
                .and_then(|v| v.as_str()),
            Some("readonly-0123456789abcdef0123456789")
        );
        assert_eq!(
            value.pointer("/remote/insecure").and_then(|v| v.as_bool()),
            Some(true)
        );
        assert_eq!(
            value.pointer("/remote/enabled").and_then(|v| v.as_bool()),
            Some(false)
        );

        // Edited top-level keys + ports.
        assert_eq!(
            value.pointer("/remote/provider").and_then(|v| v.as_str()),
            Some("cloudflared")
        );
        assert_eq!(
            value
                .pointer("/remote/gateway_addr")
                .and_then(|v| v.as_str()),
            Some("127.0.0.1:7900")
        );
        assert_eq!(
            value
                .pointer("/remote/control_addr")
                .and_then(|v| v.as_str()),
            Some("127.0.0.1:7901")
        );

        // The edited provider field is written; an unedited pre-existing field is
        // preserved; the newly-added field is present.
        assert_eq!(
            value
                .pointer("/remote/providers/cloudflared/token")
                .and_then(|v| v.as_str()),
            Some("new-token")
        );
        assert_eq!(
            value
                .pointer("/remote/providers/cloudflared/binary_path")
                .and_then(|v| v.as_str()),
            Some("/usr/local/bin/cloudflared")
        );
        assert_eq!(
            value
                .pointer("/remote/providers/cloudflared/public_url")
                .and_then(|v| v.as_str()),
            Some("https://tunnel.example.com")
        );
    }

    #[test]
    fn merge_round_trip_writes_to_temp_file_via_backup() {
        // Round-trip the full save path against a TEMP file (never ~/.lucarned):
        // an existing config is backed up and atomically replaced with the merged
        // contents carrying the edited provider field.
        let temp = tempfile::tempdir().expect("temp dir");
        let path = temp.path().join("lucarned.yaml");
        std::fs::write(
            &path,
            "remote:\n  provider: cloudflared\n  providers:\n    cloudflared:\n      token: old\n",
        )
        .expect("write initial config");

        let mut edits = base_edits();
        edits
            .fields
            .insert("token".to_string(), "rotated-token".to_string());

        let merged = merge_config_yaml(
            &std::fs::read_to_string(&path).expect("read existing"),
            &edits,
        )
        .expect("merge");
        crate::onboarding::config::write_config_with_backup(&path, &merged)
            .expect("write with backup");

        // The file now carries the rotated token.
        let written = std::fs::read_to_string(&path).expect("read written");
        let value: serde_json::Value = serde_yaml::from_str(&written).expect("parse written");
        assert_eq!(
            value
                .pointer("/remote/providers/cloudflared/token")
                .and_then(|v| v.as_str()),
            Some("rotated-token")
        );

        // A backup of the previous contents was created (the old token).
        let backups: Vec<String> = std::fs::read_dir(temp.path())
            .expect("read temp dir")
            .filter_map(|e| e.ok())
            .map(|e| e.file_name().to_string_lossy().into_owned())
            .filter(|n| n.starts_with("lucarned.yaml.bak-"))
            .collect();
        assert_eq!(
            backups.len(),
            1,
            "exactly one backup of the previous config"
        );
        let backup = std::fs::read_to_string(temp.path().join(&backups[0])).expect("read backup");
        assert!(
            backup.contains("token: old"),
            "backup must hold the previous config, got: {backup}"
        );
    }

    #[test]
    fn empty_field_is_removed_from_provider_map_on_merge() {
        // A blanked field must NOT linger in the YAML (so a cleared secret is gone).
        let raw = "remote:\n  providers:\n    cloudflared:\n      token: old\n";
        let edits = base_edits(); // token edited to empty → dropped
        let merged = merge_config_yaml(raw, &edits).expect("merge");
        let value: serde_json::Value = serde_yaml::from_str(&merged).expect("parse");
        assert!(
            value
                .pointer("/remote/providers/cloudflared/token")
                .is_none(),
            "an empty/blanked field must be removed from the merged config"
        );
    }

    #[test]
    fn cycle_provider_rebuilds_descriptor_rows() {
        // With a single built-in provider, cycling wraps back to it and rebuilds
        // the rows from its descriptor (proving rows always follow the selection).
        let mut panel = ConfigPanel::new();
        let before = panel.rows.clone();
        let provider_idx = panel
            .rows
            .iter()
            .position(|row| matches!(row, Row::Provider))
            .expect("provider row");
        panel.list.select(Some(provider_idx));
        panel.handle_key(KeyCode::Enter); // cycles provider
        assert_eq!(panel.rows, build_rows(panel.edits.provider.as_str()));
        // Rows are non-empty and start with the panel-owned keys.
        assert_eq!(panel.rows[0], Row::Enabled);
        // Single provider → same row shape after wrap.
        assert_eq!(panel.rows.len(), before.len());
    }

    #[test]
    fn cycle_provider_reseeds_persisted_fields_and_save_preserves_them() {
        // COR-001 regression: cycling the provider must RE-SEED the newly selected
        // provider's persisted fields from `lucarned.yaml` rather than blanking
        // them — otherwise a cycle (even one wrapping back to the same provider)
        // followed by a save would wipe the provider's on-disk values.
        let temp = tempfile::tempdir().expect("temp dir");
        let path = temp.path().join("lucarned.yaml");
        std::fs::write(
            &path,
            "remote:\n  provider: cloudflared\n  providers:\n    cloudflared:\n      \
             token: original-token\n      public_url: https://named.example.test\n",
        )
        .expect("write initial config");

        // Build a panel pointed at the temp file and seed it from disk (mirroring
        // what `load()` does after reading — without resolving the real
        // ~/.lucarned path).
        let mut panel = ConfigPanel::new();
        panel.config_path = Some(path.clone());
        let raw = std::fs::read_to_string(&path).expect("read seed");
        panel.seed_from_yaml(&raw);
        panel.reload_rows();
        assert_eq!(
            panel.edits.fields.get("token").map(String::as_str),
            Some("original-token"),
            "fields seeded from the file on load"
        );

        // Cycle the provider (single built-in → wraps back to cloudflared). The
        // pre-fix behavior cleared `fields` here; the fix re-seeds from the file.
        let provider_idx = panel
            .rows
            .iter()
            .position(|row| matches!(row, Row::Provider))
            .expect("provider row");
        panel.list.select(Some(provider_idx));
        panel.handle_key(KeyCode::Enter); // cycles provider → re-seeds
        assert_eq!(
            panel.edits.fields.get("token").map(String::as_str),
            Some("original-token"),
            "cycling must re-seed persisted fields, not blank them (COR-001)"
        );
        assert_eq!(
            panel.edits.fields.get("public_url").map(String::as_str),
            Some("https://named.example.test")
        );

        // Saving now must preserve the provider's persisted fields.
        panel.save();
        let written = std::fs::read_to_string(&path).expect("read written");
        let value: serde_json::Value = serde_yaml::from_str(&written).expect("parse written");
        assert_eq!(
            value
                .pointer("/remote/providers/cloudflared/token")
                .and_then(|v| v.as_str()),
            Some("original-token"),
            "a cycle then save must not wipe the provider's persisted token (COR-001)"
        );
        assert_eq!(
            value
                .pointer("/remote/providers/cloudflared/public_url")
                .and_then(|v| v.as_str()),
            Some("https://named.example.test")
        );
    }

    #[test]
    fn save_rejects_equal_gateway_and_control_ports() {
        // COR-007 / SEC-002: a control port equal to the gateway port must be
        // rejected inline (status line) and NOT written — mirroring the daemon's
        // `remote_config_from_config` distinctness check.
        let temp = tempfile::tempdir().expect("temp dir");
        let path = temp.path().join("lucarned.yaml");
        let mut panel = ConfigPanel::new();
        panel.config_path = Some(path.clone());
        // Force the two ports to collide.
        panel.edits.gateway_port = 7800;
        panel.edits.control_port = 7800;

        panel.save();
        // The error is surfaced inline and references the SEC-002 rule.
        let status = panel.status.as_deref().unwrap_or("");
        assert!(
            status.contains("validation failed") && status.contains("SEC-002"),
            "equal ports must surface a SEC-002 validation error, got: {status}"
        );
        // Nothing was written (the file was never created).
        assert!(
            !path.exists(),
            "an invalid (equal-port) config must NOT be written"
        );

        // A distinct control port passes the port check and writes.
        panel.edits.control_port = 7801;
        panel.save();
        assert!(path.exists(), "a valid config writes");
    }

    #[test]
    fn inline_edit_commits_non_secret_field_and_parses_port() {
        let mut panel = ConfigPanel::new();
        // Select the gateway-port row and edit it.
        let gateway_idx = panel
            .rows
            .iter()
            .position(|row| matches!(row, Row::GatewayPort))
            .expect("gateway port row");
        panel.list.select(Some(gateway_idx));
        panel.handle_key(KeyCode::Enter); // begin edit
        assert!(panel.editing.is_some());
        for c in "9100".chars() {
            panel.handle_key(KeyCode::Char(c));
        }
        panel.handle_key(KeyCode::Enter); // commit
        assert_eq!(panel.edits.gateway_port, 9100);
        assert!(panel.editing.is_none());

        // An invalid port is rejected with a status message (value unchanged).
        panel.handle_key(KeyCode::Enter);
        for c in "notaport".chars() {
            panel.handle_key(KeyCode::Char(c));
        }
        panel.handle_key(KeyCode::Enter);
        assert_eq!(panel.edits.gateway_port, 9100);
        assert!(panel
            .status
            .as_deref()
            .unwrap_or("")
            .contains("invalid port"));
    }

    #[test]
    fn top_level_remote_fields_seed_toggle_validate_and_merge() {
        let raw = r#"
remote:
  enabled: true
  provider: cloudflared
  gateway_addr: 127.0.0.1:7900
  control_addr: 127.0.0.1:7901
  auth_token: "0123456789abcdef0123456789abcdef"
  readonly_token: "readonly-0123456789abcdef0123456789"
  insecure: true
  providers:
    cloudflared:
      public_url: https://named.example.test
"#;
        let mut panel = ConfigPanel::new();
        panel.seed_from_yaml(raw);
        panel.reload_rows();

        assert!(panel.edits.enabled);
        assert_eq!(panel.edits.gateway_port, 7900);
        assert_eq!(panel.edits.control_port, 7901);
        assert_eq!(
            panel.edits.auth_token.as_deref(),
            Some("0123456789abcdef0123456789abcdef")
        );
        assert_eq!(
            panel.edits.readonly_token.as_deref(),
            Some("readonly-0123456789abcdef0123456789")
        );
        assert!(panel.edits.insecure);

        let enabled_idx = panel
            .rows
            .iter()
            .position(|row| matches!(row, Row::Enabled))
            .expect("enabled row");
        panel.list.select(Some(enabled_idx));
        panel.handle_key(KeyCode::Enter);
        assert!(!panel.edits.enabled, "Enter toggles autostart");

        let insecure_idx = panel
            .rows
            .iter()
            .position(|row| matches!(row, Row::Insecure))
            .expect("insecure row");
        panel.list.select(Some(insecure_idx));
        panel.handle_key(KeyCode::Enter);
        assert!(!panel.edits.insecure, "Enter toggles insecure");

        panel.edits.auth_token = Some("short".to_string());
        let err = panel.validate().expect_err("weak auth token is rejected");
        assert!(
            err.contains("auth_token"),
            "token error should name field: {err}"
        );
        panel.edits.auth_token = Some("0123456789abcdef0123456789abcdef".to_string());
        panel.edits.readonly_token = Some("short".to_string());
        let err = panel
            .validate()
            .expect_err("weak readonly token is rejected");
        assert!(
            err.contains("readonly_token"),
            "readonly token error should name field: {err}"
        );
        panel.edits.readonly_token = None;

        let merged = merge_config_yaml(raw, &panel.edits).expect("merge");
        let value: serde_json::Value = serde_yaml::from_str(&merged).expect("parse");
        assert_eq!(
            value.pointer("/remote/enabled").and_then(|v| v.as_bool()),
            Some(false)
        );
        assert_eq!(
            value.pointer("/remote/insecure").and_then(|v| v.as_bool()),
            Some(false)
        );
        assert_eq!(
            value.pointer("/remote/auth_token").and_then(|v| v.as_str()),
            Some("0123456789abcdef0123456789abcdef")
        );
        assert!(
            value.pointer("/remote/readonly_token").is_none(),
            "cleared read-only token must be removed"
        );
    }

    #[test]
    fn secret_field_edit_buffer_captures_but_display_masks() {
        // Editing a secret field captures chars into the raw buffer (so it commits
        // correctly) while the rendered value stays masked — the secret is never
        // echoed via the panel's display path.
        let mut panel = ConfigPanel::new();
        // Find the secret token field row.
        let token_idx = panel
            .rows
            .iter()
            .position(|r| matches!(r, Row::Field { secret: true, .. }))
            .expect("a secret field row");
        panel.list.select(Some(token_idx));
        panel.handle_key(KeyCode::Enter);
        for c in "tok123".chars() {
            panel.handle_key(KeyCode::Char(c));
        }
        panel.handle_key(KeyCode::Enter); // commit
        let row = panel.rows[token_idx].clone();
        assert_eq!(panel.row_display(&row), "••••");
        // The raw value is stored for save, but never surfaced via display.
        if let Row::Field { key, .. } = row {
            assert_eq!(
                panel.edits.fields.get(key).map(String::as_str),
                Some("tok123")
            );
        }
    }

    #[test]
    fn start_params_returns_provider_and_non_empty_fields() {
        // PART 1: the Go-Public `s` key starts with the Config panel's live edits.
        // `start_params` returns the selected provider id + only its NON-EMPTY
        // field values (empties are skipped so they never shadow a daemon default).
        let mut panel = ConfigPanel::new();
        panel.edits.provider = "cloudflared".to_string();
        panel
            .edits
            .fields
            .insert("token".to_string(), "tok-123".to_string());
        // A blank field must be skipped, not forwarded as "".
        panel
            .edits
            .fields
            .insert("public_url".to_string(), String::new());

        let (provider, fields) = panel.start_params();
        assert_eq!(provider, "cloudflared");
        assert_eq!(fields.get("token").map(String::as_str), Some("tok-123"));
        assert!(
            !fields.contains_key("public_url"),
            "empty field values must be skipped"
        );
    }
}
