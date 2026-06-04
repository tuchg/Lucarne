# remote-access tunnel adapter

## Context

Lucarne's terminal-monitor subsystem (see
`2026-05-30-rmux-terminal-monitor-subsystem.md`) mirrors live rmux terminal
sessions to a web client through `lucarne-termgw`. Today that gateway binds a
local address with **zero authentication** and is reachable only on the LAN.

A new capability is required: expose that gateway to the **public internet** so a
user can reach their terminal mirror + agent chat from a phone, away from home.
This is structurally the highest-severity surface in the whole project — the web
client mirrors a live terminal **and feeds keystrokes back in**, so an
unauthenticated public endpoint is effectively remote code execution.

The first concrete exposure mechanism is Cloudflare Tunnel (`cloudflared`), but
the design must not hardcode it: FRP, a self-hosted lightweight relay, and other
NAT-traversal tunnels are anticipated. We need a transport-agnostic seam plus a
hardened authentication story that is correct by default.

## Decision

Add a pluggable **remote-access tunnel adapter** as a rmux-free crate
`lucarne-remote`, wired into `lucarned` behind explicit capability features.
The source default build remains the base daemon. The `remote-access` feature
adds the tunnel provider abstraction, while `terminal-gateway` selects the
terminal gateway as the exposed capability and `product-terminal` is the release
bundle. `remote.enabled` controls autostart behavior after that capability is
built. The design is captured as nine locked decisions (L1–L9).

### L1 — Adapter abstraction mirrors `AdapterPlugin`/`AdapterRegistry`

`lucarne-remote` defines a `RemoteAccessProvider` trait
(`id`/`name`/`required_fields`/`start`/`stop`/`health`) plus a `RemoteRegistry`
(register / lookup / enumerate built-ins). This deliberately mirrors the proven
`AdapterPlugin`/`AdapterRegistry` pattern in
`lucarne-adapter/src/lib.rs:575-603`: a `Send + Sync` async trait plus a registry
that registers, looks up, and enumerates implementations. A new backend is one
trait impl in its own module plus one `RemoteRegistry::register` call — zero core
changes. Only **Cloudflared** is implemented this release.

### L2 — Adapter boundary = pure tunnel; auth never enters a provider

A `RemoteAccessProvider` is a **pure tunnel**: `start(local_addr, cfg) ->
TunnelHandle { public_url }`, `stop`, and `health`. Authentication,
authorization, and any token/ticket exchange live in the gateway/web layer and
never leak into a provider. This keeps providers single-responsibility and
honors the AGENTS.md provider-boundary rule (provider-specific logic stays at the
provider boundary; common layers orchestrate through typed contracts only).

### L3 — Gateway always binds loopback; the tunnel dials back in

In public mode the gateway still binds only `127.0.0.1:<port>` (default
`127.0.0.1:7800`); `cloudflared` connects outbound to the Cloudflare edge and the
edge dials back into the local port. The daemon **never binds `0.0.0.0`**. This
reuses the health subsystem's loopback precedent (`health.rs parse_health_addr`
rejects non-loopback); the gateway bind is hardened through
`lucarne-termgw::parse_gateway_addr`. Even if auth had a bug, there is no
directly reachable public socket — the only ingress is the authenticated tunnel.

### L4 — Default-deny: public exposure forces authentication

Enabling `remote` requires an access token. When none is configured, one is
auto-generated at startup and enforced. Exposing the gateway with auth disabled
is **refused** unless the operator explicitly opts out via `insecure: true` (or
`--insecure`), which prints a loud warning. There is no path to "forgot to turn
on auth and went naked."

### L5 — Auth transport: Bearer for `/api`, single-use short-TTL ticket for ws

`/api/*` requests carry the long-lived access token as `Authorization: Bearer`.
Browsers cannot set custom headers on a WebSocket and putting the token in the ws
URL leaks it into logs, so ws auth uses a **token → single-use ticket → upgrade**
exchange: the client first calls `POST /auth/ticket` with the Bearer token to
mint a single-use, short-TTL ticket (seed ~30s), and the ws handler validates and
consumes that ticket before `.on_upgrade()`. Comparisons are **constant-time**;
failures are rate-limited with a temporary lockout (seed ~5 failures → ~60s),
mirroring the existing wechat `rate_limit` discipline. These seed values are
fixed runtime constants, not file-config fields.

### L6 — daemon owns the tunnel lifecycle; CLI is a thin loopback control client

`lucarned` owns the tunnel so it survives CLI exit and shuts down with the
daemon, mirroring the health subsystem wiring. `lucarned remote
start|stop|status` is a thin headless loopback control client, and `lucarned
tui` provides the interactive Go Public panel. Both drive the daemon over a
loopback-only control API (`POST /api/remote/start`, etc.) and render the
returned public URL + credentials (QR via the existing `qrcode` crate).
Lifecycle ownership is unambiguous and the control plane stays on loopback.

### L7 — Reserved backends are trait seams only

FRP, a self-hosted lightweight relay, and other NAT-traversal tunnels are
**reserved**: they are represented as trait seams plus commented `provider`
placeholders in `lucarned.yaml`, and are **not implemented** this release. They
will later implement `RemoteAccessProvider` in their own modules and register
through `RemoteRegistry` with zero changes to the core.

### L8 — Engineering discipline per AGENTS.md

Nightly toolchain with `cargo -Zbuild-dir-new-layout`; provider-specific logic
stays in the provider module and does not pollute common/core layers. The remote
provider seam remains isolated, while the user-facing remote/TUI/gateway entry
is directly fused into the default `lucarned` product build.

### L9 — Definition of done includes real-device reachability

Done requires a real `cloudflared` tunnel running on the host plus a phone
reaching the public URL and authenticating against the live `termgw` terminal
mirror — not just a green build.

## Rationale

The `AdapterPlugin`/`AdapterRegistry` pattern is already proven in this workspace
(filter-enabled → sort priority → spawn), so reusing its shape for tunnels gives
a known-good seam and a registry that naturally supports "pick a built-in backend"
in the CLI and `enable` in config. Keeping the provider a pure tunnel matches both
the single-responsibility principle and the locked transport-agnostic boundary,
and keeps backend churn (e.g. cloudflared CLI quirks) confined to one module.

Loopback-only binding plus default-deny auth is defense in depth: the network
shape removes the directly-reachable socket, and the auth layer removes
unauthenticated access, so a failure in either alone is not catastrophic. The
ws ticket exchange specifically avoids the classic pitfall of long-lived
credentials leaking into ws URLs and access logs, and single-use + rate-limiting
resist replay and brute force.

## Consequences

- The default source `lucarned` build stays a base daemon and does not link
  `lucarne-remote`, `lucarne-termgw`, or `lucarne-rmux`. Release packaging opts
  into the `product-terminal` bundle so installed users still get the terminal
  remote-access product in the single `lucarned` binary.
- `lucarned.yaml` gains a `remote:` section
  (`enabled` as autostart, `provider`, `gateway_addr`, `control_addr`,
  `auth_token`, `readonly_token`, `insecure`) plus a GENERIC,
  transport-agnostic `providers:` map keyed by provider id — e.g.
  `providers: { cloudflared: { token, public_url, binary_path } }` (H6c). The
  daemon passes the selected provider's field map straight through to the
  provider without interpreting any field name, so a new backend is purely a
  `providers.<id>` block. Deprecated compatibility sections such as
  `remote.cloudflare:` are provider-declared aliases, not daemon-owned concrete
  structs; they are merged before `providers.<id>` so the generic map wins per
  key. Commented reserved-backend placeholders (`frp`/`relay`) point here.
- Providers self-describe their config rules through the trait, not the daemon:
  `RequiredField::required_when` expresses a conditional requirement (e.g.
  cloudflared's `public_url` is required-when a named-tunnel `token` is set —
  M7), and `RemoteAccessProvider::validate_config` enforces cross-field /
  format rules at config-resolution time. The daemon calls `validate_config`
  before `start` and surfaces a violation as a typed `BadConfig` (400); it never
  branches on a concrete provider id (AGENTS.md boundary).
- A named-tunnel token is handed to `cloudflared` via a `0600` `--token-file`
  (removed when the tunnel stops) rather than `--token` on argv, so the secret
  is not visible in the process command line (`ps` / `/proc/<pid>/cmdline`) —
  L3. It remains readable by the same local user while the tunnel runs (a
  same-user attacker is already inside the daemon's trust boundary).
- The web client and `termgw` gain an auth layer (Bearer + ticket exchange);
  `lucarned remote` and the `lucarned tui` Go Public panel drive the loopback
  control API.
- Adding FRP / relay / other tunnels later is a localized change: one
  `RemoteAccessProvider` impl + one registry registration + a `providers.<id>`
  config block — no daemon-config or common-layer change.

### Residual risks (explicitly accepted this release)

- **Cloudflare edge sees plaintext.** Traffic is TLS to the CF edge, but the edge
  terminates that TLS and sees the tunnel plaintext. End-to-end payload
  encryption + pairing SAS (the M2-style model) is **deferred**; this release
  relies on CF edge TLS. Operators must understand the CF edge is in the trust
  path. (SEC-012) The daemon emits a loud `tracing::warn!` whenever a *quick*
  tunnel (no `token`) starts, recommending a **named tunnel**
  (`remote.providers.cloudflared.token` + `public_url`) for sensitive sessions.
- **Single shared access token.** The full-access token is shared, not per-user
  or per-session. A second, optional **read-only** credential
  (`remote.readonly_token`, SEC-013) is supported: its ws sessions may mirror
  terminals but are refused all write frames (input / create / close / agent
  prompts). Finer-grained per-user / per-session ACL is **deferred**.
- **cloudflared is an external binary.** The backend shells out to a
  separately-installed `cloudflared` binary, which is an external supply-chain
  dependency outside this repo's build.

## References

- `2026-05-30-rmux-terminal-monitor-subsystem.md` — the terminal-monitor +
  `termgw` subsystem this exposure surface sits on top of.
- `lucarne-adapter/src/lib.rs:575-603` — the `AdapterPlugin`/`AdapterRegistry`
  pattern mirrored by `RemoteAccessProvider`/`RemoteRegistry`.
- `2026-05-24-lazy-control-plane-state.md` — cold-store discipline referenced for
  any persisted remote credentials.
