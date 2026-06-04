# Lucarne TUI (`lucarned tui`)

An interactive, full-screen terminal dashboard for the rmux terminal-monitor
subsystem — opencode-style arrow-key navigation over the same thin operations the
daemon already exposes. It is the **single** interactive entry point for managing
mirrored rmux sessions and the public-access tunnel.

> **What changed:** the standalone `term` binary (`lucarne-termctl`) has been
> **removed**. Everything it did is now reachable through `lucarned tui`, and its
> reusable logic (control-plane calls, terminal QR rendering, rmux argv, archive,
> provider field collection) was migrated into `lucarned` — not rewritten.

---

## Build & launch

The TUI is part of the explicit `lucarned` terminal product bundle
(`product-terminal`). The default source build stays a minimal base daemon;
release packaging enables the bundle so the shipped daemon binary includes the
TUI, remote control plane, terminal gateway, and rmux live binding.

```bash
# from a release build
lucarned tui

# from source (AGENTS.md build discipline: nightly + new build-dir layout)
cargo +nightly run -Zbuild-dir-new-layout -p lucarned --features product-terminal -- tui
```

---

## Layout

```
┌─lucarned────────┐┌─<panel>──────────────────────────────────┐
│  Sessions       ││ <panel body: status / list / form>       │
│> Go Public      ││                                          │
│  Config         ││                                          │
└─────────────────┘└──────────────────────────────────────────┘
 Tab/←→ panel   <panel-specific keys>                  q quit     ← bottom hint bar
```

Left = panel list, right = the focused panel, bottom = a one-line hint bar that
always shows the active panel's keys. If the window is very short, enlarge it (or
reduce the font) so the hint bar is visible.

`Tab` / `←` `→` switch panels; `q` quits; the terminal is always restored on exit
(including on panic, via a process-level hook).

---

## Panels & keybindings

### Sessions — manage system rmux sessions

Lists live sessions on your **system rmux daemon** and acts on them by shelling the
native `rmux` CLI (no new IPC). Works standalone (no `lucarned` daemon needed).

| Key | Action |
|-----|--------|
| `↑` `↓` | Move selection |
| `Enter` | **Attach (pop-out):** suspends the TUI, hands the current terminal to `rmux attach-session`; on exit the TUI re-enters |
| `d` | Detach clients (the session keeps running) |
| `k` / `Del` | Kill the session |
| `a` | Archive: capture content into the shared store, then close |
| `r` | Refresh the list |

### Go Public — start/stop the public-access tunnel

Drives the daemon's loopback control plane (`/api/remote/{start,stop,status}` on
`127.0.0.1:7801` by default) and renders the login QR.

| Key | Action |
|-----|--------|
| `s` | **Start** remote access (go public) — uses the **Config panel's live fields** |
| `x` | Stop remote access |
| `r` | Refresh status |
| `Enter` | Show the login **QR** (when a tunnel is up) — scannable, high-contrast; falls back to the plain login URL if the terminal is too small |
| `Esc` | Close the QR modal |

> **`s` uses your in-TUI Config — no `lucarned.yaml` edit needed.** Pressing `s`
> starts the tunnel with the **provider + fields currently set in the Config
> panel** (its live edit buffer), not whatever is saved on disk. The workflow is:
> configure the provider/fields in **Config**, switch to **Go Public**, press `s`.
> The daemon merges those over its pre-configured tunnel, so you can "configure +
> go public" entirely inside the TUI without saving first. The Go Public body shows
> the source on a `start uses Config: provider=<id> (<n> fields)` line.
>
> If a provider IS set, its config is validated against the provider descriptor
> before sending; an invalid config (e.g. a missing required field) is surfaced
> inline (`start blocked — …`) and nothing is sent. If the **Config panel is empty**
> (no provider selected) the body shows `start uses daemon default (no provider set
> in Config)` and `s` falls back to the daemon's **pre-configured** tunnel
> (`lucarned.yaml`'s `remote:` section).

> **Requires the `lucarned` daemon to be running.** The daemon owns the tunnel
> lifecycle (it serves the loopback control plane); the TUI is a thin front-end and
> never opens a tunnel itself. If the daemon is not running, the panel shows an
> actionable hint instead of a raw error:
>
> ```
> control plane unreachable on 127.0.0.1:7801 — the lucarned daemon isn't running.
> Start it first (`lucarned autostart start`, or `brew services start lucarned`).
> ```
>
> Actually opening a tunnel also needs `cloudflared` installed/configured (see the
> `remote:` section of `lucarned.yaml`).

Cloudflare Quick Tunnel is the zero-config testing/development path used when
the Cloudflared token is blank. It creates an ephemeral `trycloudflare.com`
hostname and should not be treated as production availability. For sensitive or
repeatable access, configure a named tunnel with `token` and `public_url`.

End-to-end (two terminals):

```bash
# Terminal A — run the daemon. It serves the loopback control plane from boot;
# the tunnel starts lazily when the TUI sends start.
cargo +nightly run -Zbuild-dir-new-layout -p lucarned

# Terminal B — open the TUI, go to "Go Public", press `s`
cargo +nightly run -Zbuild-dir-new-layout -p lucarned -- tui
```

Release smoke checklist for public access:

```bash
cargo +nightly build -Zbuild-dir-new-layout -p lucarned
LUCARNE_QUICK_TUNNEL_E2E=1 scripts/remote-quick-tunnel-e2e.sh
```

The harness is env-gated and skips by default. When enabled, it starts a
temporary daemon, opens a Quick Tunnel, verifies public auth/read-only/router
isolation through the `trycloudflare.com` URL, calls `lucarned remote stop`, and
then tears the daemon down.

### Config — edit remote-access config

A provider-config editor driven entirely by the `lucarne_remote` provider
descriptors (no hardcoded provider/field names). Edits are written back to
`lucarned.yaml` with a timestamped backup and an atomic temp+rename; on unix the
config and backup are created `0o600`. Secret fields are masked on screen.

| Key | Action |
|-----|--------|
| `↑` `↓` | Move between fields |
| `Enter` | Edit the field / cycle the selected provider |
| `s` | Save (validates, e.g. rejects gateway port == control port) |
| `Esc` | Cancel the current edit |

---

## Architecture & boundaries

- **Single entry, one binary.** Only `lucarned` ships (the release installer
  packages `lucarned`); the TUI is `lucarned tui`. No second binary to install.
- **Zero new daemon IPC.** The three panels reuse existing surfaces only:
  the native `rmux` CLI + the shared archive store (Sessions), the loopback
  `/api/remote/*` control plane (Go Public), and `lucarne_remote::builtin()` +
  `write_config_with_backup` (Config).
- **Not a live mirror.** The TUI is a list/action console; it does not render live
  terminal content. The full interactive mirror lives in the web terminal view.
- **Provider boundary (AGENTS.md).** Provider specifics stay behind
  `lucarne_remote` descriptors; the TUI/common layers route opaque ids only.
- **Explicit capability features.** The source default build keeps
  `ratatui`/`crossterm`/`lucarne-termgw`/`lucarne-rmux` out of the base daemon.
  The `product-terminal` bundle opts into the terminal gateway, remote access,
  TUI, and rmux stack for release packaging.

See the decision record:
[`docs/decisions/2026-06-01-lucarned-tui-frontend.md`](decisions/2026-06-01-lucarned-tui-frontend.md).

## Migration from the old `term` CLI

| Old `term` command | Now |
|--------------------|-----|
| `term ls` | Sessions panel (list) |
| `term attach <id>` / `term enter <id>` | Sessions panel → `Enter` |
| `term detach <id>` | Sessions panel → `d` |
| `term kill <id>` | Sessions panel → `k` / `Del` |
| `term archive <id>` | Sessions panel → `a` |
| `term go-public` | Go Public panel → `s` |
