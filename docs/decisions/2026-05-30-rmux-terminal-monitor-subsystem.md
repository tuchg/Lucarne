# rmux terminal-monitor subsystem

## Context

Lucarne is a structured agent message bridge: it spawns agent CLIs over plain
pipes and parses their stdout as NDJSON/JSON-RPC. It has no terminal emulation,
no PTY, and no multiplexer integration.

A new capability is required: monitor the terminal sessions on the user's
system-wide rmux daemon (rmux is a daemon-backed, tmux-compatible multiplexer,
v0.3.1), mirror them faithfully and interactively to a web client, allow a
session to be attached locally (popped into the user's default terminal) and
detached again (retracted) while the remote mirror keeps running, and expose a
thin CLI to list / create / attach / detach / kill those sessions.

The load-bearing mechanic — an SDK control-mode observer coexisting with a CLI
`attach-session` client on the same session, with `detach-client` leaving the
session alive — was validated against the running daemon in
`.workflow/scratch/spike4-attach-handoff.md` (all claims PASS).

## Decision

Add the terminal-monitor as a NEW core subsystem, parallel to `AgentRuntime`,
with the terminal capability concentrated in `lucarne-rmux`.

- **`lucarne-rmux`** — the terminal capability package: stable terminal grid
  value types (`Cell`/`Color`/`Style`/`Cursor`/`PaneGrid`/delta), the
  self-authored snapshot differ (rmux exposes no native cell/row delta), the
  session registry, terminal input, terminal archive helpers, `adapter` (the sole
  place that maps `rmux_sdk` value types into Lucarne terminal types), and
  `monitor` (connect to the system daemon, adopt sessions, mirror panes, inject
  input). It is the ONLY crate that names `rmux_sdk`.

It is wired into the default `lucarned` product build so release users get the
TUI, terminal gateway, remote control, and rmux live binding from the single
shipped binary.

### Monitor model

The monitor connects to the DEFAULT system socket — the same daemon the user's
own `rmux` uses — and observes it. Discovered sessions register as
`Origin::Adopted` (we observe; we do not own them); sessions created via the CLI
register as `Origin::Managed`. The SDK is a control-mode observer that coexists
with a CLI `attach-session` client on the same session, so "pop a session into a
local terminal / retract it" is rmux-native (`attach-session` / `detach-client`)
and needs no new daemon IPC.

### Boundary rules

1. `rmux_sdk` names live only in `lucarne-rmux` (`adapter` + `monitor`). Preview
   API churn stops at that boundary; nothing else in the workspace deserializes
   or matches rmux types directly.
2. The subsystem is NOT an `agent-sessions` provider. That layer parses external
   transcript FILES; a live terminal pane is not a transcript and must not be
   forced through provider parse/discovery/watch contracts (AGENTS.md).
3. The subsystem is NOT routed through the agent framer/dialect pipeline.
   Terminal bytes (a cell grid) and structured agent events are different data
   shapes; reusing the NDJSON pipeline would mean ANSI scraping, which is
   rejected.
4. Any persisted session metadata is a cold record: it must use
   `ControlPlaneSqliteStore` cold read/write APIs, not the startup hot path
   (see `2026-05-24-lazy-control-plane-state.md`).
5. Terminal scrollback/history reads must be bounded windows, never whole-pane
   scans — consistent with the existing watch/history hot-path discipline.
6. The PTY is never force-resized; viewport changes are hints and the renderer
   scales, so multiple mirror clients never fight over pane size.

## Rationale

The terminal data shape is fundamentally incompatible with Lucarne's structured
agent pipeline, so reuse there is neither possible nor desirable; a parallel
subsystem with its own typed contract is the clean fit. The original
`lucarne-term` / `lucarne-rmux` split was intentionally collapsed into
`lucarne-rmux` after package-fragmentation review: terminal vocabulary, archive
helpers, adapter, and monitor are one operational capability, while the
important boundary remains that only `lucarne-rmux` names `rmux_sdk`.

Agent chat reaching a web client is a separate concern, but this fork no longer
ships an internal `lucarne-web` production crate or `/chat` runtime route.
Terminal-bound agent prompts and transcript projection are exposed through
`lucarne-termgw` routes such as `/agent/{id}` and
`/api/sessions/{id}/agent`, which inject into the rmux pane and record terminal
binding context. Any richer web app is an external gateway API consumer; if a
future browser runtime chat is needed, it should be backed by Lucarne core
service APIs instead of reintroducing a split product layer.

## Consequences

- `lucarned`'s default product build includes the monitor, terminal gateway,
  remote control, and TUI.
- The gateway (`lucarne-termgw`) consumes the monitor's grid fan-out, applies the
  per-client differ, and serves an interactive web terminal view + an HTTP
  control surface for the CLI.
- Pop-out/retract is implemented with rmux's own `attach-session` /
  `detach-client`; the thin CLI wraps the rmux binary + the gateway HTTP — no new
  control-plane IPC is introduced.
- The preview SDK is not hidden behind a source-build feature in this fork's
  product line; the default `lucarned` build includes it through `lucarne-rmux`.
