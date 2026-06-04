
![Lucarne AI Poster](docs/assets/lucarne-ai-poster.png)

[![Release](https://github.com/tuchg/Lucarne/actions/workflows/release.yml/badge.svg)](https://github.com/tuchg/Lucarne/actions/workflows/release.yml)
![Coverage](https://img.shields.io/badge/coverage-67%2F67%20journeys-blue)
![License: MIT](https://img.shields.io/badge/license-MIT-blue)
![Telegram](https://img.shields.io/badge/channel-Telegram-26A5E4?logo=telegram)
![WeChat](https://img.shields.io/badge/channel-WeChat-07C160?logo=wechat)

English | [中文](README.cn.md)

**Stop babysitting your local AI agents.**

- No new mobile app required; receive timely, secure notifications through existing channels
- Zero-intrusion setup: no hooks, no skills, no MCP, no project changes; scan a QR code and start using it in one step
- Agents run on your local computer, so you can step away while WeChat / Telegram keep you synced on key progress
- Permission approvals, clarifying questions, and failure notifications become actionable mobile events
- Scan a WeChat QR code to receive agent messages; quote a message to continue the matching context automatically
- Use the Telegram console to view all agents, workspaces, and historical sessions
- View local agent session history and agents currently running on this machine
- Lightweight resident process with high performance and low memory use; idle agents are released automatically

---

## Quick Start

### 1. Install

macOS / Linux:

```bash
curl -LsSf https://github.com/tuchg/Lucarne/releases/latest/download/lucarned-installer.sh | sh
```

Windows PowerShell:

```powershell
powershell -c "irm https://github.com/tuchg/Lucarne/releases/latest/download/lucarned-installer.ps1 | iex"
```

<details>
<summary>Homebrew (recommended) and release archives</summary>

Homebrew:

```bash
brew tap tuchg/Lucarne https://github.com/tuchg/Lucarne
brew install lucarned
```

Release archives are also available for macOS, Linux, and Windows on x86_64 and aarch64.

</details>

### 2. Initialize

```bash
lucarned init
```

Initialization guides you through:

- Selecting enabled agents: `claude`, `codex`, `copilot`, `gemini`, `pi`
- Configuring a Telegram Bot Token and an entry chat with Topics/thread mode enabled (optional)
- Logging in to WeChat by QR code (optional)
- Generating the config file: `~/.lucarned/lucarned.yaml`

### 3. Start the background service

```bash
lucarned autostart install --start
```

<details>
<summary>Homebrew service commands (recommended)</summary>

```bash
brew services start lucarned
brew services restart lucarned
brew services stop lucarned
```

</details>

<details>
<summary>Platform notes</summary>

`lucarned autostart` uses native user-level service managers:

- macOS: LaunchAgent
- Windows: Task Scheduler logon task
- Linux: systemd user service

Linux autostart requires systemd user services. Non-systemd Linux can run `lucarned` manually.

</details>

### 4. Open the Telegram panel (optional)

```text
/panel
```

After the Lucarne panel appears, you can create workspaces, bind agents, resume historical sessions, and approve commands.

### Common commands

```bash
lucarned doctor
lucarned paths
lucarned autostart status
lucarned autostart start
lucarned autostart stop
lucarned update
```

<details>
<summary>Homebrew service commands</summary>

```bash
brew update
brew upgrade lucarned
brew services start lucarned
brew services restart lucarned
brew services stop lucarned
```

</details>

```text
macOS/Linux config: ~/.lucarned/lucarned.yaml
Windows config:     %LOCALAPPDATA%\lucarned\lucarned.yaml
Logs:               lucarned paths
```

---

## Configuration Example

See the full example at [`examples/lucarned.yaml`](examples/lucarned.yaml).

After initialization, the active config lives at: `~/.lucarned/lucarned.yaml`.

You can also override settings with environment variables:

```bash
export TELEGRAM_BOT_TOKEN="123456:..."
export TELEGRAM_CHAT_ID="123456789"
export LUCARNE_AUTHORIZED_USER_IDS="111111,222222"
```

---

## Usage

See the full command reference at [`docs/commands.md`](docs/commands.md). This README keeps only the core paths.

### WeChat: quote-to-route

1. Lucarne pushes agent progress to WeChat.
2. Quote a notification and reply; Lucarne automatically restores the matching agent session.
3. Continue the conversation with the original context attached.

WeChat quote routing uses two strategies: it prefers `message_id`, then falls back to a quoted-text hash.

### Telegram: mobile multi-agent console

Telegram needs Topics/thread mode for the entry chat. A private chat with the bot can use the bot's own topic mode (Bot API 9.4+ reports this as `has_topics_enabled` from `getMe`); use that private chat ID as `entry_chat_id`. A forum supergroup also works, but is not required.

1. Send `/panel` in the entry chat.
2. Tap `New` or send `/aN` to create an agent workspace.
3. Enter the workspace topic and assign tasks to agents like a normal chat.
4. When an agent asks for permission, tap `[Approve]` / `[Deny]`.
5. Send `/status` to inspect state, `/interrupt` to stop work, or `/fork` to branch a session.

Telegram workspaces map to Forum Topics. One project gets one topic; one topic can bind one live agent session.
- Telegram supports every WeChat feature.

### Terminal monitor and the `lucarned tui` dashboard

The terminal-monitor subsystem is delivered through the `lucarned` product
bundle feature (`product-terminal`). The default source build remains the base
daemon; release packaging opts into the bundle so installed users still get the
single `lucarned` entry. The terminal bundle mirrors sessions on your
system-wide rmux daemon to a web terminal view and lets a session be popped into
your local terminal and retracted again while the remote mirror keeps running.
External Web apps consume the same gateway API; Lucarne does not ship a separate
internal web-chat runtime crate.

`lucarned tui` is the single interactive entry for the local operator. It is a
full-screen, arrow-key-navigable dashboard and replaces the old standalone
`term` CLI. Launch it with:

```bash
lucarned tui                             # launch the full-screen dashboard
```

From source, build or run this capability with an explicit feature:

```bash
cargo +nightly run -Zbuild-dir-new-layout -p lucarned --features product-terminal -- tui
```

The dashboard has three panels (switch with `Tab` / `←` `→`, navigate items with
`↑` `↓`, quit with `q` / `Esc`):

- **Sessions** — lists your system-wide rmux sessions; `Enter` attaches (pops the
  session into your terminal and returns to the dashboard on detach), `d` detaches,
  `k` / `Del` kills, `a` archives to the shared archive store, `r` refreshes.
- **Go Public** — `s` starts the remote-access tunnel, `x` stops it, `r` checks
  status, and `Enter` shows a high-contrast QR modal of the login URL so a phone
  can reach the terminal gateway remotely (it falls back to the plain URL
  when the terminal is too small to draw a scannable QR).
- **Config** — edits the remote-access provider fields (Cloudflared-first) with
  secret fields masked and never echoed, and saves back to `lucarned.yaml` with a
  timestamped backup.

For scripts and SSH sessions, use the equivalent headless entry:

```bash
lucarned remote start
lucarned remote status --json
lucarned remote stop
```

`remote.enabled: true` means autostart the tunnel at daemon boot. With
`remote.enabled: false`, `lucarned` still serves the loopback control plane and
waits for `lucarned remote start` or the TUI Go Public panel.

Leaving the Cloudflared token blank uses Cloudflare Quick Tunnel: an ephemeral
`trycloudflare.com` URL intended for testing/development. Use a named tunnel
(`token` + `public_url`) for sensitive or repeatable remote access.

See [`docs/tui.md`](docs/tui.md) for the full TUI guide (panels, keybindings,
the daemon requirement for Go Public, and `term` → `lucarned tui` migration).

---

## Architecture Overview

```
┌─────────────┐  ┌─────────────┐
│  Telegram   │  │   WeChat    │  ← User-facing channels
└──────┬──────┘  └──────┬──────┘
       │                │
   lucarne-         lucarne-
   telegram         wechat          ← Channel adapter (commands, notifications, queues, retries)
       │                │
       └───────┬────────┘
          lucarne-adapter           ← Plugin registry
               │
           lucarne                  ← Core: runtime bus, control plane, history, daemon
               │
         agent-sessions             ← Provider parse / discovery / watch
               │
    ┌──────┬──────┬──────┬──────┐
  Claude  Codex Gemini Copilot  Pi  ← Agent CLI processes
```

### Terminal-monitor subsystem

A parallel subsystem that mirrors the user's system-wide rmux terminal sessions
to web clients through the gateway API — reachable remotely through a tunnel.

```
   ┌──────────────┐
   │  web client  │  ← External Web app / browser terminal
   └──────┬───────┘
          │
    lucarne-termgw                     ← Axum ws/HTTP gateway API
          │
    lucarne-rmux                       ← Terminal vocabulary + archive + live rmux-sdk binding
          │
   ┌──────┴───────┐
   │ system rmux  │   lucarne-remote     ← go-public tunnel registry (Cloudflared-first)
   │   daemon     │
   └──────────────┘   lucarned tui / remote CLI ← sessions / go-public / config
```

---

## Agent Capability Matrix

| Capability | Claude | Codex | Gemini | Copilot | Pi |
|---|---:|---:|---:|---:|---:|
| Reasoning / Thinking | ✅ | ✅ | ✅ | ✅ | ✅ |
| Tool calls | ✅ | ✅ | ✅ | ✅ | ✅ |
| Structured approval | ✅ | ✅ | ✅ | — | ✅ |
| AskUserQuestion | ✅ | ✅ | ✅ | — | — |
| Usage tracking | ✅ | ✅ | ✅ | ✅ | ✅ |
| Interrupt | ✅ | ✅ | ✅ | — | ✅ |
| Resume | ✅ | ✅ | ✅ | — | ✅ |
| Sub-agents | ✅ | ✅ | — | — | — |
| Native commands | ✅ | ✅ | ✅ | — | ✅ |
| Fork (create branched session) | ✅ | ✅ | — | — | ✅ |

---

## Development

```bash
git clone https://github.com/tuchg/Lucarne.git
cd Lucarne
cargo +nightly check -Zbuild-dir-new-layout
cargo +nightly test -Zbuild-dir-new-layout
```

---

## Roadmap
- [x] Linux support: installation docs, service management, release packages, and smoke tests
- [x] Windows support: installation docs, background execution, path / process compatibility, and release packages
- [ ] Message modes: steer / queue
- [x] Keep `agent-sessions` as an independent workspace crate
- [x] Support remote agent environments: rmux terminal monitor + gateway API + go-public tunnel
- [ ] More agent providers: Cursor, opencode, and more
- [ ] More channels: Discord, Slack, Feishu, DingTalk, Matrix, QQ, and more
- [ ] ....

---

## License

MIT

#
> Friend link: [linux.do](https://linux.do/)
