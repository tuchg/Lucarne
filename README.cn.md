
![Lucarne AI Poster](docs/assets/lucarne-ai-poster.png)

[![Release](https://github.com/tuchg/Lucarne/actions/workflows/release.yml/badge.svg)](https://github.com/tuchg/Lucarne/actions/workflows/release.yml)
![Coverage](https://img.shields.io/badge/coverage-67%2F67%20journeys-blue)
![License: MIT](https://img.shields.io/badge/license-MIT-blue)
![Telegram](https://img.shields.io/badge/channel-Telegram-26A5E4?logo=telegram)
![WeChat](https://img.shields.io/badge/channel-WeChat-07C160?logo=wechat)

[English](README.md) | 中文

**Agents 完成、卡住、需要你时，Lucarne 会在手机上叫你。**

- Agents 在本地电脑上跑，人可以放下电脑；微信 / Telegram 随时同步关键进展
- 0 侵入使用，无 hook、无 skills、无MCP，不动agent；扫码即可一键使用
- 权限审批、问题确认、失败通知，都变成手机上的可处理事件
- 微信扫码接收agents消息，引用消息即可自动接续对应上下文
- Telegram 控制台查看所有 Agent、工作区、历史会话
- 查看本机agents历史会话、本机当下正在运行的agent
- 高性能低内存的轻量常驻进程，闲置agent自动释放
- 无需在手机安装新的app，安全接收最及时的消息通知

---

## 快速启动

### 1. 安装

macOS / Linux：

```bash
curl -LsSf https://github.com/tuchg/Lucarne/releases/latest/download/lucarned-installer.sh | sh
```

Windows PowerShell：

```powershell
powershell -c "irm https://github.com/tuchg/Lucarne/releases/latest/download/lucarned-installer.ps1 | iex"
```

<details>
<summary>Homebrew（推荐）与发布包</summary>

Homebrew：

```bash
brew tap tuchg/Lucarne https://github.com/tuchg/Lucarne
brew install lucarned
```

也可以在 GitHub Releases 下载 macOS、Linux、Windows 的 x86_64 / aarch64 发布包。

</details>

### 2. 初始化

```bash
lucarned init
```

初始化会引导你：

- 选择启用的 Agent：`claude`、`codex`、`copilot`、`gemini`、`pi`
- 配置 Telegram Bot Token 和已开启 Topics/Thread mode 的入口 chat（可选）
- 扫码登录微信（可选）
- 生成配置文件：`~/.lucarned/lucarned.yaml`

### 3. 启动后台服务

```bash
lucarned autostart install --start
```

<details>
<summary>Homebrew service 命令（推荐）</summary>

```bash
brew services start lucarned
brew services restart lucarned
brew services stop lucarned
```

</details>

<details>
<summary>平台说明</summary>

`lucarned autostart` 使用各系统的用户级服务管理器：

- macOS：LaunchAgent
- Windows：Task Scheduler 登录任务
- Linux：systemd user service

Linux autostart 需要 systemd user service。非 systemd Linux 可以手动运行 `lucarned`。

</details>

### 4. 打开 Telegram 面板 （可选）

```text
/panel
```

看到 Lucarne 面板后，即可新建 workspace、绑定 Agent、恢复历史 session、审批命令。

### 常用命令

```bash
lucarned doctor
lucarned paths
lucarned autostart status
lucarned autostart start
lucarned autostart stop
lucarned update
```

<details>
<summary>Homebrew service 命令</summary>

```bash
brew update
brew upgrade lucarned
brew services start lucarned
brew services restart lucarned
brew services stop lucarned
```

</details>

```text
macOS/Linux 配置: ~/.lucarned/lucarned.yaml
Windows 配置:     %LOCALAPPDATA%\lucarned\lucarned.yaml
日志:             lucarned paths
```

---

## 配置示例

完整示例见 [`examples/lucarned.yaml`](examples/lucarned.yaml)。

初始化后，实际配置位于：`~/.lucarned/lucarned.yaml`。

也可用环境变量覆盖：

```bash
export TELEGRAM_BOT_TOKEN="123456:..."
export TELEGRAM_CHAT_ID="123456789"
export LUCARNE_AUTHORIZED_USER_IDS="111111,222222"
```

---

## 使用方式

完整命令参考见 [`docs/commands.md`](docs/commands.md)。README 只保留核心路径。

### WeChat：引用即路由

1. Lucarne 把 Agent 进展推送到微信。
2. 引用通知并回复，Lucarne 自动恢复对应 agent session。
3. 继续追问，自动接续原上下文。

微信引用路由支持双策略：优先 `message_id`，失败后用引用文本哈希兜底。

### Telegram：移动端多 Agent 控制台

Telegram 入口 chat 需要开启 Topics/Thread mode。可以直接使用 Bot 私聊里的 Bot 自带话题模式（Bot API 9.4+ 的 `getMe` 会返回 `has_topics_enabled`）；`entry_chat_id` 填这个私聊 ID。Forum supergroup 也可用，但不是必须。

1. 在入口 chat 发送 `/panel`。
2. 点 `New` 或发送 `/aN` 新建 Agent workspace。
3. 进入 workspace topic，像聊天一样给 Agent 派任务。
4. Agent 请求权限时，点 `[Approve]` / `[Deny]`。
5. 需要查看状态时发 `/status`；需要中断时发 `/interrupt`；需要分支时发 `/fork`。

Telegram workspace 映射为 Forum Topic。一个项目一个 topic；一个 topic 可绑定一个 live Agent session。
- Telegram支持WeChat所有功能

### 终端监控与 `lucarned tui` 控制台

终端监控子系统通过 `lucarned` 的产品 bundle feature（`product-terminal`）交付。
默认源码构建仍保持 base daemon；release packaging 会显式启用 bundle，所以安装用户仍然
只有一个 `lucarned` 入口。终端 bundle 会把系统级 rmux daemon 上的会话镜像到 web
终端视图，并允许把某个会话弹出到本地终端、再收回，而远程镜像保持运行。外部 Web app
直接消费同一套 gateway API；Lucarne 内部不再维护独立 web-chat runtime crate。

`lucarned tui` 是本地操作者的唯一交互入口：一个全屏、方向键导航的控制台，替代了
旧的独立 `term` CLI。启动：

```bash
lucarned tui                             # 启动全屏控制台
```

源码构建/运行这个能力时需要显式 feature：

```bash
cargo +nightly run -Zbuild-dir-new-layout -p lucarned --features product-terminal -- tui
```

控制台有三个面板（`Tab` / `←` `→` 切换面板，`↑` `↓` 移动选项，`q` / `Esc` 退出）：

- **Sessions（会话）** — 列出系统级 rmux 会话；`Enter` attach（把会话弹出到本地终端，
  detach 后回到控制台），`d` detach，`k` / `Del` kill，`a` archive 到共享 archive
  store，`r` 刷新。
- **Go Public（对外暴露）** — `s` 启动远程接入隧道，`x` 停止，`r` 查询状态，`Enter`
  弹出登录 URL 的高对比二维码模态，让手机能远程访问终端 gateway（终端太小无法画出
  可扫码二维码时回退为纯 URL）。
- **Config（配置）** — 编辑远程接入 provider 字段（Cloudflared 优先），密钥字段掩码
  显示且永不回显，保存时写回 `lucarned.yaml` 并生成带时间戳的备份。

脚本和 SSH 场景使用等价的 headless 入口：

```bash
lucarned remote start
lucarned remote status --json
lucarned remote stop
```

`remote.enabled: true` 表示 daemon 启动时自动拉起隧道；`remote.enabled: false`
时，`lucarned` 仍会提供 loopback control plane，等待 `lucarned remote start` 或 TUI
Go Public 面板启动隧道。

Cloudflared token 留空时使用 Cloudflare Quick Tunnel：它会生成临时
`trycloudflare.com` URL，仅适合测试/开发。敏感或需要稳定访问的场景应配置 named
tunnel（`token` + `public_url`）。

---

## 架构概览

```
┌─────────────┐  ┌─────────────┐
│  Telegram   │  │   WeChat    │  ← 用户接触面
└──────┬──────┘  └──────┬──────┘
       │                │
   lucarne-         lucarne-
   telegram         wechat          ← Channel adapter（命令、通知、队列、重试）
       │                │
       └───────┬────────┘
          lucarne-adapter           ← Plugin registry
               │
           lucarne                  ← Core: runtime bus, control plane, history, daemon
               │
         agent-sessions             ← Provider parse / discovery / watch
               │
    ┌──────┬──────┬──────┬──────┐
  Claude  Codex Gemini Copilot  Pi  ← Agent CLI 进程
```

### 终端监控子系统

一个并行子系统：把用户系统级 rmux 终端会话通过 gateway API 镜像到 web 客户端，
并可通过隧道远程访问。

```
   ┌──────────────┐
   │  web client  │  ← 外部 Web app / 浏览器终端
   └──────┬───────┘
          │
    lucarne-termgw                    ← Axum ws/HTTP gateway API
          │
    lucarne-rmux                      ← 终端词汇 + 归档 + 实时 rmux-sdk 绑定
          │
   ┌──────┴───────┐
   │ 系统 rmux    │   lucarne-remote     ← go-public 隧道注册表（Cloudflared 优先）
   │   daemon     │
   └──────────────┘   lucarned tui / remote CLI ← sessions / go-public / config
```

---

## Agent 能力矩阵

| 能力 | Claude | Codex | Gemini | Copilot | Pi |
|---|---:|---:|---:|---:|---:|
| 推理 / Thinking | ✅ | ✅ | ✅ | ✅ | ✅ |
| 工具调用 | ✅ | ✅ | ✅ | ✅ | ✅ |
| 结构化审批 | ✅ | ✅ | ✅ | — | ✅ |
| AskUserQuestion | ✅ | ✅ | ✅ | — | — |
| 使用量追踪 | ✅ | ✅ | ✅ | ✅ | ✅ |
| 中断 | ✅ | ✅ | ✅ | — | ✅ |
| Resume | ✅ | ✅ | ✅ | — | ✅ |
| 子 Agent | ✅ | ✅ | — | — | — |
| 原生命令 | ✅ | ✅ | ✅ | — | ✅ |
| Fork（创建分支会话） | ✅ | ✅ | — | — | ✅ |

---

## 开发

```bash
git clone https://github.com/tuchg/Lucarne.git
cd Lucarne
cargo +nightly check -Zbuild-dir-new-layout
cargo +nightly test -Zbuild-dir-new-layout
```

---

## Roadmap
- [x] Linux 支持：补齐安装说明、服务管理、发布包与 smoke test
- [x] Windows 支持：补齐安装说明、后台运行、路径 / 进程兼容与发布包
- [ ] 消息模式 steer/queue
- [x] `agent-sessions` 保持为独立 workspace crate
- [x] 支持远程 agent 环境：rmux 终端监控 + gateway API + go-public 隧道
- [ ] 更多 Agent Provider：Cursor、opencode 等
- [ ] More channels：Discord、Slack、飞书、钉钉、Matrix、QQ 等更多入口
- [ ] ....

---

## License

MIT
