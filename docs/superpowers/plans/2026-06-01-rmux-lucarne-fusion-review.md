# rmux 能力融合审查与实施方案

> **给后续 agentic workers：** 这份文档是把 fork 中的 rmux 终端能力融合回
> Lucarne 原始 daemon 入口的基准方案。后续实现要保持证据驱动：任务范围变化时更新
> 本文档，提交时只包含当前任务直接涉及的文件。

**日期：** 2026-06-01

**审查分支：** `feat/rmux-terminal-monitor`，当前提交 `bf333d1`

**上游基准：** 本地 `upstream/main`、`origin/main`、`main` 都指向 `63de15e`。
审查中尝试过 `git fetch upstream --prune`，但网络 TLS 失败：
`LibreSSL SSL_connect: SSL_ERROR_SYSCALL`。因此本文的上游新鲜度以当前
checkout 里的本地 refs 为准。

**差异规模：** 相对 `upstream/main...HEAD`，当前分支共有
`66 files changed, 17554 insertions(+), 71 deletions(-)`。

**Go / No-Go 结论：** fork 产品线可以继续推进直接融合；但仍不建议不分阶段地直接合并
进上游主线。当前已按最新决策把用户可见入口融合到 `lucarned` 默认构建：remote
control、terminal gateway、TUI 和 rmux live binding 都随 `lucarned` 发布，不再要求安装
用户 source build feature。`lucarne-rmux` 仍作为独立 crate 保留，用来隔离 preview
`rmux_sdk` 边界。剩余风险集中在更完整的 integration/E2E 证据、remote config typed
service 统一、multi-pane/输入细节、public gateway 行为级隔离 harness 和 public tunnel
acceptance，不是需要重写整个子系统。

---

## 分析方法

本次审查结合了本地代码扫描、现有 ADR、workspace diff、Rust 验证命令，以及并行子代理
review：

- `lucarne-arch-origin`：原始 Lucarne 架构、可复用边界和上游 merge 风险。
- `lucarne-rmux-review`：rmux 终端子系统、gateway 合同和终端协议。
- `lucarne-cli-fusion`：CLI、daemon 入口、发行和控制面融合。
- `lucarne-security-review`：认证、隧道、本机信任边界和测试缺口。
- 追加 3 个只读子代理复核：架构归属、E2E 安全门、rmux/TUI archive 与 release 边界。


## 验证证据

2026-06-01 直接融合推进后已通过：

```bash
cargo +nightly check -Zbuild-dir-new-layout -p lucarned
cargo +nightly test -Zbuild-dir-new-layout -p lucarned-ctl args
cargo +nightly test -Zbuild-dir-new-layout -p lucarne-rmux scrollback_capture_window_is_bounded
cargo +nightly test -Zbuild-dir-new-layout -p lucarned archive_capture_args_are_bounded
cargo +nightly test -Zbuild-dir-new-layout -p lucarned default_lucarned_build_contains_terminal_gateway_tui_and_rmux_stack
cargo +nightly test -Zbuild-dir-new-layout -p lucarned remote::tests
cargo +nightly test -Zbuild-dir-new-layout -p lucarned tui::config
cargo +nightly test -Zbuild-dir-new-layout -p lucarne-termgw sec002_control_plane
cargo +nightly test -Zbuild-dir-new-layout -p lucarne-termgw
cargo +nightly test -Zbuild-dir-new-layout -p lucarned
cargo +nightly test -Zbuild-dir-new-layout -p lucarne --features terminal-agent-bind --test terminal_live_journey_manifest
cargo +nightly check -Zbuild-dir-new-layout --workspace --all-features
git diff --check
```

当前状态：`lucarned` 默认构建现在包含 remote/TUI/gateway/rmux 栈；`default_build_fusion`
测试会通过 `cargo tree -p lucarned` 断言 `lucarne-termgw`、`lucarne-rmux`、`lucarne-remote`、
`rmux-sdk`、`ratatui`、`crossterm` 均存在。`lucarned remote::tests` 还覆盖了 lazy gateway
start、provider start 失败回 Idle、gateway start 失败回 Idle、重复 start 幂等和 stop 回 Idle。
现在还覆盖了 health=Down 后 status 清理 handle 并允许下次 start relaunch，以及 stop
可恢复失败时保留 handle 供 retry。
`lucarne-termgw sec002_control_plane` 覆盖了 loopback control router 的真实 Axum 路由测试。
最新完整结果：`lucarne-termgw` 43 个测试通过；`lucarned` 151 个单元测试和 2 个
default fusion tests 通过；`tui::config` 12 个测试通过；`lucarned-ctl args` 10 个测试通过；
core `terminal_live_journey_manifest` 2 个测试通过；workspace `--all-features` check 和
`git diff --check` 均通过。

新增 Cloudflare API contract E2E 门禁：

```bash
LUCARNE_CF_API_E2E=1 \
CLOUDFLARE_ACCOUNT_ID=... \
CLOUDFLARE_API_TOKEN=... \
cargo +nightly test -Zbuild-dir-new-layout -p lucarne-remote --test cloudflare_api_e2e
```

该测试依据 Cloudflare 官方 Tunnel API：`POST /accounts/{account_id}/cfd_tunnel`
创建 named tunnel，`GET /accounts/{account_id}/cfd_tunnel/{tunnel_id}/token` 获取
connector token，最后 `DELETE /accounts/{account_id}/cfd_tunnel/{tunnel_id}` 清理。它
需要 API token 具备目标 account 的 Cloudflare Tunnel write/edit 权限。默认
未设置 `LUCARNE_CF_API_E2E=1` 或缺少凭据时只打印 skip 信息并返回，不把缺少真实
Cloudflare 凭据伪装成 public tunnel acceptance 已通过；显式设置
`LUCARNE_CF_API_E2E=1` 但缺少 account/token 会失败，避免误以为已经跑过 live API。
测试也不会打印 connector token。注意：这条验证 Cloudflare REST API contract 与 named
tunnel 凭据链，不覆盖 `lucarne-remote` 生产路径的 `Cloudflared::start` 二进制执行。

新增可复跑 Quick Tunnel E2E harness：

```bash
cargo +nightly build -Zbuild-dir-new-layout -p lucarned
LUCARNE_QUICK_TUNNEL_E2E=1 scripts/remote-quick-tunnel-e2e.sh
```

该脚本默认跳过，只有显式设置 `LUCARNE_QUICK_TUNNEL_E2E=1` 才触网。它会启动临时
`lucarned` daemon，使用 Cloudflare Quick Tunnel 打开免费 `trycloudflare.com` URL，
验证公网 unauth 401、public gateway 不暴露 `/api/remote/*`、full token list 200、
read-only HTTP create 403、full WS 首帧 `session_list`、read-only WS create 403，
随后调用 `lucarned remote stop` 并关闭临时 daemon。Quick Tunnel 按 Cloudflare 官方口径只用于
testing/development，不作为 production 发布替代；production 仍推荐 named tunnel。

2026-06-01 并行 review 复核结论：

- 架构、crate 清理、测试/发布、文档幻觉 4 个只读子代理均完成审查。
- 未发现已删除的 `lucarne-term`、`lucarne-archive`、`lucarne-web`、`lucarne-agentbind`
  仍作为 workspace member、lock package、path dependency 或源码目录存在。
- `lucarne-fakeagent` 是测试 harness，不在 default members / dist 包中；`lucarned-ctl`
  是原项目 helper library crate；`lucarne-termgw` 和 `lucarne-remote` 是当前 `lucarned`
  默认产品入口的运行时依赖，均不属于 rmux 割裂残留。
- 已修正复核指出的 stale 文档：remote ADR 不再声称 optional `remote` Cargo feature 或
  `termctl go-public`；terminal monitor ADR 不再描述已删除的 `lucarne-web` runtime bridge；
  README 开发路径改为 `cd Lucarne`；旧 Homebrew/autostart plan 加了 superseded/current
  reality note；`termgw-dev` 和 `web/js/protocol.js` 的旧 feature/type 注释已对齐。
- 测试口径需要保持诚实：`terminal_live_journey_manifest` 是 live/E2E 需求清单守卫，
  不是完整真实设备 E2E；`terminal-agent-bind` 的 lib 单测需要用
  `cargo +nightly test -Zbuild-dir-new-layout -p lucarne --features terminal-agent-bind`
  单独执行，不能只用 `--test terminal_live_journey_manifest` 代替。

## 架构基线

原始 Lucarne 仍然是 structured agent message bridge：

- `lucarne` 持有核心 `AgentRuntime` 和事件总线。
- `agent-sessions` 负责解析外部 agent transcript 文件。
- `lucarne-channel` 定义 channel 风格的投递合同。
- Telegram / WeChat 仍然是 channel/provider 集成。
- `lucarned` 是 daemon composition root，也是发行的主二进制。

fork 新增的是并行的终端能力，当前已经把小包收敛为较少的稳定边界：

- `lucarne-rmux`：统一终端能力包，包含终端词汇、grid、diff、input、registry、
  wire protocol、archive helpers、rmux SDK adapter 和 monitor；也是唯一直接命名
  `rmux_sdk` 的 crate。
- `lucarne-termgw`：Axum terminal gateway，包含认证、WebSocket、terminal HTTP API 和
  loopback remote control routes。
- 外部 Web app：直接消费 `lucarne-termgw` 的统一 gateway API；不再作为 Lucarne 内部
  production crate。
- `lucarne-remote`：可插拔 tunnel provider 抽象，当前实现是 `cloudflared`。
- `lucarne::terminal_agent_bind`：位于 `lucarne` core 的 opt-in feature module，
  负责终端 session cwd 与 agent transcript context 的绑定；不再作为独立 crate 存在。
- `lucarned tui`：单一交互式 operator frontend，已经直接融合进 `lucarned` 默认构建。

主要 live terminal 数据路径：

```text
system rmux daemon
  -> lucarne-rmux::RmuxMonitor
  -> broadcast<GridUpdate>
  -> lucarne-termgw /ws
  -> browser terminal renderer
```

当前不再内置 browser `/chat` runtime 路径。Web app 作为外部消费者接入
`lucarne-termgw`，terminal-bound agent prompt / transcript 由 `/agent/{id}` 和
`/api/sessions/{id}/agent` 承担：

```text
external web app
  -> lucarne-termgw /ws, /agent/{id}, /api/*
  -> rmux pane + terminal-agent binding projection
```

公网访问路径：

```text
browser / tunnel edge
  -> cloudflared
  -> loopback lucarne-termgw
  -> bearer token / single-use websocket ticket
```

## 已经对齐得比较好的部分

fork 没有把 terminal pane 强行塞进 Lucarne 原始 transcript parser 或 NDJSON framer。
这是正确的：terminal grid 和 agent message stream 是不同数据形态。

`lucarne-term` / `lucarne-archive` 已并入 `lucarne-rmux`。当前最重要的架构边界不是继续
维持小包拆分，而是把终端操作能力收敛到一个 crate，同时让 `rmux_sdk` preview API 只在
`lucarne-rmux` 内部出现；gateway、TUI 和 daemon 只消费 `lucarne_rmux` 的稳定公开类型。

remote tunnel 设计复用了原项目已经证明过的 registry 形态：
`RemoteAccessProvider` / `RemoteRegistry` 让后续 FRP、自托管 relay 或其他 NAT
穿透 backend 可以在 provider 边界内扩展，不污染 daemon core。

TUI 合并方向解决了早期 delivery split。删除独立 `term` binary，并暴露
`lucarned tui`，符合当前 cargo-dist 只发布 `lucarned` 的现实。

release 入口已经按产品融合口径闭环：默认 `lucarned` 构建直接包含 remote/TUI/gateway/rmux
栈，安装用户不需要知道 Cargo feature。default build 现在由 `default_build_fusion` 测试守护，
证明发行二进制具备统一入口能力。

## 原始项目可复用点

应该复用的原始 seam：

- `lucarned` 继续作为唯一 composition root 和产品入口。
- `AgentRuntime` 应成为 terminal/web 暴露的 agent chat session 的唯一 runtime bus。
- provider descriptor 模式可复用于 remote provider 表单字段和 config validation。
- cold control-plane persistence 纪律应复用于 binding、archive metadata、remote config state
  和未来的 session metadata。
- release/install 继续围绕 cargo-dist 和单一二进制展开。

不应该复用到 terminal bytes 的原始 seam：

- 不要把 live terminal pane 建模成 `agent-sessions` provider。
- 不要把 terminal grid update 走 channel adapter message contracts。
- 不要为了复用 agent framer 而做 ANSI/PTY scraping。

## 关键问题

### P0：release packaging / 默认构建入口已直接融合

已按最新决策处理：`lucarned` 默认构建直接包含 remote control、terminal gateway、
TUI、`lucarne-rmux` live binding、`lucarne-remote` 和共享 archive store。cargo-dist 仍然只
发布 `packages = ["lucarned"]`，但这个二进制已经包含 `lucarned tui` 与
`lucarned remote start|stop|status`，安装用户不需要知道 Cargo feature。

当前实现：

- `crates/lucarned/Cargo.toml` 不再把 `remote` / `tui` 作为可选 feature；相关依赖都是
  `lucarned` 的默认依赖。
- `crates/lucarned/src/main.rs` 不再保留要求用户重新编译才能启用 remote / TUI 的运行时分支。
- `crates/lucarned/tests/default_build_fusion.rs` 通过 `cargo tree -p lucarned` 断言默认构建
  必须包含 `lucarne-termgw`、`lucarne-rmux`、`lucarne-remote`、`rmux-sdk`、`ratatui`、
  `crossterm`。
- workspace `default-members` 已补入 `lucarne-rmux`、`lucarne-termgw`、`lucarne-remote`，
  裸 `cargo test` / `cargo build` 的默认包集合不再漏掉融合产品 crate。

结论：fork 发行线已经是单一 `lucarned` 产品入口。若未来向上游拆分，需要另起 upstream
compatibility 分支处理轻量默认构建，而不是在当前 fork 产品线上保留安装用户不可直接获得的
能力。

### P0：非交互 remote lifecycle 已恢复到 `lucarned remote`

独立 `term go-public` 已删除，`lucarned tui` 负责交互式 start/stop/status。现在已补回
等价 headless 命令：`lucarned remote start|stop|status`。

当前实现：命令解析在 `lucarned-ctl`，执行在 `lucarned` 内，底层复用
loopback `/api/remote/{start,stop,status}`。支持稳定文本输出、`--json`、`--control-port`，
以及 `start --provider --field KEY=VALUE` provider override。

剩余验证方向：后续可增加带 fake daemon/control server 的 CLI integration test，但
当前 parser、URL 构造、默认构建编译和 daemon remote tests 已覆盖基础路径。

### P1：WebSocket ticket cap 会淘汰合法 fresh ticket

已修复：`TicketStore::issue_scoped` 现在返回 `Result<String, TicketIssueError>`。
达到 outstanding cap 或 issue-rate cap 时拒绝签发新 ticket，不再 FIFO 淘汰旧的 live
ticket。HTTP `/auth/ticket` 将拒绝映射为 `429 Too Many Requests`。

影响：已认证调用方不能再通过 mint flood 让其他客户端刚拿到、还未使用的 ticket 失效。

验证：`lucarne-termgw` 单测覆盖了 outstanding cap 和 rate cap 两条路径，断言拒绝新票
时已有 fresh ticket 仍可消费。

### P1：TUI archive 全量 scrollback 已修复

已修复：`lucarne-rmux::monitor::scrollback_capture_start_arg()` 统一生成 bounded
`capture-pane -S -1000` 参数，HTTP gateway archive 和 TUI archive 都不再读取完整 pane
history。`lucarned` TUI 回归测试 `archive_capture_args_are_bounded` 覆盖了确切参数。

同时已修复 archive store 分叉：`lucarne-termgw` 删除私有 inline archive module，改为复用
`lucarne-rmux::archive`，与 TUI 使用同一目录和 schema。

### P1：终端 agent binding 已开始并入 Lucarne core

原先的独立 `lucarne-agentbind` crate 已移除，功能并入
`lucarne::terminal_agent_bind`，并由 `terminal-agent-bind` feature 隔离。`termgw`
现在通过 daemon 传入的 `ControlPlaneSqliteStore` 记录 terminal-agent binding history，
不再写 `~/.lucarne/agents.db` 旁路数据库。

剩余影响：这一步解决了 crate 边界和旁路 DB，但还需要继续把更高层语义纳入
`LucarneCore` API，例如 terminal-bound agent history 是否应该显示为 workspace/provider
session projection。

后续修复：增加 core service 级 API，而不是让 `termgw` 长期直接操作低层 store。

### P1：内置 `lucarne-web` 已移除，Web app 降级为外部消费者

`lucarne-web` 原先创建或持有自己的 `AgentRuntime`，与 daemon 现有 channel runtime
不够清晰。按最新融合决策，这个独立 production crate 已删除；`lucarned remote`
现在只组合 `lucarne-termgw`。

影响：入口层更清楚，Web app 不再是内部架构边界，而是外部消费者。浏览器侧需要的
terminal、agent transcript 和 prompt 注入能力通过统一 gateway API 接入。

后续如果需要 runtime chat over WebSocket，应落在 `lucarne` core service 或
`lucarne-termgw` feature route 中，不能重新引入独立 `lucarne-web` 产品层。

### P1：`lucarned-ctl` 是原项目已有 helper crate，不属于 rmux 割裂层

`lucarned-ctl` 已存在于本地 `upstream/main` / `main`，来源是原项目 install、autostart、
doctor、update 控制面设计。它不是独立发布二进制，用户入口仍只有 `lucarned`。因此本轮
不把它作为需要融合删除的 rmux 新包处理，只保留必要的 `Command::Remote` / `Command::Tui`
解析扩展。

保留理由：它的原始设计目标是 std-only、小依赖、跨平台 autostart / path / doctor 逻辑隔离，
避免控制命令 helper 反向依赖 `lucarne` core、adapter、SQLite 或 async runtime。

### P2：gateway 直接依赖 `lucarne-rmux`

`lucarne-termgw` 的 public constructor 仍直接消费 `lucarne_rmux::RmuxMonitor`，生产边界
保持 rmux-specific；内部已补一个窄 `TerminalMonitor` seam，生产实现转发到 `RmuxMonitor`，
测试可用 fake monitor 驱动 `/agent/{id}` 等真实 Axum/WebSocket 路径而不启动系统 rmux
daemon。

影响：gateway 本身仍不是 rmux-free。对当前 fork 可以接受；当前 test seam 已解决独立
gateway tests 的主要阻力，但 alternate terminal backend 仍需要正式 public trait 设计。

建议：现在不引入对外 `TerminalBackend` trait，避免为了抽象而抽象。后续如果出现第二个
backend，再把内部 `TerminalMonitor` seam 提升为正式 public trait。

### P2：`/chat` 内置 route 已移除，`/agent/{id}` 保留 terminal-bound 语义

`/agent/{id}` 是 pane-bound transcript/prompt injection 到 rmux pane。独立 `/chat`
runtime route 已随 `lucarne-web` 删除。

影响：用户和维护者不会再把 daemon-owned runtime chat session 与 terminal-bound
prompt injection 混淆。

建议：外部 Web app 直接对接 `/ws`、`/agent/{id}`、`/api/*`。如要恢复 runtime chat，
必须走 core service API，并保持 daemon canonical runtime。

### P2：remote config 顶层字段已补齐，仍需集中 typed service

daemon startup 使用 typed `RemoteFileConfig`，TUI config editor 仍用 ad-hoc YAML 操作。
本轮已把 `enabled`、`auth_token`、`readonly_token`、`insecure` 补入 TUI Config 面板：
seed、toggle/edit、validate、merge/write-back 都已有 `tui::config` 单测覆盖。

影响：安全关键字段不再缺失，但 daemon 与 TUI 仍有两套 parse / validate / write-back
实现，未来字段变化仍可能漂移。`remote.enabled` 的语义已经统一为 autostart：`false` 不再
表示禁用 remote 子系统，只表示 daemon 启动时不自动打开公网 tunnel。

后续修复：集中 remote config 的 parse、validate、default、write-back，形成共享 typed
config service。当前文档语义应保持为：

```text
remote.enabled = false 表示不 autostart；`lucarned` 仍会提供 loopback control plane，
用于 lazy start。
```

### P1：lazy-start state transition 已有 fake-provider integration 覆盖

已补测：`lucarned remote::tests` 使用 test-only gateway starter 和 fake
`RemoteAccessProvider`，不连接真实 rmux / cloudflared，覆盖：

- 首次 start 会先启动 lazy gateway，再启动 provider。
- provider start 失败后回到 Idle，可再次 start。
- gateway start 失败后不会启动 provider，并回到 Idle。
- 已运行且健康时重复 start 幂等，不重复启动 gateway / provider。
- stop 成功后回 Idle。
- status 发现 provider health=Down 后清理 stale handle，下一次 start 会 relaunch。
- stop 遇到 recoverable provider error 时保留 handle，下一次 stop 可 retry。

这部分关闭了 daemon-owned remote lifecycle 的核心 state-machine 缺口。

### P2：gateway/control router 隔离仍需要更强的 public-router integration harness

已有 `lucarne-termgw sec002_control_plane` 真实 Axum router 测试，覆盖 control router
loopback 正常访问和非 loopback 拒绝。当前已补内部 fake monitor seam，并新增 `/agent/{id}`
read-only prompt WebSocket integration test，断言 read-only prompt 在 terminal inject 之前被
拒绝。仍缺 public gateway router 对 `/api/remote/*` 返回未注册 / fallback 的行为级断言；
现在已经可以基于同一个 fake monitor seam 补上，不再需要真实 rmux。

建议：下一步把 public gateway router 的 `/api/remote/*` 隔离断言改成纯 router
integration test，替换目前的源码结构检查。

### P2：rmux binary resolution 重复且校验较弱

`lucarne-rmux` 和 TUI session code 都在解析 `rmux` binary：优先 `~/.cargo/bin/rmux`，否则
退回 `$PATH`。

影响：维护容易漂移，也存在本机 trust-boundary 风险。它不是远程未认证漏洞，但 archive/control
路径确实会 shell out 到该 binary。

建议：提取一个 rmux binary resolver，记录 resolved path，并在可行范围内拒绝 world-writable
目录解析结果。

### P2：monitor 目前实际只覆盖 pane `(0,0)`

session id 形态暗示 `{session}:{window}:{pane}`，但 monitor 逻辑实际仍固定到首个 pane。

影响：多 window / 多 pane rmux session 的行为会让用户困惑。

建议：要么当前产品明确限制 single-pane session，要么补齐 window/pane selection 和 switching。

### P3：WebSocket resync 与 modifier input 未完整接入

`ClientFrame::Resync.have_rev` 未使用，`TermInput::Key.mods` 在 monitor injection 中被忽略。

影响：主要是 UX 完整度，不是架构阻塞。

建议：在把 web terminal 标为生产级之前，补齐 revision-aware resync 和 modifier-aware input
injection。

## 融合策略

推荐的融合方向是：**单一产品入口、共享核心 composition、隔离 terminal backend**。

1. `lucarned` 保持唯一安装二进制和 composition root。
2. terminal vocabulary、archive helpers 和 rmux live binding 统一留在 `lucarne-rmux`。
3. `lucarne-termgw` 作为 API/gateway 层，不承担产品 lifecycle owner。
4. operator flows 收敛到 `lucarned`：
   - `lucarned tui`：交互式操作。
   - `lucarned remote start|stop|status`：headless automation。
   - 未来可选 `lucarned terminal list|attach|detach|kill|archive`：给 TUI action 做脚本化对等入口。
5. 删除独立 `lucarne-web` 产品层；Web app 是外部消费者，直接消费 `lucarne-termgw` API。
6. persistence 对齐原项目 cold control-plane store 纪律。
7. 用 default build fusion tests 守住发行入口：默认 `lucarned` 必须包含 TUI、gateway、
   remote 和 rmux live binding。

## 分阶段计划

### Phase 0：Merge Readiness Guardrails

- [x] 明确并修复 `lucarned tui` 的 release/distribution 决策：默认 `lucarned` 直接融合。
- [x] 增加 `lucarned remote start|stop|status`。
- [x] 修复 ticket mint flood 语义。
- [x] 用 bounded capture 替换 TUI whole-pane archive。
- [x] 更新 `remote.enabled` 文档语义。
- [x] 运行核心验证：

```bash
cargo +nightly check -Zbuild-dir-new-layout -p lucarned
cargo +nightly test -Zbuild-dir-new-layout -p lucarned-ctl args
cargo +nightly test -Zbuild-dir-new-layout -p lucarne-termgw
cargo +nightly test -Zbuild-dir-new-layout -p lucarned
cargo +nightly check -Zbuild-dir-new-layout --workspace --all-features
```

退出条件：发行入口明确，headless remote control 恢复，P1 安全问题关闭，default fusion
tests 通过。

### Phase 1：Core Fusion

- [x] 移除独立 `lucarne-web` crate，将 Web app 定位为外部 gateway API consumer。
- [x] 移除独立 `lucarne-agentbind` crate，将终端 agent binding 融入 `lucarne` core feature。
- [x] 将 terminal-agent binding history 从 `~/.lucarne/agents.db` 迁入 `ControlPlaneSqliteStore` cold entity。
- [x] `lucarne-termgw` 和 TUI 归档统一复用 `lucarne-rmux::archive`，不再维护两份 archive schema。
- [x] 增加 terminal-adjacent live/E2E journey manifest gate，覆盖 open/resume、prompt、streaming、
      approvals/interrupts、close、remote read-only refusal、reconnect/disconnect、archive-and-close 不污染
      `LucarneCore.live_sessions`。
- [x] TUI Config 补齐 `enabled`、`auth_token`、`readonly_token`、`insecure` 的 seed / edit /
      validate / merge 覆盖。
- [x] 集中 daemon remote config read/validation/default/env merge 到 `remote_config` typed service。
- [x] 明确 `/chat` 内置 route 删除，`/agent/{id}` 保留 terminal-bound prompt/transcript 语义。
- [x] 增加 remote lazy-start state transition integration tests。
- [x] 增加不依赖真实 rmux monitor 的 public gateway router 隔离 integration test。

退出条件：daemon、TUI、remote control 和 gateway 都使用共享 lifecycle、config 和
persistence 合同；外部 Web app 只消费公开 gateway API。

### Phase 2：Terminal Backend Hardening

- [x] 决定当前 `lucarne-termgw` 保持 rmux-specific；仅保留内部 `TerminalMonitor` test seam，
      不把 `TerminalBackend` 提升成 public abstraction。
- [x] 明确限制 pane `(0,0)`，并对非 primary pane id 返回显式错误。
- [x] 接入 `Resync.have_rev`，Resync 前比较客户端 rev 与服务端 baseline 并重新发 full snapshot。
- [x] 保留 key modifiers 并映射到 rmux/tmux `send-keys` token。
- [x] 将 rmux CLI shell-out 收敛到统一 resolver，并给非交互调用加 timeout。

退出条件：terminal 行为与 API 暴露模型一致，hot path 没有明显可避免的 blocking work。

### Phase 3：Public Access Productization

- [x] 把 real Quick Tunnel acceptance test 沉淀为 env-gated harness 和 release checklist。
- [x] 补齐 read-only token 在 `/ws`、`/agent`、write frames 上的覆盖：`/ws` 覆盖
      write-frame refusal，`/agent/{id}` 覆盖 prompt refusal before terminal inject。
- [x] 增加 env-gated Cloudflare API contract E2E，覆盖 named tunnel create/token/delete，
      不依赖外部设备或真实采集。
- [x] 在环境允许时增加 cloudflared binary/provider Quick Tunnel smoke harness。
- [x] 在用户文档中说明 Cloudflare Quick Tunnel testing/development 边界和 named tunnel 推荐。
      `docs/tui.md` 现在包含 release smoke checklist；harness 也把 `HOME` /
      `XDG_CONFIG_HOME` / `XDG_CACHE_HOME` 隔离到临时目录，避免读取开发机既有
      cloudflared 配置。

退出条件：public remote access 有明确认证语义、信任边界和可复现 operator docs。

## 建议下一组命令

```bash
cargo +nightly test -Zbuild-dir-new-layout -p lucarne-termgw
cargo +nightly test -Zbuild-dir-new-layout -p lucarned
cargo +nightly test -Zbuild-dir-new-layout -p lucarne-remote
cargo +nightly test -Zbuild-dir-new-layout --workspace --all-features --exclude agent-sessions
cargo +nightly clippy -Zbuild-dir-new-layout --workspace --all-targets --all-features --exclude agent-sessions --no-deps -- -D warnings
LUCARNE_QUICK_TUNNEL_E2E=1 scripts/remote-quick-tunnel-e2e.sh
```

Clippy matrix 说明：为让 `-D warnings` 在当前 workspace 基线上可复跑，`lucarne`、
`lucarne-adapter`、`lucarne-telegram`、`lucarne-wechat` 和少量 integration test target
加入了显式 clippy baseline allow。它只冻结既有结构/测试风格 lint，不代表已经完成
core runtime / channel adapter 的 clippy 驱动重构；rmux/remote 本轮新增路径仍按具体 lint
修复收敛。

## 当前剩余缺口

- `remote` config daemon read/default/env/validation 已集中到 `remote_config` typed service；
  TUI write-back 仍保留 YAML merge 层，但 seed 顶层字段复用 typed parse。
- public gateway router 的 `/api/remote/*` 隔离已有 fake-monitor behavior integration harness。
- terminal live/E2E 目前有 manifest gate；`/agent/{id}` read-only prompt 已有本地
  WebSocket integration 覆盖，Cloudflare API create/token/delete 已补 env-gated contract
  E2E，Quick Tunnel 真实公网 harness 已沉淀为 `scripts/remote-quick-tunnel-e2e.sh`。
- rmux binary resolver 已统一到 `lucarne-rmux::cli`，TUI 和 monitor 复用同一路径。
- multi-window / multi-pane 当前明确限制 primary pane `(0,0)`；`Resync.have_rev`、
  modifier-aware input injection 和 rmux CLI timeout 均已补齐。

## 上游合并风险图

高冲突文件：

- `Cargo.toml`
- `Cargo.lock`
- `crates/lucarned/src/main.rs`
- `crates/lucarned-ctl/src/args.rs`
- `.github/workflows/release.yml` / cargo-dist 配置

低风险 additive 区域：

- `crates/lucarne-rmux`
- `crates/lucarne-termgw`
- `crates/lucarne-remote`
- `crates/lucarne/src/terminal_agent_bind.rs`，作为 core feature module，不再是独立 crate

推荐 merge shape：

1. 先落统一终端能力包：`lucarne-rmux`（term 类型、archive、adapter、monitor）。
2. 落带 integration tests 的 gateway/auth：`lucarne-termgw`。
3. 落 remote provider seam：`lucarne-remote`。
4. 在 `lucarned` 中接入 remote/TUI/control commands，保持单一产品入口。
5. 将 terminal-agent binding 作为 `lucarne` core opt-in feature 落地，而不是独立 crate。
6. 在 release packaging 验证后，把 TUI 作为默认 `lucarned` 产品 frontend 落地。

这个顺序能把与上游 daemon/channel code 的冲突压到最低，并保留清晰 fallback 点。
