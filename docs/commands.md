# Lucarne 命令参考

## Telegram

### Entry / 通知 topic

| 命令 | 用途 |
|---|---|
| `/panel` / `/start` | 打开或刷新管理面板 |
| `/help` | 查看入口帮助 |
| `/config` | 查看全局配置 |
| `/config global bypass\|notifications on\|off` | 开关全局 bypass / 通知 |
| `/status` | 查看全局 Agent 资源状态 |
| `/kill all\|<session_id:pid>` | 全局终止 Agent 进程 |
| `/clear_workspaces` | 清空 workspace 记录 |
| `/reset_notifications` | 重建通知 topic |
| `/aN` | 用面板第 N 个 agent 新建 session |
| `/hN` | 恢复当前页第 N 条历史 session |
| `/wN` | 打开当前视图第 N 个 workspace |

隐藏兼容输入：`/refresh`、`/next`、`/prev` 仍可手输或由按钮路径触发，但不注册到 Telegram BotCommand，也不作为公开命令展示。

### Workspace topic

| 命令 | 用途 |
|---|---|
| `/help` | 查看 workspace 命令帮助 |
| `/rename <name>` | 重命名当前 workspace |
| `/config workspace\|session bypass\|notifications on\|off` | 设置 workspace / session 级配置 |
| `/commands` | 列出当前 Agent 支持的命令 |
| `/commands <command>` | 通过 Lucarne 调用 Agent 命令 |
| `/commands <command> help` | 查看某个命令帮助 |
| `/model [model] [reasoning]` / `/models` | 查看 / 切换模型和推理档位 |
| `/permissions [mode]` | 查看 / 设置权限模式 |
| `/skills` | 列出可用 skills |
| `/status` | 查看当前 workspace Agent 状态和进程资源 |
| `/interrupt` | 中断当前 turn（绕过队列） |
| `/kill all\|<session_id:pid>` | 终止当前 workspace Agent 进程 |
| `/fork [target]` | 列出 fork 目标或 fork 指定目标 |
| `/fN` | fork `/fork` 列表中的第 N 个目标 |
| `/new` | 新建 Agent 对话 |
| `/quit` | 关闭当前 live session |

## WeChat

| 命令 / 操作 | 用途 |
|---|---|
| 引用 Lucarne 通知并回复 | 恢复对应 provider session，继续上下文 |
| 引用 Lucarne 通知并回复 `/new` | 为对应 workspace 新建 Agent 对话，后续回复接续新 session |
| 直接发普通消息 | 提示先引用通知 |
| `/status` | 查看全局或单 workspace 状态 |
| `/new`（未引用） | 提示先引用通知，避免无法确定 workspace |
| `/kill all` | 终止所有 Agent 进程 |
| `/kill <session_id:pid>` | 终止指定 Agent 进程 |
| `/help` | 查看 WeChat 命令帮助 |
| `/config` | 查看当前 bypass、notifications 状态 |
| `/config global notifications on\|off` | 开关全局通知 |
| `/config global bypass on\|off` | 开关全局权限绕过 |

## 终端控制台（`lucarned tui`）

`lucarned tui` 是本地操作者的唯一交互入口（替代旧的 `term` CLI）：

> 完整说明（面板、键位、Go Public 的守护进程依赖、`term` → `lucarned tui` 迁移）见 [`docs/tui.md`](tui.md)。

```bash
lucarned tui
```

全局键位：

| 键位 | 用途 |
|---|---|
| `↑` / `↓` | 在当前面板内上下移动选项 |
| `Tab` / `←` / `→` | 在 Sessions / Go Public / Config 三个面板间切换 |
| `r` | 刷新当前面板 |
| `q` / `Esc` | 关闭模态（如打开）/ 退出控制台 |
| `Ctrl-C` | 强制退出 |

### Sessions 面板（rmux 会话）

| 键位 | 用途 |
|---|---|
| `Enter` | attach：把会话弹出到本地终端，detach 后回到控制台 |
| `d` | detach 当前客户端（远程镜像继续运行） |
| `k` / `Del` | kill 选中会话 |
| `a` | archive 选中会话到共享 archive store |
| `r` | 刷新会话列表 |

### Go Public 面板（远程接入）

| 键位 | 用途 |
|---|---|
| `s` | 启动远程接入隧道（`/api/remote/start`） |
| `x` | 停止隧道（`/api/remote/stop`） |
| `r` | 查询状态（`/api/remote/status`） |
| `Enter` | 弹出登录 URL 的高对比二维码模态（终端过小时回退为纯 URL） |
| `Esc` / `q` / `Enter` | 关闭二维码模态 |

## Headless 远程接入（`lucarned remote`）

`lucarned remote` 是 TUI Go Public 面板的脚本化对等入口。它只调用 daemon 的
loopback control plane，不直接拥有隧道生命周期；daemon 必须正在运行。

```bash
lucarned remote start
lucarned remote start --provider cloudflared --field token=... --field public_url=https://...
lucarned remote status --json
lucarned remote stop
```

| 命令 | 用途 |
|---|---|
| `lucarned remote start` | 通过 `/api/remote/start` 启动远程接入隧道 |
| `lucarned remote status` | 通过 `/api/remote/status` 查询隧道状态 |
| `lucarned remote stop` | 通过 `/api/remote/stop` 停止隧道 |
| `--control-port PORT` | 指定 loopback control plane 端口，默认 `7801` |
| `--json` | 输出稳定 JSON，便于脚本消费 |
| `--provider ID` | 仅 `start`：覆盖 daemon 配置中的 provider |
| `--field KEY=VALUE` | 仅 `start`：覆盖/补充 provider 字段 |

### Config 面板（远程配置）

| 键位 | 用途 |
|---|---|
| `Enter` | 编辑选中字段 / 切换 provider（密钥字段掩码显示，不回显） |
| 输入字符 / `Backspace` | 编辑进行中的字段值 |
| `Enter`（编辑中） | 提交编辑 |
| `Esc`（编辑中） | 取消编辑 |
| `s` / `S` | 校验并保存回 `lucarned.yaml`（生成带时间戳的备份） |
