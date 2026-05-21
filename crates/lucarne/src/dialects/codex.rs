//! Codex dialect — translates Codex app-server JSON-RPC into canonical
//! events. Stateful per-session (tracks `threadID` from the init
//! handshake). Always create via [`Codex::new`].
//!
//! Wire overview (happy path):
//!
//! ```text
//! us  -> initialize                                  (id=0)
//! agt -> result { protocolVersion? }
//! us  -> initialized (notification)
//! us  -> thread/start | thread/resume                (id=1)
//! agt -> result { threadId | thread.id }
//! us  -> turn/start                                  (id=N)
//! agt -> turn/started   notification
//! agt -> item/started   notification* (commandExecution)
//! agt -> item/completed notification* (agent_message | command_execution | file_change)
//! agt -> turn/completed notification { usage | turn.usage }
//! ```
//!
//! Server-initiated reverse-RPCs (we reply on the same id):
//!
//! * `item/commandExecution/requestApproval` (alias: `execCommandApproval`)
//! * `item/fileChange/requestApproval`       (alias: `applyPatchApproval`)
//! * `item/tool/requestUserInput`            (alias: `tool/requestUserInput`)
//!
//! Legacy protocol (older Codex versions): agent emits
//! `codex/event { msg: { type: "..." } }` notifications instead of
//! `item/completed`. Auto-detected from the first substantive notification.

use smol_str::SmolStr;

use crate::{
    agent_runtime::{
        AgentCommand, AgentCommandCatalog, AgentCommandCompletion, AgentCommandInput,
        AgentCommandInvocation, AgentCommandSource, AgentContextUsage, AgentForkResult,
        AgentForkTarget, AgentForkTargetCatalog, AgentModelCatalog, AgentModelOption,
        AgentPermissionCatalog, AgentPermissionOption, AgentReasoningOption, AgentSkillCatalog,
        AgentSkillSummary, AgentStatus, AgentTokenUsage, SessionRef,
    },
    dialect::{
        command_result_events, CommandDispatch, CommandMessage, CommandResult, Dialect, Input,
        ModelSelection, OutFrame, PermissionMode, PermissionSelection, SessionParams,
    },
    error::{LucarneError, Result},
    event::{
        self, Event, LogLine, Payload, PermissionAnswer, PermissionQuestion,
        PermissionQuestionOption, PermissionRequest, PermissionResponse, ResumeHandle, Risk,
        SessionClosed, SessionStarted, Timeline, TimelineItem, ToolCall, ToolResult, TurnCompleted,
        TurnFailed, Usage, UsageDelta,
    },
};
use base64::Engine;
use serde_json::{json, Map, Value};
use std::collections::{BTreeMap, HashMap, HashSet};
use std::path::PathBuf;

const MAX_CANONICAL_DIFF_LEN: usize = 12_000;
const CODEX_BYPASS_PERMISSION_PROFILE_ID: &str = ":danger-no-sandbox";
const CODEX_PERMISSION_PRESETS: &[CodexPermissionPreset] = &[
    CodexPermissionPreset {
        id: "default",
        display_name: "Default",
        description: "Codex can read and edit files in the current workspace, and run commands. Approval is required to access the internet or edit other files.",
        approval_policy: "untrusted",
        approvals_reviewer: "user",
        sandbox_mode: "workspace-write",
        default_permissions: None,
    },
    CodexPermissionPreset {
        id: "auto-review",
        display_name: "Auto-review",
        description: "Same workspace-write permissions as Default, but eligible `on-request` approvals are routed through the auto-reviewer subagent.",
        approval_policy: "on-request",
        approvals_reviewer: "guardian_subagent",
        sandbox_mode: "workspace-write",
        default_permissions: None,
    },
    CodexPermissionPreset {
        id: "full-access",
        display_name: "Full Access",
        description: "Codex can edit files outside this workspace and access the internet without asking for approval. Exercise caution when using.",
        approval_policy: "never",
        approvals_reviewer: "user",
        sandbox_mode: "danger-full-access",
        default_permissions: None,
    },
    CodexPermissionPreset {
        id: "bypass",
        display_name: "Bypass",
        description: "Codex requests the built-in :danger-no-sandbox permission profile for new or resumed threads. Managed requirements can still constrain the effective profile.",
        approval_policy: "never",
        approvals_reviewer: "user",
        sandbox_mode: "",
        default_permissions: Some(CODEX_BYPASS_PERMISSION_PROFILE_ID),
    },
];
#[derive(Clone, Copy, Debug)]
struct CodexPermissionPreset {
    id: &'static str,
    display_name: &'static str,
    description: &'static str,
    approval_policy: &'static str,
    approvals_reviewer: &'static str,
    sandbox_mode: &'static str,
    default_permissions: Option<&'static str>,
}

#[derive(Clone, Debug, PartialEq)]
enum CodexPermissionPresetWrite {
    Single { key: &'static str, value: Value },
    Batch { edits: Vec<(&'static str, Value)> },
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum ApprovalKind {
    Decision,
    Question,
}

/// Which notification protocol the server is speaking. Detected on the
/// first substantive notification frame and then sticky for the rest
/// of the session.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum NotifProto {
    /// Not yet determined — waiting for the first substantive frame.
    Unknown,
    /// Legacy: agent emits `codex/event { msg: { type: ... } }`.
    Legacy,
    /// Raw: agent emits `item/*`, `turn/*`, `thread/*` notifications directly.
    Raw,
}

#[derive(Clone, Debug)]
struct CodexQuestionOption {
    label: String,
    description: String,
}

#[derive(Clone, Debug)]
struct CodexQuestion {
    id: String,
    header: String,
    question: String,
    options: Vec<CodexQuestionOption>,
    multi_select: bool,
    is_other: bool,
    is_secret: bool,
}

#[derive(Clone, Debug)]
struct CodexForkChoice {
    target: AgentForkTarget,
    rollback_turns: u64,
}

#[derive(Clone, Debug)]
struct CodexPendingStatus {
    thread_result: Value,
}

#[derive(Clone, Debug)]
struct ServerApproval {
    rpc_id: Value,
    kind: ApprovalKind,
    questions: Vec<CodexQuestion>,
}

pub struct Codex {
    cfg: SessionParams,
    seq_id: i64,

    init_req_id: i64,
    init_method: String,
    thread_id: Option<String>,
    current_turn_id: String,
    turn_active: bool,
    thread_ready: bool,
    resume_fallback_queued: bool,

    // Client-initiated pending RPCs: id → method. (Gemini uses the same
    // pattern; we don't currently block on them, we just record them so
    // we know what they were.)
    pending: HashMap<i64, String>,

    // Server-initiated approval requests: lucarne reqID → original server rpc id.
    server_approvals: HashMap<String, ServerApproval>,

    notif_proto: NotifProto,

    pending_out: Vec<OutFrame>,
    /// Queue of user prompts received before the thread handshake completed.
    pending_prompts: Vec<Input>,
    turn_has_non_message_activity: bool,

    completed_turn_ids: HashSet<String>,
    started_item_ids: HashSet<String>,
    completed_item_ids: HashSet<String>,
    file_change_items: HashMap<String, Value>,
    fork_target_rollbacks: HashMap<String, u64>,
    pending_fork_source_thread_id: Option<String>,
    current_permission_preset: Option<String>,
    current_reasoning_effort: Option<String>,
    latest_usage: Option<Usage>,
    latest_token_usage: Option<Value>,
    pending_status: Option<CodexPendingStatus>,
    command_catalog: AgentCommandCatalog,
}

impl Codex {
    fn list_models(&mut self) -> Result<CommandDispatch> {
        self.queue_request("model/list", json!({}))?;
        Ok(CommandDispatch::deferred(std::mem::take(
            &mut self.pending_out,
        )))
    }

    fn set_model(&mut self, model: &str, reasoning: Option<&str>) -> Result<CommandDispatch> {
        let model = normalize_model_alias(model);
        self.cfg.model = model.clone();
        if let Some(effort) = reasoning {
            let effort = validate_codex_reasoning(effort)?;
            self.queue_request_with_pending(
                "config/value/write",
                codex_config_write_params("model", &model),
                format!("config/value/write|model_with_reasoning|{model}|{effort}"),
            )?;
        } else {
            self.queue_request_with_pending(
                "config/value/write",
                codex_config_write_params("model", &model),
                format!("config/value/write|model|{model}"),
            )?;
        }
        Ok(CommandDispatch::deferred(std::mem::take(
            &mut self.pending_out,
        )))
    }

    fn list_permissions(&mut self) -> Result<CommandDispatch> {
        self.queue_request_with_pending("config/read", json!({}), "config/read|permissions")?;
        Ok(CommandDispatch::deferred(std::mem::take(
            &mut self.pending_out,
        )))
    }

    fn set_permissions(&mut self, mode: &str) -> Result<CommandDispatch> {
        let preset = codex_permission_preset(mode)?;
        self.queue_permission_preset_write(preset, 0)?;
        Ok(CommandDispatch::deferred(std::mem::take(
            &mut self.pending_out,
        )))
    }

    fn list_skills(&mut self) -> Result<CommandDispatch> {
        self.queue_skills_list()?;
        Ok(CommandDispatch::deferred(std::mem::take(
            &mut self.pending_out,
        )))
    }

    fn status(&mut self) -> Result<CommandDispatch> {
        let thread_id = self
            .thread_id
            .as_deref()
            .ok_or_else(|| LucarneError::dialect("codex: command invoked before thread is ready"))?
            .to_string();
        self.queue_request_with_pending(
            "thread/read",
            json!({"threadId": thread_id, "includeTurns": false}),
            "thread/read|status",
        )?;
        Ok(CommandDispatch::deferred(std::mem::take(
            &mut self.pending_out,
        )))
    }

    fn list_fork(&mut self) -> Result<CommandDispatch> {
        let thread_id = self
            .thread_id
            .as_deref()
            .ok_or_else(|| LucarneError::dialect("codex: fork invoked before thread is ready"))?;
        self.queue_request_with_pending(
            "thread/read",
            json!({"threadId": thread_id, "includeTurns": true}),
            "thread/read|fork_targets",
        )?;
        Ok(CommandDispatch::deferred(std::mem::take(
            &mut self.pending_out,
        )))
    }

    fn new_thread_command(&mut self) -> Result<CommandDispatch> {
        self.queue_request_with_pending(
            "thread/start",
            self.thread_start_params(),
            "thread/start|new",
        )?;
        Ok(CommandDispatch::deferred(std::mem::take(
            &mut self.pending_out,
        )))
    }

    fn quit(&mut self) -> Result<CommandDispatch> {
        Ok(CommandDispatch::deferred(vec![OutFrame::close_stdin()]))
    }

    fn fork(&mut self, target: Option<&str>) -> Result<CommandDispatch> {
        let thread_id = self
            .thread_id
            .as_deref()
            .ok_or_else(|| LucarneError::dialect("codex: fork invoked before thread is ready"))?
            .to_string();
        let Some(target) = target.map(str::trim).filter(|value| !value.is_empty()) else {
            return self.list_fork();
        };
        let rollback_turns = if target == "current" || target == thread_id {
            0
        } else {
            *self.fork_target_rollbacks.get(target).ok_or_else(|| {
                LucarneError::dialect(format!(
                    "codex: unknown fork target {target:?}; run /fork to list fork targets"
                ))
            })?
        };
        self.queue_request_with_pending(
            "thread/fork",
            self.thread_fork_params(&thread_id),
            format!("thread/fork|fork|{rollback_turns}"),
        )?;
        self.pending_fork_source_thread_id = Some(thread_id);
        Ok(CommandDispatch::deferred(std::mem::take(
            &mut self.pending_out,
        )))
    }
}

impl Codex {
    pub fn new() -> Self {
        tracing::debug!(
            target: "lucarne::dialects::codex",
            "codex dialect created"
        );
        Self {
            cfg: SessionParams::default(),
            seq_id: 0,
            init_req_id: 0,
            init_method: String::new(),
            thread_id: None,
            current_turn_id: String::new(),
            turn_active: false,
            thread_ready: false,
            resume_fallback_queued: false,
            pending: HashMap::new(),
            server_approvals: HashMap::new(),
            notif_proto: NotifProto::Unknown,
            pending_out: Vec::new(),
            pending_prompts: Vec::new(),
            turn_has_non_message_activity: false,
            completed_turn_ids: HashSet::new(),
            started_item_ids: HashSet::new(),
            completed_item_ids: HashSet::new(),
            file_change_items: HashMap::new(),
            fork_target_rollbacks: HashMap::new(),
            pending_fork_source_thread_id: None,
            current_permission_preset: None,
            current_reasoning_effort: None,
            latest_usage: None,
            latest_token_usage: None,
            pending_status: None,
            command_catalog: AgentCommandCatalog::default(),
        }
    }

    fn next_id(&mut self) -> i64 {
        self.seq_id += 1;
        self.seq_id
    }

    fn queue_request(&mut self, method: &str, params: Value) -> Result<()> {
        self.queue_request_with_pending(method, params, method)
    }

    fn ready_command_message(
        title: impl Into<smol_str::SmolStr>,
        text: impl Into<SmolStr>,
    ) -> CommandDispatch {
        CommandDispatch::ready(CommandResult::Message(CommandMessage {
            title: Some(title.into()),
            text: text.into(),
            data: Value::Null,
        }))
    }

    fn dispatch_goal_command(
        &mut self,
        command: &AgentCommandInvocation,
        thread_id: &str,
    ) -> Result<CommandDispatch> {
        let args = command.args.as_deref().unwrap_or("").trim();
        match args.to_ascii_lowercase().as_str() {
            "" => self.queue_request(
                "thread/goal/get",
                json!({
                    "threadId": thread_id,
                }),
            )?,
            "clear" => self.queue_request(
                "thread/goal/clear",
                json!({
                    "threadId": thread_id,
                }),
            )?,
            "pause" => self.queue_request(
                "thread/goal/set",
                json!({
                    "threadId": thread_id,
                    "status": "paused",
                }),
            )?,
            "resume" => self.queue_request(
                "thread/goal/set",
                json!({
                    "threadId": thread_id,
                    "status": "active",
                }),
            )?,
            _ => self.queue_request(
                "thread/goal/set",
                json!({
                    "threadId": thread_id,
                    "objective": args,
                    "status": "active",
                }),
            )?,
        }
        Ok(CommandDispatch::deferred(std::mem::take(
            &mut self.pending_out,
        )))
    }

    fn dispatch_fast_command(
        &mut self,
        command: &AgentCommandInvocation,
    ) -> Result<CommandDispatch> {
        let args = command
            .args
            .as_deref()
            .unwrap_or("")
            .trim()
            .to_ascii_lowercase();
        match args.as_str() {
            "on" => {
                self.queue_request_with_pending(
                    "config/value/write",
                    codex_config_write_params("service_tier", "fast"),
                    "config/value/write|fast|on",
                )?;
                Ok(CommandDispatch::deferred(std::mem::take(
                    &mut self.pending_out,
                )))
            }
            "off" => {
                self.queue_request_with_pending(
                    "config/value/write",
                    codex_config_write_params("service_tier", ""),
                    "config/value/write|fast|off",
                )?;
                Ok(CommandDispatch::deferred(std::mem::take(
                    &mut self.pending_out,
                )))
            }
            "" | "status" => Ok(Self::ready_command_message(
                "fast",
                "Fast mode can be changed with `/fast on` or `/fast off`.",
            )),
            _ => Ok(Self::ready_command_message(
                "fast",
                "Usage: /fast [on|off|status]",
            )),
        }
    }

    fn queue_skills_list(&mut self) -> Result<()> {
        let cwd = self.cfg.cwd.trim().to_string();
        if cwd.is_empty() {
            self.queue_request("skills/list", json!({}))
        } else {
            self.queue_request_with_pending(
                "skills/list",
                json!({"cwds": [cwd]}),
                "skills/list|cwds",
            )
        }
    }

    fn queue_skills_list_cwd_fallback(&mut self) -> Result<bool> {
        let cwd = self.cfg.cwd.trim().to_string();
        if cwd.is_empty() {
            return Ok(false);
        }
        self.queue_request_with_pending("skills/list", json!({"cwd": cwd}), "skills/list|cwd")?;
        Ok(true)
    }

    fn queue_request_with_pending(
        &mut self,
        method: &str,
        params: Value,
        pending_method: impl Into<String>,
    ) -> Result<()> {
        let id = self.next_id();
        let rpc = json!({"jsonrpc": "2.0", "id": id, "method": method, "params": params});
        let mut bytes = serde_json::to_vec(&rpc)?;
        bytes.push(b'\n');
        self.pending.insert(id, pending_method.into());
        self.pending_out.push(OutFrame::rpc_request(bytes));
        Ok(())
    }

    fn queue_permission_preset_write(
        &mut self,
        preset: &'static CodexPermissionPreset,
        step: usize,
    ) -> Result<()> {
        let Some(write) = codex_permission_preset_write(preset, step) else {
            self.queue_request_with_pending(
                "config/read",
                json!({}),
                format!("config/read|verify|permissions_preset|{}", preset.id),
            )?;
            return Ok(());
        };
        match write {
            CodexPermissionPresetWrite::Single { key, value } => self.queue_request_with_pending(
                "config/value/write",
                codex_config_write_value_params(key, value),
                format!("config/value/write|permissions_preset|{}|{step}", preset.id),
            ),
            CodexPermissionPresetWrite::Batch { edits } => self.queue_request_with_pending(
                "config/batchWrite",
                codex_config_batch_write_params(edits),
                format!("config/value/write|permissions_preset|{}|{step}", preset.id),
            ),
        }
    }

    fn current_permission_preset(&self) -> &str {
        self.current_permission_preset
            .as_deref()
            .unwrap_or_else(|| codex_permission_preset_from_session_mode(self.cfg.permission_mode))
    }

    fn build_turn_start_frame(&mut self, input: &Input) -> Result<OutFrame> {
        // Callers must ensure `can_send_turn()` is true before invoking.
        self.turn_has_non_message_activity = false;
        let tid = self
            .thread_id
            .as_deref()
            .ok_or_else(|| {
                LucarneError::dialect("codex: build_turn_start_frame without thread_id")
            })?
            .to_string();
        let params = self.turn_start_params(&tid, input);
        let id = self.next_id();
        let rpc = json!({
            "jsonrpc": "2.0",
            "id": id,
            "method": "turn/start",
            "params": params,
        });
        let mut bytes = serde_json::to_vec(&rpc)?;
        bytes.push(b'\n');
        self.pending.insert(id, "turn/start".into());
        Ok(OutFrame::rpc_request(bytes))
    }

    /// True once the init/thread handshake has produced a valid thread id
    /// and we're past the `thread_ready` gate — i.e. we may safely issue
    /// `turn/start` frames.
    fn can_send_turn(&self) -> bool {
        self.thread_ready && self.thread_id.is_some()
    }

    fn flush_pending_prompts(&mut self) {
        if !self.can_send_turn() {
            return;
        }
        let prompts: Vec<Input> = std::mem::take(&mut self.pending_prompts);
        for p in prompts {
            match self.build_turn_start_frame(&p) {
                Ok(f) => self.pending_out.push(f),
                Err(e) => tracing::warn!("codex: failed to flush deferred prompt: {}", e),
            }
        }
    }
}

impl Default for Codex {
    fn default() -> Self {
        Self::new()
    }
}

impl Dialect for Codex {
    fn name(&self) -> &'static str {
        "codex"
    }

    fn init(&mut self, cfg: &SessionParams) -> Vec<OutFrame> {
        self.cfg = cfg.clone();
        tracing::debug!(
            target: "lucarne::dialects::codex",
            has_resume_data = cfg.resume_data().is_some(),
            "codex dialect initializing"
        );
        let id: i64 = 0; // match Go: initialize uses id=0
        self.init_req_id = id;
        self.init_method = "initialize".into();
        let req = json!({
            "jsonrpc": "2.0",
            "id": id,
            "method": "initialize",
            "params": {
                "clientInfo": {"name": "lucarne", "title": "lucarne", "version": "0.1.0"},
                "capabilities": {"experimentalApi": true}
            }
        });
        let Ok(mut bytes) = serde_json::to_vec(&req) else {
            return Vec::new();
        };
        bytes.push(b'\n');
        vec![OutFrame::rpc_request(bytes)]
    }

    fn drain_out_frames(&mut self) -> Vec<OutFrame> {
        std::mem::take(&mut self.pending_out)
    }

    fn encode_user_message(&mut self, input: &Input) -> Result<Vec<OutFrame>> {
        if !self.can_send_turn() {
            // Defer until the handshake completes; the runtime will
            // flush via drain_out_frames once we become ready.
            self.pending_prompts.push(input.clone());
            return Ok(Vec::new());
        }
        let frame = self.build_turn_start_frame(input)?;
        Ok(vec![frame])
    }

    fn command_catalog(&self) -> AgentCommandCatalog {
        self.command_catalog.clone()
    }

    fn handle_native_command(
        &mut self,
        command: &AgentCommandInvocation,
    ) -> Result<CommandDispatch> {
        let name = normalize_command_name(command.name.as_str());
        let thread_id = self
            .thread_id
            .as_deref()
            .ok_or_else(|| LucarneError::dialect("codex: command invoked before thread is ready"))?
            .to_string();
        match name {
            "review" => {
                self.queue_request(
                    "review/start",
                    json!({
                        "threadId": thread_id,
                        "target": codex_review_target(command),
                        "delivery": codex_review_delivery(command),
                    }),
                )?;
                Ok(CommandDispatch::deferred(std::mem::take(
                    &mut self.pending_out,
                )))
            }
            "compact" => {
                self.queue_request("thread/compact/start", json!({"threadId": thread_id}))?;
                Ok(CommandDispatch::deferred(std::mem::take(
                    &mut self.pending_out,
                )))
            }
            "rename" => {
                let name = codex_required_text_arg(command, "name")?;
                self.queue_request(
                    "thread/name/set",
                    json!({"threadId": thread_id, "name": name}),
                )?;
                Ok(CommandDispatch::deferred(std::mem::take(
                    &mut self.pending_out,
                )))
            }
            "stop" | "clean" => {
                self.queue_request(
                    "thread/backgroundTerminals/clean",
                    json!({"threadId": thread_id}),
                )?;
                Ok(CommandDispatch::deferred(std::mem::take(
                    &mut self.pending_out,
                )))
            }
            "fast" => self.dispatch_fast_command(command),
            "hooks" => {
                let cwd = self.cfg.cwd.trim().to_string();
                let params = if cwd.is_empty() {
                    json!({})
                } else {
                    json!({"cwds": [cwd]})
                };
                self.queue_request("hooks/list", params)?;
                Ok(CommandDispatch::deferred(std::mem::take(
                    &mut self.pending_out,
                )))
            }
            "apps" => {
                self.queue_request(
                    "app/list",
                    json!({
                        "cursor": null,
                        "limit": null,
                        "threadId": thread_id,
                    }),
                )?;
                Ok(CommandDispatch::deferred(std::mem::take(
                    &mut self.pending_out,
                )))
            }
            "goal" => self.dispatch_goal_command(command, &thread_id),
            "mcp" => {
                self.queue_request("mcpServerStatus/list", json!({}))?;
                Ok(CommandDispatch::deferred(std::mem::take(
                    &mut self.pending_out,
                )))
            }
            "plugins" => {
                self.queue_request("plugin/list", json!({}))?;
                Ok(CommandDispatch::deferred(std::mem::take(
                    &mut self.pending_out,
                )))
            }
            "" => Err(LucarneError::dialect("codex: empty command name")),
            _ => Err(LucarneError::dialect(format!(
                "codex: unsupported command {:?}",
                command.name
            ))),
        }
    }

    fn handle_system_command(
        &mut self,
        command: &AgentCommandInvocation,
    ) -> Result<CommandDispatch> {
        let name = normalize_command_name(command.name.as_str());
        match name {
            "list_commands" => {
                self.queue_request_with_pending(
                    "experimentalFeature/list",
                    json!({}),
                    "experimentalFeature/list|commands",
                )?;
                Ok(CommandDispatch::deferred(std::mem::take(
                    &mut self.pending_out,
                )))
            }
            "status" => self.status(),
            "new" => self.new_thread_command(),
            "quit" | "exit" => self.quit(),
            "fork" => match codex_fork_name(command) {
                Some(target) => self.fork(Some(&target)),
                None => self.list_fork(),
            },
            "model" => {
                if let Some((model, effort)) = codex_model_args(command)? {
                    self.set_model(&model, effort.as_deref())
                } else {
                    self.list_models()
                }
            }
            "skills" => self.list_skills(),
            "permissions" => {
                if let Some(mode) = codex_permission_preset_arg(command) {
                    self.set_permissions(&mode)
                } else {
                    self.list_permissions()
                }
            }
            "" => Err(LucarneError::dialect("codex: empty system command name")),
            _ => Err(LucarneError::dialect(format!(
                "codex: unsupported system command {:?}",
                command.name
            ))),
        }
    }

    fn encode_permission_response(
        &mut self,
        req_id: &str,
        resp: &PermissionResponse,
    ) -> Result<Vec<OutFrame>> {
        let approval = self.server_approvals.remove(req_id).ok_or_else(|| {
            LucarneError::dialect(format!("codex: unknown permission request {:?}", req_id))
        })?;
        let result: Value = match approval.kind {
            ApprovalKind::Question => {
                let answers = if resp.decision == event::Decision::Allow {
                    encode_question_answers(&approval.questions, &resp.answers)
                } else {
                    Value::Object(Map::new())
                };
                json!({"answers": answers})
            }
            ApprovalKind::Decision => {
                let decision = if resp.decision == event::Decision::Allow {
                    "accept"
                } else {
                    "reject"
                };
                json!({"decision": decision})
            }
        };
        let rpc = json!({
            "jsonrpc": "2.0",
            "id": approval.rpc_id,
            "result": result,
        });
        let mut bytes = serde_json::to_vec(&rpc)?;
        bytes.push(b'\n');
        Ok(vec![OutFrame::rpc_response(bytes)])
    }

    fn encode_interrupt(&mut self) -> Result<Vec<OutFrame>> {
        let Some(tid) = self.thread_id.clone() else {
            return Ok(vec![OutFrame::signal("SIGINT")]);
        };
        let mut params = Map::new();
        params.insert("threadId".into(), Value::String(tid));
        if !self.current_turn_id.is_empty() {
            params.insert("turnId".into(), Value::String(self.current_turn_id.clone()));
        }
        let id = self.next_id();
        let rpc = json!({
            "jsonrpc": "2.0",
            "id": id,
            "method": "turn/interrupt",
            "params": Value::Object(params),
        });
        let mut bytes = serde_json::to_vec(&rpc)?;
        bytes.push(b'\n');
        Ok(vec![OutFrame::rpc_request(bytes)])
    }

    fn translate(&mut self, raw: &[u8]) -> Vec<Event> {
        let Ok(v) = serde_json::from_slice::<Value>(raw) else {
            return vec![log_err(format!(
                "codex: invalid json-rpc frame: {:?}",
                String::from_utf8_lossy(raw)
            ))];
        };
        let Some(obj) = v.as_object() else {
            return Vec::new();
        };
        let has_id = obj.contains_key("id") && !obj.get("id").unwrap_or(&Value::Null).is_null();
        let method = obj
            .get("method")
            .and_then(|v| v.as_str())
            .unwrap_or("")
            .to_string();
        let has_method = !method.is_empty();
        match (has_id, has_method) {
            (true, false) => self.handle_response(obj),
            (false, true) => self.handle_notification(&method, obj),
            (true, true) => self.handle_server_request(&method, obj),
            _ => Vec::new(),
        }
    }

    fn on_exit(&mut self, exit_code: i32, err: Option<String>) -> Vec<Event> {
        // Ensure any callers waiting on thread_ready unblock (no-op in Rust
        // but equivalent for symmetry with Go).
        self.thread_ready = true;

        let mut payload = SessionClosed {
            reason: String::new(),
            resume: None,
        };
        if let Some(tid) = &self.thread_id {
            let mut data: BTreeMap<String, Value> = BTreeMap::new();
            data.insert("thread_id".into(), Value::String(tid.clone()));
            if !self.cfg.cwd.is_empty() {
                data.insert("cwd".into(), Value::String(self.cfg.cwd.clone()));
            }
            payload.resume = Some(ResumeHandle { version: 1, data });
        }
        if exit_code != 0 || err.is_some() {
            let mut msg = format!("codex exited with code {}", exit_code);
            if let Some(e) = err {
                msg.push_str(": ");
                msg.push_str(&e);
            }
            payload.reason = msg;
        }
        vec![Event::new(Payload::SessionClosed(payload))]
    }
}

// ——— Handlers ——————————————————————————————————————————————————

impl Codex {
    fn handle_response(&mut self, obj: &Map<String, Value>) -> Vec<Event> {
        let id = match obj.get("id").and_then(|v| v.as_i64()) {
            Some(i) => i,
            None => return Vec::new(),
        };

        // Thread init response?
        if id == self.init_req_id {
            if self.init_method == "initialize" {
                if let Some(err) = obj.get("error") {
                    let msg = err
                        .get("message")
                        .and_then(|v| v.as_str())
                        .unwrap_or("")
                        .trim()
                        .to_string();
                    let mut reason = "codex initialize failed".to_string();
                    if !msg.is_empty() {
                        reason.push_str(": ");
                        reason.push_str(&msg);
                    }
                    return self.close_thread_init(&reason);
                }
                return self.handle_initialize_response();
            }
            // thread/start or thread/resume
            if let Some(err) = obj.get("error") {
                return self.handle_thread_error(err);
            }
            let result = obj.get("result").cloned().unwrap_or(Value::Null);
            return self.handle_thread_response(&result);
        }

        if let Some(method) = self.pending.remove(&id) {
            return self.handle_client_response(id, &method, obj);
        }
        Vec::new()
    }

    fn handle_client_response(
        &mut self,
        id: i64,
        method: &str,
        obj: &Map<String, Value>,
    ) -> Vec<Event> {
        let turn_id = format!("client-rpc-{id}");
        if let Some(err) = obj.get("error") {
            if codex_should_retry_skills_list_with_cwd_fallback(method, err) {
                return match self.queue_skills_list_cwd_fallback() {
                    Ok(true) => Vec::new(),
                    Ok(false) => vec![Event::new(Payload::TurnFailed(TurnFailed {
                        turn_id,
                        error: codex_error_message(err, "codex command failed"),
                        code: String::new(),
                    }))],
                    Err(error) => vec![Event::new(Payload::TurnFailed(TurnFailed {
                        turn_id,
                        error: format!("codex command failed to queue skills fallback: {error}"),
                        code: String::new(),
                    }))],
                };
            }
            return vec![Event::new(Payload::TurnFailed(TurnFailed {
                turn_id,
                error: codex_error_message(err, "codex command failed"),
                code: String::new(),
            }))];
        }

        let result = obj.get("result").cloned().unwrap_or(Value::Null);
        if method == "turn/start" {
            if let Some(turn_id) = codex_turn_id(&result) {
                self.current_turn_id = turn_id.to_string();
                self.turn_active = true;
            }
            return Vec::new();
        }
        if method == "thread/start|new" {
            return self.handle_new_thread_command(&turn_id, &result);
        }
        if method == "thread/read|status" {
            self.pending_status = Some(CodexPendingStatus {
                thread_result: result,
            });
            if self
                .queue_request_with_pending("config/read", json!({}), "config/read|status")
                .is_err()
            {
                self.pending_status = None;
                return vec![Event::new(Payload::TurnFailed(TurnFailed {
                    turn_id,
                    error: "codex command failed to queue status config read".into(),
                    code: String::new(),
                }))];
            }
            return Vec::new();
        }
        if method == "config/read|status" {
            if let Some(preset) = codex_permission_preset_from_result(&result) {
                self.current_permission_preset = Some(preset.id.to_string());
            }
            let Some(pending) = self.pending_status.take() else {
                return vec![Event::new(Payload::TurnFailed(TurnFailed {
                    turn_id,
                    error: "codex status config returned without thread status".into(),
                    code: String::new(),
                }))];
            };
            let current_permission_preset = self.current_permission_preset();
            let result = CommandResult::Status(codex_status(
                &pending.thread_result,
                &self.cfg.model,
                self.current_reasoning_effort.as_deref(),
                current_permission_preset,
                self.latest_token_usage.as_ref(),
                self.latest_usage.as_ref(),
                Some(&result),
            ));
            return command_result_events(&turn_id, result);
        }
        if method == "thread/read|fork_targets" {
            let choices = codex_fork_choices(&result);
            self.fork_target_rollbacks = choices
                .iter()
                .map(|choice| (choice.target.id.to_string(), choice.rollback_turns))
                .collect();
            return command_result_events(
                &turn_id,
                CommandResult::ForkTargets(AgentForkTargetCatalog {
                    targets: choices.into_iter().map(|choice| choice.target).collect(),
                }),
            );
        }
        if let Some(rollback_turns) = codex_fork_rollback_marker(method) {
            return self.handle_fork_command(&turn_id, &result, rollback_turns);
        }
        if method == "thread/rollback|fork" {
            return self.handle_fork_completed_command(
                &turn_id,
                &result,
                "codex /fork rollback returned no threadId",
            );
        }
        if method.starts_with("config/value/write|")
            && result.get("status").and_then(|value| value.as_str()) == Some("ok")
        {
            if let Some((preset, step)) = codex_permission_preset_write_marker(method) {
                if self
                    .queue_permission_preset_write(preset, step + 1)
                    .is_err()
                {
                    return vec![Event::new(Payload::TurnFailed(TurnFailed {
                        turn_id,
                        error: "codex command failed to queue permission preset update".into(),
                        code: String::new(),
                    }))];
                }
                return Vec::new();
            }
            if let Some((model, effort)) = codex_model_with_reasoning_marker(method) {
                if self
                    .queue_request_with_pending(
                        "config/value/write",
                        codex_config_write_params("model_reasoning_effort", &effort),
                        format!("config/value/write|reasoning_after_model|{model}|{effort}"),
                    )
                    .is_err()
                {
                    return vec![Event::new(Payload::TurnFailed(TurnFailed {
                        turn_id,
                        error: "codex command failed to queue reasoning update".into(),
                        code: String::new(),
                    }))];
                }
                return Vec::new();
            }
            if let Some((model, effort)) = codex_reasoning_after_model_marker(method) {
                if self
                    .queue_request_with_pending(
                        "config/read",
                        json!({}),
                        format!("config/read|verify|model_with_reasoning|{model}|{effort}"),
                    )
                    .is_err()
                {
                    return vec![Event::new(Payload::TurnFailed(TurnFailed {
                        turn_id,
                        error: "codex command failed to queue model reasoning verification".into(),
                        code: String::new(),
                    }))];
                }
                return Vec::new();
            }
            if let Some((target, value)) = codex_config_marker_target_value(method) {
                if self
                    .queue_request_with_pending(
                        "config/read",
                        json!({}),
                        format!("config/read|verify|{target}|{value}"),
                    )
                    .is_err()
                {
                    return vec![Event::new(Payload::TurnFailed(TurnFailed {
                        turn_id,
                        error: "codex command failed to queue config verification".into(),
                        code: String::new(),
                    }))];
                }
                return Vec::new();
            }
        }
        if let Some(preset) = codex_permission_preset_verify_marker(method) {
            if codex_permission_preset_matches(&result, preset) {
                self.current_permission_preset = Some(preset.id.to_string());
                return codex_permission_preset_updated_events(&turn_id, preset);
            }
            return vec![Event::new(Payload::TurnFailed(TurnFailed {
                turn_id,
                error: format!(
                    "codex permission verification failed for {}",
                    preset.display_name
                ),
                code: String::new(),
            }))];
        }
        if method.starts_with("config/read|verify|") {
            return codex_config_verify_events(&turn_id, method, &result);
        }
        if method == "config/read|permissions" {
            if let Some(preset) = codex_permission_preset_from_result(&result) {
                self.current_permission_preset = Some(preset.id.to_string());
            }
        }
        if method == "experimentalFeature/list|commands" {
            let catalog = codex_feature_command_catalog(&result, self.command_catalog.revision + 1);
            self.command_catalog = catalog.clone();
            return command_result_events(&turn_id, CommandResult::Commands(catalog));
        }
        let current_permission_preset = self.current_permission_preset();
        if let Some(result) = codex_command_result(
            method,
            &result,
            &self.cfg.model,
            self.current_reasoning_effort.as_deref(),
            current_permission_preset,
            self.latest_token_usage.as_ref(),
            self.latest_usage.as_ref(),
            None,
        ) {
            return command_result_events(&turn_id, result);
        }
        let Some(text) = codex_command_response_text(method, &result, current_permission_preset)
        else {
            return Vec::new();
        };

        vec![
            tl(event::new_timeline_assistant(&turn_id, &text, false)),
            Event::new(Payload::TurnCompleted(TurnCompleted {
                turn_id,
                usage: None,
            })),
        ]
    }

    fn handle_initialize_response(&mut self) -> Vec<Event> {
        // Send the "initialized" notification (no id).
        let notif = json!({
            "jsonrpc": "2.0",
            "method": "initialized",
            "params": {}
        });
        let Ok(mut nbytes) = serde_json::to_vec(&notif) else {
            return self.close_thread_init("codex initialize follow-up failed");
        };
        nbytes.push(b'\n');

        // Decide thread/start vs thread/resume.
        let (method, params) = if let Some(prior) = self.resumable_thread_id() {
            ("thread/resume", self.thread_resume_params(&prior))
        } else {
            ("thread/start", self.thread_start_params())
        };
        let id = self.next_id();
        let req = json!({
            "jsonrpc": "2.0",
            "id": id,
            "method": method,
            "params": params,
        });
        let Ok(mut rbytes) = serde_json::to_vec(&req) else {
            return self.close_thread_init("codex initialize follow-up failed");
        };
        rbytes.push(b'\n');

        self.init_req_id = id;
        self.init_method = method.into();

        self.pending_out.push(OutFrame::rpc_request(nbytes));
        self.pending_out.push(OutFrame::rpc_request(rbytes));
        Vec::new()
    }

    fn handle_thread_response(&mut self, result: &Value) -> Vec<Event> {
        let tid = codex_thread_id(result);
        if tid.is_empty() {
            if self.queue_fresh_start_fallback() {
                return Vec::new();
            }
            let method = self.init_method.clone();
            return self.close_thread_init(&format!(
                "codex {} returned no threadId",
                if method.is_empty() {
                    "thread/start"
                } else {
                    &method
                }
            ));
        }
        self.set_active_thread(tid.clone());
        if let Some(model) = string_at(result, &["model"]) {
            self.cfg.model = model.to_string();
        }
        if let Some(reasoning) = codex_reasoning_effort_from_result(result) {
            self.current_reasoning_effort = Some(reasoning.to_string());
        }
        if let Some(preset) = codex_permission_preset_from_result(result) {
            self.current_permission_preset = Some(preset.id.to_string());
        }
        self.flush_pending_prompts();
        vec![Event::new(Payload::SessionStarted(SessionStarted {
            session_id: tid,
            model: self.cfg.model.clone(),
        }))]
    }

    fn handle_new_thread_command(&mut self, turn_id: &str, result: &Value) -> Vec<Event> {
        self.handle_started_thread_command(turn_id, result, "codex /new returned no threadId")
    }

    fn handle_started_thread_command(
        &mut self,
        turn_id: &str,
        result: &Value,
        missing_thread_error: &str,
    ) -> Vec<Event> {
        let tid = codex_thread_id(result);
        if tid.is_empty() {
            return vec![Event::new(Payload::TurnFailed(TurnFailed {
                turn_id: turn_id.into(),
                error: missing_thread_error.into(),
                code: String::new(),
            }))];
        }
        self.set_active_thread(tid.clone());
        vec![
            Event::new(Payload::SessionStarted(SessionStarted {
                session_id: tid.clone(),
                model: self.cfg.model.clone(),
            })),
            tl(event::new_timeline_assistant(
                turn_id,
                &format!("Started new thread {tid}."),
                false,
            )),
            Event::new(Payload::TurnCompleted(TurnCompleted {
                turn_id: turn_id.into(),
                usage: None,
            })),
        ]
    }

    fn handle_fork_command(
        &mut self,
        turn_id: &str,
        result: &Value,
        rollback_turns: u64,
    ) -> Vec<Event> {
        let tid = codex_thread_id(result);
        if tid.is_empty() {
            return vec![Event::new(Payload::TurnFailed(TurnFailed {
                turn_id: turn_id.into(),
                error: "codex /fork returned no threadId".into(),
                code: String::new(),
            }))];
        }
        self.thread_id = Some(tid.clone());
        self.thread_ready = true;
        if rollback_turns == 0 {
            return self.finish_fork_command(turn_id, tid);
        }
        if self
            .queue_request_with_pending(
                "thread/rollback",
                json!({"threadId": tid, "numTurns": rollback_turns}),
                "thread/rollback|fork",
            )
            .is_err()
        {
            return vec![Event::new(Payload::TurnFailed(TurnFailed {
                turn_id: turn_id.into(),
                error: "codex /fork failed to queue rollback".into(),
                code: String::new(),
            }))];
        }
        Vec::new()
    }

    fn handle_fork_completed_command(
        &mut self,
        turn_id: &str,
        result: &Value,
        missing_thread_error: &str,
    ) -> Vec<Event> {
        let tid = codex_thread_id(result);
        if tid.is_empty() {
            return vec![Event::new(Payload::TurnFailed(TurnFailed {
                turn_id: turn_id.into(),
                error: missing_thread_error.into(),
                code: String::new(),
            }))];
        }
        self.thread_id = Some(tid.clone());
        self.thread_ready = true;
        self.finish_fork_command(turn_id, tid)
    }

    fn finish_fork_command(&mut self, turn_id: &str, thread_id: String) -> Vec<Event> {
        let source_session_ref = self
            .pending_fork_source_thread_id
            .take()
            .map(|source| SessionRef(source.into()));
        let mut events = vec![Event::new(Payload::SessionStarted(SessionStarted {
            session_id: thread_id.clone(),
            model: self.cfg.model.clone(),
        }))];
        events.extend(command_result_events(
            turn_id,
            CommandResult::Forked(AgentForkResult {
                session_ref: Some(SessionRef(thread_id.into())),
                source_session_ref,
            }),
        ));
        events
    }

    fn handle_thread_error(&mut self, err: &Value) -> Vec<Event> {
        let msg = err
            .get("message")
            .and_then(|v| v.as_str())
            .unwrap_or("")
            .trim()
            .to_string();
        if self.queue_fresh_start_fallback() {
            return Vec::new();
        }
        let method = if self.init_method.is_empty() {
            "thread/start".to_string()
        } else {
            self.init_method.clone()
        };
        let mut reason = format!("codex {} failed", method);
        if !msg.is_empty() {
            reason.push_str(": ");
            reason.push_str(&msg);
        }
        self.close_thread_init(&reason)
    }

    fn close_thread_init(&mut self, reason: &str) -> Vec<Event> {
        self.thread_ready = true;
        vec![Event::new(Payload::SessionClosed(SessionClosed {
            reason: reason.into(),
            resume: None,
        }))]
    }

    fn queue_fresh_start_fallback(&mut self) -> bool {
        if self.init_method != "thread/resume" || self.resume_fallback_queued {
            return false;
        }
        let id = self.next_id();
        let req = json!({
            "jsonrpc": "2.0",
            "id": id,
            "method": "thread/start",
            "params": self.thread_start_params(),
        });
        let Ok(mut bytes) = serde_json::to_vec(&req) else {
            return false;
        };
        bytes.push(b'\n');
        self.init_req_id = id;
        self.init_method = "thread/start".into();
        self.resume_fallback_queued = true;
        self.pending_out.push(OutFrame::rpc_request(bytes));
        true
    }

    fn handle_notification(&mut self, method: &str, obj: &Map<String, Value>) -> Vec<Event> {
        let params_map: Map<String, Value> = obj
            .get("params")
            .and_then(|v| v.as_object())
            .cloned()
            .unwrap_or_default();

        if let Some(turn_id) = notification_turn_id(&params_map) {
            if method == "turn/started"
                || (self.current_turn_id.is_empty()
                    && !method.starts_with("turn/")
                    && !self.completed_turn_ids.contains(&turn_id))
            {
                self.current_turn_id = turn_id;
            }
        }
        if method == "turn/started" {
            self.turn_active = true;
        }

        // Auto-detect protocol version on first substantive notification.
        if self.notif_proto == NotifProto::Unknown {
            match method {
                "codex/event" => self.notif_proto = NotifProto::Legacy,
                "turn/started" | "turn/completed" | "thread/started" => {
                    self.notif_proto = NotifProto::Raw
                }
                m if m.starts_with("item/") => self.notif_proto = NotifProto::Raw,
                _ => {}
            }
        }

        if method == "codex/event" {
            return self.handle_legacy_event(&params_map);
        }
        if self.notif_proto == NotifProto::Legacy && is_raw_codex_notification(method) {
            return Vec::new();
        }
        self.handle_raw_notification(method, &params_map)
    }

    fn handle_legacy_event(&mut self, params: &Map<String, Value>) -> Vec<Event> {
        let Some(msg) = params.get("msg").and_then(|v| v.as_object()) else {
            return Vec::new();
        };
        let msg_type = msg.get("type").and_then(|v| v.as_str()).unwrap_or("");
        match msg_type {
            "task_started" => Vec::new(),
            "agent_message" => {
                let text = msg.get("message").and_then(|v| v.as_str()).unwrap_or("");
                if text.is_empty() {
                    return Vec::new();
                }
                if codex_agent_message_phase_is_commentary(msg.get("phase")) {
                    return vec![tl(event::new_timeline_reasoning("", text))];
                }
                vec![tl(event::new_timeline_assistant("", text, false))]
            }
            "exec_command_begin" => {
                let call_id = msg.get("call_id").and_then(|v| v.as_str()).unwrap_or("");
                let command = msg.get("command").and_then(|v| v.as_str()).unwrap_or("");
                vec![tl(event::new_timeline_tool_call(
                    call_id,
                    event::shell(command),
                ))]
            }
            "exec_command_end" => {
                let call_id = msg.get("call_id").and_then(|v| v.as_str()).unwrap_or("");
                let output = msg.get("output").and_then(|v| v.as_str()).unwrap_or("");
                vec![tl(event::new_timeline_tool_result(
                    "",
                    call_id,
                    ToolResult {
                        output: output.into(),
                        ..Default::default()
                    },
                ))]
            }
            "patch_apply_begin" => {
                let call_id = msg.get("call_id").and_then(|v| v.as_str()).unwrap_or("");
                let file = msg.get("file").and_then(|v| v.as_str()).unwrap_or("");
                vec![tl(event::new_timeline_tool_call(
                    call_id,
                    event::edit_tool(file, "", ""),
                ))]
            }
            "patch_apply_end" => {
                let call_id = msg.get("call_id").and_then(|v| v.as_str()).unwrap_or("");
                vec![tl(event::new_timeline_tool_result(
                    "",
                    call_id,
                    ToolResult {
                        output: "applied".into(),
                        ..Default::default()
                    },
                ))]
            }
            "task_complete" => {
                self.current_turn_id.clear();
                self.turn_active = false;
                vec![Event::new(Payload::TurnCompleted(TurnCompleted::default()))]
            }
            "turn_aborted" => {
                self.current_turn_id.clear();
                self.turn_active = false;
                vec![Event::new(Payload::TurnFailed(TurnFailed {
                    turn_id: String::new(),
                    error: "aborted".into(),
                    code: String::new(),
                }))]
            }
            _ => Vec::new(),
        }
    }

    fn handle_raw_notification(&mut self, method: &str, params: &Map<String, Value>) -> Vec<Event> {
        if self.is_foreign_thread(params) {
            return Vec::new();
        }
        match method {
            "turn/started" | "thread/started" => self.handle_thread_started_notification(params),
            "thread/status/changed" => Vec::new(),
            "turn/completed" => {
                let mut turn_id = String::new();
                let mut status = "completed".to_string();
                let mut usage_raw = params.get("usage").cloned().unwrap_or(Value::Null);

                if let Some(turn_val) = params.get("turn") {
                    if let Some(s) = turn_val.get("id").and_then(|v| v.as_str()) {
                        turn_id = s.to_string();
                    }
                    if let Some(s) = turn_val.get("status").and_then(|v| v.as_str()) {
                        if !s.is_empty() {
                            status = s.to_string();
                        }
                    }
                    if let Some(u) = turn_val.get("usage") {
                        if !u.is_null() {
                            usage_raw = u.clone();
                        }
                    }
                    match status.as_str() {
                        "cancelled" | "canceled" | "aborted" | "interrupted" => {
                            self.finish_turn_state(&turn_id);
                            return vec![Event::new(Payload::TurnFailed(TurnFailed {
                                turn_id,
                                error: status,
                                code: String::new(),
                            }))];
                        }
                        "failed" => {
                            let err_msg = turn_val
                                .pointer("/error/message")
                                .and_then(|v| v.as_str())
                                .filter(|s| !s.is_empty())
                                .unwrap_or("codex turn failed")
                                .to_string();
                            self.finish_turn_state(&turn_id);
                            return vec![Event::new(Payload::TurnFailed(TurnFailed {
                                turn_id,
                                error: err_msg,
                                code: String::new(),
                            }))];
                        }
                        _ => {}
                    }
                }
                if self.seen_completed_turn(&turn_id) {
                    return Vec::new();
                }
                self.finish_turn_state(&turn_id);
                let usage = parse_usage(&usage_raw).or_else(|| self.latest_usage.clone());
                vec![Event::new(Payload::TurnCompleted(TurnCompleted {
                    turn_id,
                    usage,
                }))]
            }
            "turn/failed" | "turn_aborted" => {
                let mut reason = "failed".to_string();
                if let Some(err) = params.get("error") {
                    if let Some(m) = err.get("message").and_then(|v| v.as_str()) {
                        if !m.is_empty() {
                            reason = m.into();
                        }
                    }
                }
                self.current_turn_id.clear();
                self.turn_active = false;
                vec![Event::new(Payload::TurnFailed(TurnFailed {
                    turn_id: String::new(),
                    error: reason,
                    code: String::new(),
                }))]
            }
            "error" => {
                let will_retry = params
                    .get("willRetry")
                    .and_then(|v| v.as_bool())
                    .unwrap_or(false);
                if will_retry {
                    return Vec::new();
                }
                let err_msg = params
                    .get("error")
                    .and_then(|e| e.get("message"))
                    .and_then(|v| v.as_str())
                    .filter(|s| !s.is_empty())
                    .or_else(|| params.get("message").and_then(|v| v.as_str()))
                    .unwrap_or("")
                    .to_string();
                if err_msg.is_empty() {
                    return Vec::new();
                }
                vec![Event::new(Payload::TurnFailed(TurnFailed {
                    turn_id: String::new(),
                    error: err_msg,
                    code: String::new(),
                }))]
            }
            "item/started" => self.handle_item_started(params),
            "item/completed" => self.handle_item_completed(params),
            "thread/tokenUsage/updated" => {
                if self.is_foreign_thread(params) {
                    return Vec::new();
                }
                let token_usage = params.get("tokenUsage").cloned().unwrap_or(Value::Null);
                if !token_usage.is_null() {
                    self.latest_token_usage = Some(token_usage.clone());
                }
                let snapshot = parse_token_usage(&token_usage);
                let Some(snap) = snapshot else {
                    return Vec::new();
                };
                let delta = self.update_latest_usage_snapshot(&snap);
                vec![Event::new(Payload::UsageDelta(UsageDelta { delta }))]
            }
            _ => Vec::new(),
        }
    }

    fn handle_thread_started_notification(&mut self, params: &Map<String, Value>) -> Vec<Event> {
        let tid = codex_thread_id(&Value::Object(params.clone()));
        if tid.is_empty() {
            return Vec::new();
        }
        let is_new_thread = self.thread_id.as_deref() != Some(tid.as_str());
        self.set_active_thread(tid.clone());
        self.flush_pending_prompts();
        if is_new_thread {
            vec![Event::new(Payload::SessionStarted(SessionStarted {
                session_id: tid,
                model: self.cfg.model.clone(),
            }))]
        } else {
            Vec::new()
        }
    }

    fn handle_item_started(&mut self, params: &Map<String, Value>) -> Vec<Event> {
        let Some(item) = params.get("item") else {
            return Vec::new();
        };
        let ty = normalize_item_type(item.get("type").and_then(|v| v.as_str()).unwrap_or(""));
        let id = item
            .get("id")
            .and_then(|v| v.as_str())
            .unwrap_or("")
            .to_string();
        if !id.is_empty() && !self.started_item_ids.insert(id.clone()) {
            return Vec::new();
        }
        if codex_is_subagent_item_type(&ty) {
            self.turn_has_non_message_activity = true;
            return self.handle_subagent_item(&id, item);
        }
        if ty == "file_change" {
            self.turn_has_non_message_activity = true;
            if !id.is_empty() {
                self.file_change_items.insert(id, item.clone());
            }
            return Vec::new();
        }
        if ty != "command_execution" {
            return Vec::new();
        }
        if item.get("source").and_then(|v| v.as_str()) != Some("unifiedExecStartup") {
            self.turn_has_non_message_activity = true;
        }
        let command = decode_command_execution_command(item.get("command"));
        let cwd = item
            .get("cwd")
            .and_then(|v| v.as_str())
            .unwrap_or("")
            .to_string();
        let mut input = serde_json::json!({ "command": command });
        if !cwd.is_empty() {
            if let Some(obj) = input.as_object_mut() {
                obj.insert("cwd".into(), Value::String(cwd));
            }
        }
        let call = event::tool_call("shell", input);
        vec![tl(event::new_timeline_tool_call(&id, call))]
    }

    fn handle_item_completed(&mut self, params: &Map<String, Value>) -> Vec<Event> {
        let Some(item) = params.get("item") else {
            return Vec::new();
        };
        let id = item
            .get("id")
            .and_then(|v| v.as_str())
            .unwrap_or("")
            .to_string();
        if !id.is_empty() && !self.completed_item_ids.insert(id.clone()) {
            return Vec::new();
        }
        let ty = normalize_item_type(item.get("type").and_then(|v| v.as_str()).unwrap_or(""));
        if ty == "file_change" && !id.is_empty() {
            self.file_change_items.insert(id.clone(), item.clone());
        }
        let status = item
            .get("status")
            .and_then(|v| v.as_str())
            .unwrap_or("")
            .to_string();
        match ty.as_str() {
            ty if codex_is_subagent_item_type(ty) => {
                self.turn_has_non_message_activity = true;
                self.handle_subagent_item(&id, item)
            }
            "agent_message" => {
                let text = item
                    .get("text")
                    .and_then(|v| v.as_str())
                    .unwrap_or("")
                    .to_string();
                if codex_agent_message_phase_is_commentary(item.get("phase")) {
                    return vec![tl(event::new_timeline_reasoning(&id, &text))];
                }
                vec![tl(event::new_timeline_assistant(&id, &text, false))]
            }
            "command_execution" => {
                if item.get("source").and_then(|v| v.as_str()) != Some("unifiedExecStartup") {
                    self.turn_has_non_message_activity = true;
                }
                let output = clean_command_execution_output(
                    item.get("output")
                        .and_then(|v| v.as_str())
                        .filter(|s| !s.is_empty())
                        .or_else(|| item.get("aggregatedOutput").and_then(|v| v.as_str()))
                        .unwrap_or(""),
                );
                let command = decode_command_execution_command(item.get("command"));
                let cwd = item
                    .get("cwd")
                    .and_then(|v| v.as_str())
                    .unwrap_or("")
                    .to_string();
                let mut input = serde_json::json!({ "command": command });
                if !cwd.is_empty() {
                    if let Some(obj) = input.as_object_mut() {
                        obj.insert("cwd".into(), Value::String(cwd));
                    }
                }
                let call = event::tool_call("shell", input);
                let mut evs = vec![tl(event::new_timeline_tool_call(&id, call))];
                let mut result = ToolResult::default();
                if matches!(status.as_str(), "failed" | "declined" | "rejected") {
                    result.error = status.clone();
                }
                if !output.is_empty() {
                    result.output = output;
                }
                if !result.output.is_empty() || !result.error.is_empty() {
                    evs.push(tl(event::new_timeline_tool_result("", &id, result)));
                }
                evs
            }
            "image_generation" => self.handle_image_generation_item(&id, item),
            "file_change" => {
                self.turn_has_non_message_activity = true;
                let mut path = item
                    .get("file")
                    .and_then(|v| v.as_str())
                    .unwrap_or("")
                    .to_string();
                let mut patch = item
                    .get("patch")
                    .and_then(|v| v.as_str())
                    .unwrap_or("")
                    .to_string();
                let mut content = String::new();
                let mut kind = String::new();
                if path.is_empty() || patch.is_empty() {
                    if let Some(ch) = first_file_change(item.get("changes")) {
                        if path.is_empty() {
                            path = ch.path;
                        }
                        if patch.is_empty() {
                            patch = first_non_empty(&[&ch.unified_diff, &ch.diff, &ch.patch])
                                .to_string();
                        }
                        if content.is_empty() {
                            content = ch.content.clone();
                        }
                        if kind.is_empty() {
                            kind = first_non_empty(&[&ch.kind, &ch.ty]).to_string();
                        }
                    }
                }
                if !patch.is_empty() {
                    patch = normalize_patch_envelope(&patch);
                }
                if patch.is_empty() && looks_like_patch(&content) {
                    patch = normalize_patch_envelope(&content);
                    content = String::new();
                }
                if kind == "delete" {
                    patch = file_delete_diff(&path, &patch, &content);
                    content = String::new();
                }
                patch = truncate_canonical_diff(&patch);

                let call = {
                    let mut raw_map = Map::new();
                    raw_map.insert(
                        "changes".into(),
                        item.get("changes").cloned().unwrap_or(Value::Null),
                    );
                    if !path.is_empty() {
                        raw_map.insert("path".into(), Value::String(path.clone()));
                    }
                    if !patch.is_empty() {
                        raw_map.insert("patch".into(), Value::String(patch.clone()));
                    }
                    if !content.is_empty() {
                        raw_map.insert("content".into(), Value::String(content.clone()));
                    }
                    event::tool_call("fileChange", Value::Object(raw_map))
                };
                let mut evs = vec![tl(event::new_timeline_tool_call(&id, call))];
                let result = match status.as_str() {
                    "failed" | "declined" | "rejected" => ToolResult {
                        error: status.clone(),
                        ..Default::default()
                    },
                    _ => ToolResult {
                        output: "applied".into(),
                        ..Default::default()
                    },
                };
                evs.push(tl(event::new_timeline_tool_result("", &id, result)));
                evs
            }
            _ => Vec::new(),
        }
    }

    fn handle_image_generation_item(&mut self, id: &str, item: &Value) -> Vec<Event> {
        let result = item
            .get("result")
            .and_then(Value::as_str)
            .unwrap_or("")
            .trim();
        if result.is_empty() {
            return Vec::new();
        }
        let short_id = if id.trim().is_empty() { "image" } else { id };
        // Codex `revisedPrompt` / `revised_prompt` is image-model prompt
        // metadata, not user-facing caption text. Keep it hidden here; channel
        // layers may split true captions when providers expose one.
        vec![Event::new(Payload::Attachment(event::Attachment {
            id: SmolStr::from(short_id),
            filename: SmolStr::from(format!("codex-image-{short_id}.png")),
            media_type: SmolStr::from("image/png"),
            data_base64: result.to_string(),
            caption: None,
        }))]
    }

    fn handle_subagent_item(&mut self, id: &str, item: &Value) -> Vec<Event> {
        let calls = codex_subagent_tool_calls(item);
        if calls.is_empty() {
            return Vec::new();
        }
        calls
            .into_iter()
            .enumerate()
            .map(|(idx, call)| {
                let item_id = if id.trim().is_empty() {
                    format!("subagent-{idx}")
                } else if idx > 0 {
                    format!("{id}:{idx}")
                } else {
                    id.to_string()
                };
                tl(event::new_timeline_tool_call(&item_id, call))
            })
            .collect()
    }

    fn handle_server_request(&mut self, method: &str, obj: &Map<String, Value>) -> Vec<Event> {
        match method {
            "item/commandExecution/requestApproval"
            | "execCommandApproval"
            | "item/fileChange/requestApproval"
            | "applyPatchApproval" => self.emit_approval(obj),
            "item/tool/requestUserInput" | "tool/requestUserInput" => {
                self.emit_question_request(obj)
            }
            _ => Vec::new(),
        }
    }

    fn emit_approval(&mut self, obj: &Map<String, Value>) -> Vec<Event> {
        self.turn_has_non_message_activity = true;
        let rpc_id = obj.get("id").cloned().unwrap_or(Value::Null);
        let params = obj.get("params").cloned().unwrap_or(Value::Null);

        let lucarne_id = format!("codex-{}", self.next_id());
        self.server_approvals.insert(
            lucarne_id.clone(),
            ServerApproval {
                rpc_id,
                kind: ApprovalKind::Decision,
                questions: Vec::new(),
            },
        );

        let tool = params
            .get("tool")
            .and_then(|v| v.as_str())
            .filter(|s| !s.is_empty())
            .map(|s| s.to_string())
            .unwrap_or_else(|| {
                obj.get("method")
                    .and_then(|v| v.as_str())
                    .unwrap_or("")
                    .to_string()
            });
        let mut input_map: Map<String, Value> = params.as_object().cloned().unwrap_or_default();
        if let Some(c) = params.get("command").and_then(|v| v.as_str()) {
            input_map.insert("command".into(), Value::String(c.to_string()));
        }
        if let Some(f) = params.get("file").and_then(|v| v.as_str()) {
            input_map.insert("file".into(), Value::String(f.to_string()));
        }
        if is_file_change_approval_method(obj.get("method").and_then(|v| v.as_str()).unwrap_or(""))
        {
            self.enrich_file_change_approval_input(&mut input_map);
        }

        let risk = match input_map.get("command").and_then(|v| v.as_str()) {
            Some(cmd) if !cmd.is_empty() => risk_from_command(cmd),
            _ => Risk::Medium,
        };
        vec![Event::new(Payload::PermissionRequest(PermissionRequest {
            req_id: lucarne_id,
            tool,
            input: Some(Value::Object(input_map)),
            risk,
            questions: Vec::new(),
        }))]
    }

    fn enrich_file_change_approval_input(&self, input_map: &mut Map<String, Value>) {
        let Some(item_id) = input_map.get("itemId").and_then(Value::as_str) else {
            return;
        };
        let Some(item) = self.file_change_items.get(item_id) else {
            return;
        };
        input_map.insert(
            "approvalKind".into(),
            Value::String("file_change".to_string()),
        );
        if let Some(changes) = item.get("changes") {
            input_map.insert("changes".into(), changes.clone());
        }
        if let Some(change) = first_file_change(item.get("changes")) {
            if !change.path.is_empty() {
                input_map.insert("path".into(), Value::String(change.path));
            }
            let kind = first_non_empty(&[&change.kind, &change.ty]);
            if !kind.is_empty() {
                input_map.insert("changeKind".into(), Value::String(kind.to_string()));
            }
        }
    }

    fn emit_question_request(&mut self, obj: &Map<String, Value>) -> Vec<Event> {
        self.turn_has_non_message_activity = true;
        let rpc_id = obj.get("id").cloned().unwrap_or(Value::Null);
        let params = obj.get("params").cloned().unwrap_or(Value::Null);

        let item_id = params
            .get("itemId")
            .and_then(|v| v.as_str())
            .unwrap_or("")
            .to_string();
        let thread_id = params
            .get("threadId")
            .and_then(|v| v.as_str())
            .unwrap_or("")
            .to_string();
        let turn_id = params
            .get("turnId")
            .and_then(|v| v.as_str())
            .unwrap_or("")
            .to_string();
        let questions = parse_questions(params.get("questions"));

        let lucarne_id = format!("codex-{}", self.next_id());
        let mut input_map: Map<String, Value> = Map::new();
        input_map.insert(
            "questions".into(),
            params
                .get("questions")
                .cloned()
                .unwrap_or(Value::Array(vec![])),
        );
        if !item_id.is_empty() {
            input_map.insert("itemId".into(), Value::String(item_id.clone()));
        }
        if !thread_id.is_empty() {
            input_map.insert("threadId".into(), Value::String(thread_id));
        }
        if !turn_id.is_empty() {
            input_map.insert("turnId".into(), Value::String(turn_id));
        }
        let call_id = if !item_id.is_empty() {
            item_id
        } else {
            lucarne_id.clone()
        };

        self.server_approvals.insert(
            lucarne_id.clone(),
            ServerApproval {
                rpc_id,
                kind: ApprovalKind::Question,
                questions: questions.clone(),
            },
        );

        vec![
            Event::new(Payload::Timeline(Timeline {
                item: event::new_timeline_tool_call(
                    &call_id,
                    event::unknown_tool(
                        "request_user_input",
                        Some(Value::Object(input_map.clone())),
                    ),
                ),
            })),
            Event::new(Payload::PermissionRequest(PermissionRequest {
                req_id: lucarne_id,
                tool: "request_user_input".into(),
                input: Some(Value::Object(input_map)),
                risk: Risk::Low,
                questions: project_questions(&questions),
            })),
        ]
    }

    fn is_foreign_thread(&self, params: &Map<String, Value>) -> bool {
        let tid = params
            .get("threadId")
            .and_then(|v| v.as_str())
            .unwrap_or("");
        if tid.is_empty() {
            return false;
        }
        self.thread_id.as_deref().is_some_and(|cur| cur != tid)
    }

    fn set_active_thread(&mut self, tid: String) {
        if self.thread_id.as_deref() != Some(tid.as_str()) {
            self.clear_thread_scoped_status_cache();
        }
        self.thread_id = Some(tid);
        self.thread_ready = true;
    }

    fn clear_thread_scoped_status_cache(&mut self) {
        self.latest_usage = None;
        self.latest_token_usage = None;
        self.pending_status = None;
    }

    fn seen_completed_turn(&mut self, turn_id: &str) -> bool {
        if turn_id.is_empty() {
            return false;
        }
        !self.completed_turn_ids.insert(turn_id.to_string())
    }

    fn finish_turn_state(&mut self, turn_id: &str) {
        if turn_id.is_empty() || self.current_turn_id == turn_id {
            self.current_turn_id.clear();
            self.turn_active = false;
            self.turn_has_non_message_activity = false;
        }
    }

    fn update_latest_usage_snapshot(&mut self, snapshot: &Usage) -> Usage {
        let delta = if let Some(prev) = &self.latest_usage {
            Usage {
                input_tokens: non_neg_delta(snapshot.input_tokens, prev.input_tokens),
                output_tokens: non_neg_delta(snapshot.output_tokens, prev.output_tokens),
                cache_read_tokens: non_neg_delta(
                    snapshot.cache_read_tokens,
                    prev.cache_read_tokens,
                ),
                cache_write_tokens: non_neg_delta(
                    snapshot.cache_write_tokens,
                    prev.cache_write_tokens,
                ),
                cost_usd: non_neg_float_delta(snapshot.cost_usd, prev.cost_usd),
            }
        } else {
            snapshot.clone()
        };
        self.latest_usage = Some(snapshot.clone());
        delta
    }

    fn thread_start_params(&self) -> Value {
        let mut m = Map::new();
        insert_non_empty_string(&mut m, "model", &self.cfg.model);
        m.insert("cwd".into(), Value::String(self.cfg.cwd.clone()));
        m.insert(
            "developerInstructions".into(),
            nil_if_empty(&self.cfg.system_prompt),
        );
        m.insert("persistExtendedHistory".into(), Value::Bool(true));
        m.insert(
            "approvalPolicy".into(),
            Value::String(codex_approval_policy(self.cfg.permission_mode).into()),
        );
        insert_codex_thread_permission_params(&mut m, self.cfg.permission_mode);
        Value::Object(m)
    }

    fn thread_resume_params(&self, prior: &str) -> Value {
        let mut m = Map::new();
        m.insert("threadId".into(), Value::String(prior.into()));
        m.insert("cwd".into(), Value::String(self.cfg.cwd.clone()));
        insert_non_empty_string(&mut m, "model", &self.cfg.model);
        m.insert(
            "developerInstructions".into(),
            nil_if_empty(&self.cfg.system_prompt),
        );
        m.insert(
            "approvalPolicy".into(),
            Value::String(codex_approval_policy(self.cfg.permission_mode).into()),
        );
        insert_codex_thread_permission_params(&mut m, self.cfg.permission_mode);
        m.insert("persistExtendedHistory".into(), Value::Bool(true));
        Value::Object(m)
    }

    fn thread_fork_params(&self, thread_id: &str) -> Value {
        let mut m = Map::new();
        m.insert("threadId".into(), Value::String(thread_id.into()));
        m.insert("cwd".into(), Value::String(self.cfg.cwd.clone()));
        insert_non_empty_string(&mut m, "model", &self.cfg.model);
        m.insert(
            "developerInstructions".into(),
            nil_if_empty(&self.cfg.system_prompt),
        );
        m.insert(
            "approvalPolicy".into(),
            Value::String(codex_approval_policy(self.cfg.permission_mode).into()),
        );
        insert_codex_thread_permission_params(&mut m, self.cfg.permission_mode);
        m.insert("persistExtendedHistory".into(), Value::Bool(true));
        Value::Object(m)
    }

    fn turn_start_params(&self, thread_id: &str, input: &Input) -> Value {
        let mut m = Map::new();
        m.insert("threadId".into(), Value::String(thread_id.into()));
        let mut content = Vec::new();
        for (idx, image) in input.images.iter().enumerate() {
            if image.media_type.trim().is_empty() || image.data.is_empty() {
                continue;
            }
            content.push(json!({
                "type": "text",
                "text": format!("[Image #{}]", idx + 1),
            }));
            content.push(json!({
                "type": "image",
                "url": format!(
                    "data:{};base64,{}",
                    image.media_type,
                    base64::engine::general_purpose::STANDARD.encode(&image.data)
                ),
            }));
        }
        if !input.text.is_empty() {
            content.push(json!({"type": "text", "text": input.text}));
        }
        m.insert("input".into(), Value::Array(content));
        m.insert("cwd".into(), Value::String(self.cfg.cwd.clone()));
        insert_non_empty_string(&mut m, "model", &self.cfg.model);
        m.insert(
            "approvalPolicy".into(),
            Value::String(codex_approval_policy(self.cfg.permission_mode).into()),
        );
        insert_codex_turn_permission_params(&mut m, self.cfg.permission_mode);
        Value::Object(m)
    }

    fn resumable_thread_id(&self) -> Option<String> {
        let data = self.cfg.resume_data()?;
        let prior = data.get("thread_id").and_then(|v| v.as_str())?;
        if prior.is_empty() {
            return None;
        }
        let resume_cwd = data.get("cwd").and_then(|v| v.as_str()).unwrap_or("");
        if self.cfg.cwd.is_empty() || resume_cwd.is_empty() {
            return Some(prior.to_string());
        }
        let clean = |s: &str| PathBuf::from(s).components().collect::<PathBuf>();
        if clean(&self.cfg.cwd) != clean(resume_cwd) {
            return None;
        }
        Some(prior.to_string())
    }
}

// ——— pure helpers ———

fn tl(item: TimelineItem) -> Event {
    Event::new(Payload::Timeline(Timeline { item }))
}

fn log_err(text: String) -> Event {
    Event::new(Payload::Log(LogLine {
        level: "error".into(),
        stream: "stdout".into(),
        text,
    }))
}

fn nil_if_empty(s: &str) -> Value {
    if s.is_empty() {
        Value::Null
    } else {
        Value::String(s.into())
    }
}

fn insert_non_empty_string(m: &mut Map<String, Value>, key: &str, value: &str) {
    if !value.trim().is_empty() {
        m.insert(key.into(), Value::String(value.into()));
    }
}

#[derive(Clone, Copy, Debug)]
struct CodexSlashCommandSpec {
    name: &'static str,
    description: &'static str,
    inline_args: bool,
    aliases: &'static [&'static str],
}

// Expose only Codex slash commands that map directly to a Codex app-server or
// native adapter call. Composite UI workflows stay out of /commands and are
// never sent as literal prompt text.
const CODEX_SLASH_COMMAND_SPECS: &[CodexSlashCommandSpec] = &[
    CodexSlashCommandSpec {
        name: "fast",
        description: "toggle Fast mode to enable fastest inference with increased plan usage",
        inline_args: true,
        aliases: &[],
    },
    CodexSlashCommandSpec {
        name: "hooks",
        description: "view and manage lifecycle hooks",
        inline_args: false,
        aliases: &[],
    },
    CodexSlashCommandSpec {
        name: "review",
        description: "review my current changes and find issues",
        inline_args: true,
        aliases: &[],
    },
    CodexSlashCommandSpec {
        name: "rename",
        description: "rename the current thread",
        inline_args: true,
        aliases: &[],
    },
    CodexSlashCommandSpec {
        name: "compact",
        description: "summarize conversation to prevent hitting the context limit",
        inline_args: false,
        aliases: &[],
    },
    CodexSlashCommandSpec {
        name: "goal",
        description: "set, view, pause, resume, or clear the goal",
        inline_args: true,
        aliases: &[],
    },
    CodexSlashCommandSpec {
        name: "mcp",
        description: "list configured MCP tools; use /mcp verbose for details",
        inline_args: true,
        aliases: &[],
    },
    CodexSlashCommandSpec {
        name: "apps",
        description: "manage apps",
        inline_args: false,
        aliases: &[],
    },
    CodexSlashCommandSpec {
        name: "plugins",
        description: "browse plugins",
        inline_args: false,
        aliases: &[],
    },
    CodexSlashCommandSpec {
        name: "stop",
        description: "stop all background terminals",
        inline_args: false,
        aliases: &["clean"],
    },
];

fn codex_feature_command_catalog(_result: &Value, revision: u64) -> AgentCommandCatalog {
    AgentCommandCatalog {
        commands: CODEX_SLASH_COMMAND_SPECS
            .iter()
            .map(codex_slash_command_to_agent_command)
            .collect(),
        complete: true,
        revision,
    }
}

fn codex_slash_command_to_agent_command(spec: &CodexSlashCommandSpec) -> AgentCommand {
    AgentCommand {
        name: spec.name.into(),
        description: Some(spec.description.into()),
        aliases: spec.aliases.iter().map(|alias| (*alias).into()).collect(),
        source: AgentCommandSource::ProviderNative,
        input: codex_slash_command_input(spec),
        completion: codex_slash_command_completion(spec.name),
    }
}

fn codex_slash_command_input(spec: &CodexSlashCommandSpec) -> AgentCommandInput {
    match spec.name {
        "fast" => AgentCommandInput::Text {
            label: "on|off|status".into(),
            required: false,
        },
        "review" => AgentCommandInput::Text {
            label: "instructions".into(),
            required: false,
        },
        "rename" => AgentCommandInput::Text {
            label: "name".into(),
            required: true,
        },
        "goal" => AgentCommandInput::Text {
            label: "objective|pause|resume|clear".into(),
            required: false,
        },
        "mcp" => AgentCommandInput::Text {
            label: "verbose".into(),
            required: false,
        },
        _ if spec.inline_args => AgentCommandInput::Text {
            label: "arguments".into(),
            required: false,
        },
        _ => AgentCommandInput::None,
    }
}

fn codex_slash_command_completion(name: &str) -> AgentCommandCompletion {
    match name {
        "review" | "rename" | "compact" => AgentCommandCompletion::TurnCompleted,
        _ => AgentCommandCompletion::ProviderIdle,
    }
}

fn codex_review_target(command: &AgentCommandInvocation) -> Value {
    if let Some(target) = command.values.get("target") {
        return target.clone();
    }
    let args = command.args.as_deref().unwrap_or("").trim();
    if args.is_empty() {
        json!({"type": "uncommittedChanges"})
    } else {
        json!({"type": "custom", "instructions": args})
    }
}

fn codex_review_delivery(command: &AgentCommandInvocation) -> &str {
    command
        .values
        .get("delivery")
        .and_then(|value| value.as_str())
        .unwrap_or("inline")
}

fn codex_required_text_arg(command: &AgentCommandInvocation, label: &str) -> Result<String> {
    let value = codex_optional_text_arg(command, label).unwrap_or_default();
    if value.is_empty() {
        return Err(LucarneError::dialect(format!(
            "codex: command /{} requires {label}",
            normalize_command_name(command.name.as_str())
        )));
    }
    Ok(value)
}

fn codex_optional_text_arg(command: &AgentCommandInvocation, label: &str) -> Option<String> {
    let value = command
        .values
        .get(label)
        .and_then(|value| value.as_str())
        .or(command.args.as_deref())
        .unwrap_or("")
        .trim();
    (!value.is_empty()).then(|| value.to_string())
}

fn codex_permission_preset_arg(command: &AgentCommandInvocation) -> Option<String> {
    let value = command
        .values
        .get("permission")
        .or_else(|| command.values.get("mode"))
        .and_then(Value::as_str)
        .or(command.args.as_deref())
        .unwrap_or("")
        .trim();
    (!value.is_empty()).then(|| value.to_string())
}

fn codex_fork_name(command: &AgentCommandInvocation) -> Option<String> {
    command
        .values
        .get("target_id")
        .and_then(Value::as_str)
        .or_else(|| command.values.get("name").and_then(Value::as_str))
        .or(command.args.as_deref())
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(Into::into)
}

fn codex_model_args(command: &AgentCommandInvocation) -> Result<Option<(String, Option<String>)>> {
    let value_model = command
        .values
        .get("model")
        .and_then(Value::as_str)
        .map(str::trim)
        .filter(|value| !value.is_empty());
    let value_reasoning = command
        .values
        .get("reasoning")
        .or_else(|| command.values.get("effort"))
        .and_then(Value::as_str)
        .map(str::trim)
        .filter(|value| !value.is_empty());
    if let Some(model) = value_model {
        let reasoning = value_reasoning.map(validate_codex_reasoning).transpose()?;
        return Ok(Some((normalize_model_alias(model), reasoning)));
    }

    let raw = command.args.as_deref().unwrap_or("").trim();
    if raw.is_empty() {
        return Ok(None);
    }
    let mut parts = raw.split_whitespace();
    let model = parts.next().unwrap_or_default();
    if model == "reason" || model == "reasoning" {
        return Err(LucarneError::dialect(
            "codex: reasoning is configured through /model <model> [reasoning]",
        ));
    }
    let reasoning = parts.next().map(validate_codex_reasoning).transpose()?;
    if parts.next().is_some() {
        return Err(LucarneError::dialect(
            "codex: /model expects <model> [reasoning]",
        ));
    }
    Ok(Some((normalize_model_alias(model), reasoning)))
}

fn validate_codex_reasoning(value: &str) -> Result<String> {
    if ["low", "medium", "high", "xhigh"].contains(&value) {
        Ok(value.to_string())
    } else {
        Err(LucarneError::dialect(format!(
            "codex: unsupported reasoning effort {value:?}; expected one of low, medium, high, xhigh"
        )))
    }
}

fn normalize_model_alias(value: &str) -> String {
    let value = value.trim();
    if let Some(rest) = value.strip_prefix("gpt") {
        if !rest.starts_with('-') && rest.chars().next().is_some_and(|ch| ch.is_ascii_digit()) {
            return format!("gpt-{rest}");
        }
    }
    value.to_string()
}

fn codex_thread_id(result: &Value) -> String {
    let top = result
        .get("threadId")
        .and_then(|v| v.as_str())
        .unwrap_or("")
        .to_string();
    if !top.is_empty() {
        top
    } else {
        result
            .pointer("/thread/id")
            .and_then(|v| v.as_str())
            .unwrap_or("")
            .to_string()
    }
}

fn codex_turn_id(result: &Value) -> Option<&str> {
    result
        .get("turnId")
        .and_then(|v| v.as_str())
        .filter(|s| !s.is_empty())
        .or_else(|| {
            result
                .pointer("/turn/id")
                .and_then(|v| v.as_str())
                .filter(|s| !s.is_empty())
        })
}

fn notification_turn_id(params: &Map<String, Value>) -> Option<String> {
    params
        .get("turnId")
        .and_then(|v| v.as_str())
        .filter(|s| !s.is_empty())
        .map(ToString::to_string)
        .or_else(|| {
            params
                .get("turn")
                .and_then(|turn| turn.get("id"))
                .and_then(|v| v.as_str())
                .filter(|s| !s.is_empty())
                .map(ToString::to_string)
        })
}

fn codex_fork_choices(result: &Value) -> Vec<CodexForkChoice> {
    const MAX_FORK_TARGETS: usize = 20;
    let Some(turns) = result
        .pointer("/thread/turns")
        .or_else(|| result.get("turns"))
        .and_then(Value::as_array)
    else {
        return Vec::new();
    };

    let total_turns = turns.len();
    let mut raw = Vec::new();
    for (turn_index, turn) in turns.iter().enumerate() {
        let turn_number = turn_index + 1;
        let rollback_turns = total_turns.saturating_sub(turn_number) as u64;
        let Some(items) = turn.get("items").and_then(Value::as_array) else {
            continue;
        };
        let mut latest_user_message = None;
        let mut user_message_count = 0usize;
        for item in items {
            if item.get("type").and_then(Value::as_str) != Some("userMessage") {
                continue;
            }
            let Some(id) = item
                .get("id")
                .and_then(Value::as_str)
                .map(str::trim)
                .filter(|value| !value.is_empty())
            else {
                continue;
            };
            user_message_count += 1;
            latest_user_message = Some((id.to_string(), codex_user_message_preview(item)));
        }
        if let Some((id, preview)) = latest_user_message {
            raw.push((id, turn_number, preview, rollback_turns, user_message_count));
        }
    }

    let skip = raw.len().saturating_sub(MAX_FORK_TARGETS);
    raw.into_iter()
        .skip(skip)
        .map(
            |(id, turn_number, preview, rollback_turns, user_message_count)| {
                let label = if preview.is_empty() {
                    format!("turn {turn_number}")
                } else {
                    preview
                };
                let description = if user_message_count > 1 {
                    format!("turn {turn_number} · {user_message_count} user messages")
                } else {
                    format!("turn {turn_number}")
                };
                CodexForkChoice {
                    target: AgentForkTarget {
                        id: id.into(),
                        label: Some(label.into()),
                        description: Some(description.into()),
                    },
                    rollback_turns,
                }
            },
        )
        .collect()
}

fn codex_user_message_preview(item: &Value) -> String {
    let mut parts = Vec::new();
    if let Some(content) = item.get("content").and_then(Value::as_array) {
        for entry in content {
            if entry.get("type").and_then(Value::as_str) == Some("text") {
                if let Some(text) = entry.get("text").and_then(Value::as_str) {
                    parts.push(text);
                }
            }
        }
    }
    if parts.is_empty() {
        if let Some(text) = item.get("text").and_then(Value::as_str) {
            parts.push(text);
        }
    }
    compact_codex_list_text(&parts.join(" "))
}

fn codex_fork_rollback_marker(method: &str) -> Option<u64> {
    let mut parts = method.split('|');
    if parts.next()? != "thread/fork" || parts.next()? != "fork" {
        return None;
    }
    parts.next()?.parse().ok()
}

fn codex_config_write_params(key_path: &str, value: &str) -> Value {
    codex_config_write_value_params(key_path, json!(value))
}

fn codex_config_write_value_params(key_path: &str, value: Value) -> Value {
    json!({
        "keyPath": key_path,
        "value": value,
        "mergeStrategy": "replace",
    })
}

fn codex_config_batch_write_params(edits: Vec<(&str, Value)>) -> Value {
    let edits = edits
        .into_iter()
        .map(|(key_path, value)| {
            json!({
                "keyPath": key_path,
                "value": value,
                "mergeStrategy": "replace",
            })
        })
        .collect::<Vec<_>>();
    json!({
        "edits": edits,
    })
}

fn codex_command_result_method(method: &str) -> &str {
    match method {
        "skills/list|cwds" | "skills/list|cwd" => "skills/list",
        _ => method,
    }
}

fn codex_should_retry_skills_list_with_cwd_fallback(method: &str, err: &Value) -> bool {
    if method != "skills/list|cwds" {
        return false;
    }
    let Some(code) = err.get("code").and_then(Value::as_i64) else {
        return false;
    };
    if code != -32600 && code != -32602 {
        return false;
    }
    let message = err
        .get("message")
        .and_then(Value::as_str)
        .unwrap_or_default()
        .to_ascii_lowercase();
    message.contains("invalid")
        || message.contains("unknown field")
        || message.contains("unrecognized field")
        || message.contains("missing field")
}

fn codex_command_response_text(
    method: &str,
    result: &Value,
    fallback_permission_preset: &str,
) -> Option<String> {
    let method = codex_command_result_method(method);
    let label = match method {
        "thread/name/set" => return Some("Renamed thread.".into()),
        "thread/goal/get" => return Some(codex_goal_get_text(result)),
        "thread/goal/set" => return Some(codex_goal_set_text(result)),
        "thread/goal/clear" => return Some(codex_goal_clear_text(result)),
        "thread/backgroundTerminals/clean" => {
            return Some("Stopping all background terminals.".into())
        }
        "thread/read" => "Thread status",
        "model/list" => return Some(codex_models_text(result)),
        "skills/list" => return Some(codex_skills_text(result)),
        "hooks/list" => "Hooks",
        "mcpServerStatus/list" => "MCP servers",
        "app/list" => "Apps",
        "plugin/list" => "Plugins",
        "config/read" => "Permissions",
        "config/read|permissions" => {
            return Some(codex_permission_modes_text(
                result,
                fallback_permission_preset,
            ));
        }
        method if method.starts_with("config/value/write|") => {
            return codex_config_write_response_text(method, result);
        }
        _ => return None,
    };
    Some(format!("{label}:\n{}", pretty_json(result)))
}

fn codex_goal_get_text(result: &Value) -> String {
    let Some(goal) = result.get("goal").filter(|goal| !goal.is_null()) else {
        return "No active goal.".into();
    };
    codex_goal_text("Goal", goal)
}

fn codex_goal_set_text(result: &Value) -> String {
    let Some(goal) = result.get("goal").filter(|goal| !goal.is_null()) else {
        return format!("Updated goal:\n{}", pretty_json(result));
    };
    codex_goal_text("Updated goal", goal)
}

fn codex_goal_clear_text(result: &Value) -> String {
    match result.get("cleared").and_then(Value::as_bool) {
        Some(true) => "Cleared goal.".into(),
        Some(false) => "No active goal to clear.".into(),
        None => format!("Cleared goal:\n{}", pretty_json(result)),
    }
}

fn codex_goal_text(label: &str, goal: &Value) -> String {
    let objective = goal
        .get("objective")
        .and_then(Value::as_str)
        .unwrap_or("")
        .trim();
    let status = goal
        .get("status")
        .and_then(Value::as_str)
        .unwrap_or("")
        .trim();
    let mut lines = vec![label.to_string()];
    if !objective.is_empty() {
        lines.push(format!("Objective: {objective}"));
    }
    if !status.is_empty() {
        lines.push(format!("Status: {status}"));
    }
    if let Some(tokens_used) = goal.get("tokensUsed").and_then(Value::as_i64) {
        lines.push(format!("Tokens used: {tokens_used}"));
    }
    if lines.len() == 1 {
        lines.push(pretty_json(goal));
    }
    lines.join("\n")
}

fn codex_command_result(
    method: &str,
    result: &Value,
    current_model: &str,
    current_reasoning_effort: Option<&str>,
    fallback_permission_preset: &str,
    latest_token_usage: Option<&Value>,
    latest_usage: Option<&Usage>,
    config_result: Option<&Value>,
) -> Option<CommandResult> {
    let method = codex_command_result_method(method);
    match method {
        "thread/read" => Some(CommandResult::Status(codex_status(
            result,
            current_model,
            current_reasoning_effort,
            fallback_permission_preset,
            latest_token_usage,
            latest_usage,
            config_result,
        ))),
        "model/list" => Some(CommandResult::Models(codex_model_catalog(
            result,
            current_model,
        ))),
        "skills/list" => Some(CommandResult::Skills(codex_skill_catalog(result))),
        "config/read|permissions" => Some(CommandResult::Permissions(codex_permission_catalog(
            result,
            fallback_permission_preset,
        ))),
        _ => None,
    }
}

fn codex_status(
    result: &Value,
    current_model: &str,
    current_reasoning_effort: Option<&str>,
    fallback_permission_preset: &str,
    latest_token_usage: Option<&Value>,
    latest_usage: Option<&Usage>,
    config_result: Option<&Value>,
) -> AgentStatus {
    let thread = result.get("thread").unwrap_or(result);
    let config = config_result.map(codex_config_root);
    let null = Value::Null;
    let token_usage = result
        .get("tokenUsage")
        .or_else(|| thread.get("tokenUsage"))
        .or(latest_token_usage)
        .unwrap_or(&null);
    let tokens = codex_status_tokens(token_usage, latest_usage);
    let context = codex_status_context(token_usage);
    let model = string_at(result, &["model"])
        .or_else(|| string_at(thread, &["model"]))
        .or_else(|| (!current_model.trim().is_empty()).then_some(current_model))
        .or_else(|| config.and_then(|value| string_at(value, &["model"])));
    AgentStatus {
        version: string_at(thread, &["cliVersion", "cli_version"]).map(Into::into),
        session_id: string_at(thread, &["id"])
            .or_else(|| string_at(result, &["threadId", "thread_id"]))
            .map(Into::into),
        directory: string_at(result, &["cwd"])
            .or_else(|| string_at(thread, &["cwd"]))
            .map(Into::into),
        model: model.map(Into::into),
        reasoning: codex_reasoning_effort_from_result(result)
            .or_else(|| codex_reasoning_effort_from_result(thread))
            .or(current_reasoning_effort)
            .or_else(|| {
                config.and_then(|value| {
                    codex_reasoning_effort_from_result(value)
                        .or_else(|| string_at(value, &["model_reasoning_effort"]))
                })
            })
            .map(Into::into),
        permissions: Some(
            codex_status_permissions(result, config, fallback_permission_preset).into(),
        ),
        account: string_at(result, &["account"])
            .or_else(|| result.pointer("/auth/account").and_then(Value::as_str))
            .or_else(|| config.and_then(|value| string_at(value, &["account"])))
            .map(Into::into),
        agents_md: string_at(
            result,
            &["agentsMd", "agents_md", "agentsPath", "agents_path"],
        )
        .or_else(|| {
            string_at(
                thread,
                &["agentsMd", "agents_md", "agentsPath", "agents_path"],
            )
        })
        .or_else(|| first_string_in_array(result, &["instructionSources", "instruction_sources"]))
        .or_else(|| first_string_in_array(thread, &["instructionSources", "instruction_sources"]))
        .or_else(|| {
            config.and_then(|value| {
                string_at(
                    value,
                    &["agentsMd", "agents_md", "agentsPath", "agents_path"],
                )
                .or_else(|| {
                    first_string_in_array(value, &["instructionSources", "instruction_sources"])
                })
            })
        })
        .map(Into::into),
        tokens,
        context,
        compactions: u64_at(token_usage, &["compactions"])
            .or_else(|| u64_at(result, &["compactions"])),
        ..Default::default()
    }
}

fn codex_reasoning_effort_from_result(result: &Value) -> Option<&str> {
    string_at(result, &["reasoningEffort", "reasoning_effort"])
        .or_else(|| string_at(result, &["modelReasoningEffort", "model_reasoning_effort"]))
}

fn codex_status_tokens(
    token_usage: &Value,
    latest_usage: Option<&Usage>,
) -> Option<AgentTokenUsage> {
    let total = token_usage.get("total").unwrap_or(token_usage);
    let input = u64_at(total, &["inputTokens", "input_tokens"]);
    let output = u64_at(total, &["outputTokens", "output_tokens"]);
    let total_tokens = u64_at(total, &["totalTokens", "total_tokens"]);
    if input.is_some() || output.is_some() || total_tokens.is_some() {
        return Some(AgentTokenUsage {
            input_tokens: input,
            output_tokens: output,
            total_tokens,
        });
    }
    latest_usage.map(|usage| AgentTokenUsage {
        input_tokens: u64_from_i64(usage.input_tokens),
        output_tokens: u64_from_i64(usage.output_tokens),
        total_tokens: u64_from_i64(
            usage.input_tokens
                + usage.output_tokens
                + usage.cache_read_tokens
                + usage.cache_write_tokens,
        ),
    })
}

fn codex_status_context(token_usage: &Value) -> Option<AgentContextUsage> {
    let current = token_usage
        .get("last")
        .or_else(|| token_usage.get("current"))
        .or_else(|| token_usage.get("total"))
        .unwrap_or(token_usage);
    let used = u64_at(current, &["totalTokens", "total_tokens"]);
    let max = u64_at(token_usage, &["modelContextWindow", "model_context_window"]);
    if used.is_none() && max.is_none() {
        return None;
    }
    let percent = match (used, max) {
        (Some(used), Some(max)) if max > 0 => {
            Some(((used as f64 / max as f64) * 100.0).round() as u8)
        }
        _ => None,
    };
    Some(AgentContextUsage {
        used_tokens: used,
        max_tokens: max,
        percent_used: percent,
    })
}

fn codex_status_permissions(
    result: &Value,
    config: Option<&Value>,
    fallback_permission_preset: &str,
) -> String {
    if let Some(preset) = codex_permission_preset_from_result(result)
        .or_else(|| config.and_then(codex_permission_preset_from_result))
    {
        return preset.display_name.to_string();
    }
    let approval = string_at(result, &["approvalPolicy", "approval_policy"])
        .or_else(|| {
            config.and_then(|value| string_at(value, &["approvalPolicy", "approval_policy"]))
        })
        .or_else(|| {
            result
                .pointer("/config/approval_policy")
                .and_then(Value::as_str)
        })
        .or_else(|| {
            config.and_then(|value| {
                value
                    .pointer("/config/approval_policy")
                    .and_then(Value::as_str)
            })
        });
    let sandbox = result
        .get("sandbox")
        .and_then(|value| {
            value
                .get("type")
                .or_else(|| value.get("mode"))
                .and_then(Value::as_str)
        })
        .or_else(|| string_at(result, &["sandboxMode", "sandbox_mode"]))
        .or_else(|| config.and_then(|value| string_at(value, &["sandboxMode", "sandbox_mode"])))
        .or_else(|| {
            result
                .pointer("/config/sandbox_mode")
                .and_then(Value::as_str)
        })
        .or_else(|| {
            config.and_then(|value| {
                value
                    .pointer("/config/sandbox_mode")
                    .and_then(Value::as_str)
            })
        })
        .map(codex_status_sandbox_label);
    match (sandbox, approval) {
        (Some(sandbox), Some(approval)) => format!("Custom ({sandbox}, {approval})"),
        _ => codex_permission_preset(fallback_permission_preset)
            .map(|preset| preset.display_name.to_string())
            .unwrap_or_else(|_| fallback_permission_preset.to_string()),
    }
}

fn codex_status_sandbox_label(value: &str) -> String {
    match value {
        "workspaceWrite" => "workspace-write".into(),
        "readOnly" => "read-only".into(),
        "dangerFullAccess" => "danger-full-access".into(),
        other => other.to_string(),
    }
}

fn codex_config_root(value: &Value) -> &Value {
    value.get("config").unwrap_or(value)
}

fn string_at<'a>(value: &'a Value, keys: &[&str]) -> Option<&'a str> {
    keys.iter().find_map(|key| {
        value
            .get(*key)
            .and_then(Value::as_str)
            .filter(|s| !s.trim().is_empty())
    })
}

fn first_string_in_array<'a>(value: &'a Value, keys: &[&str]) -> Option<&'a str> {
    keys.iter().find_map(|key| {
        value
            .get(*key)
            .and_then(Value::as_array)
            .and_then(|items| items.iter().find_map(Value::as_str))
            .filter(|s| !s.trim().is_empty())
    })
}

fn u64_at(value: &Value, keys: &[&str]) -> Option<u64> {
    keys.iter()
        .find_map(|key| value.get(*key).and_then(Value::as_u64))
}

fn u64_from_i64(value: i64) -> Option<u64> {
    (value > 0).then_some(value as u64)
}

fn codex_skill_catalog(result: &Value) -> AgentSkillCatalog {
    let mut by_name: BTreeMap<String, AgentSkillSummary> = BTreeMap::new();
    for skill in codex_skill_values(result)
        .into_iter()
        .map(codex_skill_summary)
        .filter(|skill| !skill.name.trim().is_empty())
    {
        let key = skill.name.trim().to_ascii_lowercase();
        match by_name.get(&key) {
            Some(existing) if existing.enabled != Some(true) && skill.enabled == Some(true) => {
                by_name.insert(key, skill);
            }
            None => {
                by_name.insert(key, skill);
            }
            _ => {}
        }
    }
    AgentSkillCatalog {
        skills: by_name.into_values().collect(),
    }
}

fn codex_skill_values(result: &Value) -> Vec<&Value> {
    let mut raw_skills = Vec::new();
    if let Some(data) = result.get("data").and_then(Value::as_array) {
        let before = raw_skills.len();
        for group in data {
            if let Some(items) = group.get("skills").and_then(Value::as_array) {
                raw_skills.extend(items.iter());
            }
        }
        if raw_skills.len() == before {
            raw_skills.extend(data.iter().filter(|item| codex_skill_name(item).is_some()));
        }
    }
    if raw_skills.is_empty() {
        if let Some(items) = result.get("skills").and_then(Value::as_array) {
            raw_skills.extend(items.iter());
        }
    }
    raw_skills
}

fn codex_skill_summary(skill: &Value) -> AgentSkillSummary {
    let name = codex_skill_name(skill).unwrap_or_default();
    let title = skill
        .pointer("/interface/displayName")
        .and_then(Value::as_str)
        .or_else(|| skill.get("displayName").and_then(Value::as_str))
        .or_else(|| skill.get("title").and_then(Value::as_str))
        .filter(|value| !value.trim().is_empty());
    let intro = skill
        .pointer("/interface/shortDescription")
        .and_then(Value::as_str)
        .or_else(|| skill.get("shortDescription").and_then(Value::as_str))
        .or_else(|| skill.get("description").and_then(Value::as_str))
        .map(compact_codex_skill_intro)
        .filter(|value| !value.is_empty());
    AgentSkillSummary {
        name: name.trim().into(),
        display_name: title.map(|value| value.trim().to_string().into()),
        description: intro.map(Into::into),
        path: skill
            .get("path")
            .and_then(Value::as_str)
            .map(str::trim)
            .filter(|value| !value.is_empty())
            .map(|value| value.to_string().into()),
        scope: skill
            .get("scope")
            .and_then(Value::as_str)
            .map(str::trim)
            .filter(|value| !value.is_empty())
            .map(|value| value.to_string().into()),
        source: skill
            .get("source")
            .and_then(Value::as_str)
            .map(str::trim)
            .filter(|value| !value.is_empty())
            .map(|value| value.to_string().into()),
        tokens: skill.get("tokens").and_then(Value::as_u64),
        enabled: skill.get("enabled").and_then(Value::as_bool),
    }
}

fn codex_skill_name(skill: &Value) -> Option<&str> {
    skill
        .get("name")
        .and_then(Value::as_str)
        .map(str::trim)
        .filter(|value| !value.is_empty())
}

fn codex_model_catalog(result: &Value, current_model: &str) -> AgentModelCatalog {
    let models = result
        .get("data")
        .or_else(|| result.get("models"))
        .and_then(Value::as_array)
        .cloned()
        .unwrap_or_default();
    AgentModelCatalog {
        current_model: (!current_model.trim().is_empty()).then(|| current_model.into()),
        current_reasoning: None,
        models: models.iter().map(codex_model_option).collect(),
    }
}

fn codex_model_option(model: &Value) -> AgentModelOption {
    let id = model
        .get("id")
        .or_else(|| model.get("model"))
        .and_then(Value::as_str)
        .filter(|value| !value.trim().is_empty())
        .unwrap_or("(unnamed)")
        .trim();
    let description = model
        .get("description")
        .and_then(Value::as_str)
        .map(compact_codex_list_text)
        .filter(|value| !value.is_empty());
    let default = model
        .get("defaultReasoningEffort")
        .and_then(Value::as_str)
        .unwrap_or("");
    let supported_reasoning = model
        .get("supportedReasoningEfforts")
        .and_then(Value::as_array)
        .into_iter()
        .flat_map(|items| items.iter())
        .filter_map(|item| {
            item.as_str().or_else(|| {
                item.get("reasoningEffort")
                    .or_else(|| item.get("id"))
                    .and_then(Value::as_str)
            })
        })
        .filter(|value| !value.trim().is_empty())
        .map(|value| AgentReasoningOption {
            value: value.trim().into(),
            description: None,
            is_default: Some(value == default),
        })
        .collect();
    AgentModelOption {
        id: id.into(),
        display_name: None,
        description: description.map(Into::into),
        supported_reasoning,
    }
}

fn codex_permission_catalog(
    result: &Value,
    fallback_permission_preset: &str,
) -> AgentPermissionCatalog {
    let current = codex_permission_preset_from_result(result)
        .map(|preset| preset.id)
        .unwrap_or(fallback_permission_preset);
    AgentPermissionCatalog {
        current_mode: Some(current.into()),
        modes: CODEX_PERMISSION_PRESETS
            .iter()
            .map(|preset| AgentPermissionOption {
                id: preset.id.into(),
                display_name: Some(preset.display_name.into()),
                description: Some(preset.description.into()),
            })
            .collect(),
    }
}

fn codex_skills_text(result: &Value) -> String {
    let catalog = codex_skill_catalog(result);
    if catalog.skills.is_empty() {
        return "Available skills:\n(none)".into();
    }

    let mut lines = vec!["Available skills:".to_string()];
    for (index, skill) in catalog.skills.iter().enumerate() {
        let title = skill.display_name.as_deref().unwrap_or(skill.name.as_str());
        let intro = skill
            .description
            .as_deref()
            .unwrap_or("No description provided.");

        lines.push(format!(
            "{}. {}\n   `{}`\n   {}",
            index + 1,
            title.trim(),
            skill.name.trim(),
            intro
        ));
    }
    lines.join("\n")
}

fn codex_models_text(result: &Value) -> String {
    let models = result
        .get("data")
        .or_else(|| result.get("models"))
        .and_then(Value::as_array)
        .cloned()
        .unwrap_or_default();
    if models.is_empty() {
        return "Available models:\n(none)".into();
    }

    let mut lines = vec!["Available models:".to_string()];
    for (index, model) in models.iter().enumerate() {
        let id = model
            .get("id")
            .or_else(|| model.get("model"))
            .and_then(Value::as_str)
            .filter(|value| !value.trim().is_empty())
            .unwrap_or("(unnamed)");
        let description = model
            .get("description")
            .and_then(Value::as_str)
            .map(compact_codex_list_text)
            .filter(|value| !value.is_empty());

        lines.push(format!("{}. `{}`", index + 1, id.trim()));
        if let Some(description) = description {
            lines.push(format!("   {description}"));
        }
        let reasoning = codex_model_reasoning_efforts(model);
        if !reasoning.is_empty() {
            lines.push(format!("   reasoning: {}", reasoning.join(", ")));
        }
    }
    lines.join("\n")
}

fn codex_model_reasoning_efforts(model: &Value) -> Vec<String> {
    let default = model
        .get("defaultReasoningEffort")
        .and_then(Value::as_str)
        .unwrap_or("");
    model
        .get("supportedReasoningEfforts")
        .and_then(Value::as_array)
        .into_iter()
        .flat_map(|items| items.iter())
        .filter_map(|item| {
            item.as_str().or_else(|| {
                item.get("reasoningEffort")
                    .or_else(|| item.get("id"))
                    .and_then(Value::as_str)
            })
        })
        .filter(|value| !value.trim().is_empty())
        .map(|value| {
            if value == default {
                format!("`{}` (default)", value.trim())
            } else {
                format!("`{}`", value.trim())
            }
        })
        .collect()
}

fn compact_codex_skill_intro(value: &str) -> String {
    compact_codex_list_text(value)
}

fn compact_codex_list_text(value: &str) -> String {
    const MAX_CHARS: usize = 220;
    let compact = value.split_whitespace().collect::<Vec<_>>().join(" ");
    if compact.chars().count() <= MAX_CHARS {
        return compact;
    }
    let mut shortened = compact.chars().take(MAX_CHARS).collect::<String>();
    shortened.truncate(shortened.trim_end().len());
    format!("{shortened}...")
}

fn codex_config_write_response_text(method: &str, result: &Value) -> Option<String> {
    if result.get("status").and_then(|value| value.as_str()) != Some("ok") {
        return Some(format!("Updated configuration:\n{}", pretty_json(result)));
    }
    let mut parts = method.splitn(3, '|');
    let _ = parts.next();
    let target = parts.next().unwrap_or("");
    let value = parts.next().unwrap_or("");
    match target {
        "model" if !value.is_empty() => Some(format!("Updated model to {value}.")),
        "permissions" if !value.is_empty() => Some(format!("Updated permissions to {value}.")),
        _ => Some("Updated configuration.".into()),
    }
}

fn codex_config_marker_target_value(method: &str) -> Option<(&str, &str)> {
    let mut parts = method.splitn(3, '|');
    let _ = parts.next()?;
    let target = parts.next()?;
    let value = parts.next()?;
    Some((target, value))
}

fn codex_config_verify_events(turn_id: &str, method: &str, result: &Value) -> Vec<Event> {
    if let Some((model, effort)) = codex_verify_model_reasoning_marker(method) {
        if result.pointer("/config/model").and_then(Value::as_str) != Some(model.as_str())
            || result
                .pointer("/config/model_reasoning_effort")
                .and_then(Value::as_str)
                != Some(effort.as_str())
        {
            return vec![Event::new(Payload::TurnFailed(TurnFailed {
                turn_id: turn_id.into(),
                error: format!(
                    "codex config verification failed for model={model} reasoning={effort}"
                ),
                code: String::new(),
            }))];
        }
        return command_result_events(
            turn_id,
            CommandResult::ModelChanged(ModelSelection {
                model: model.into(),
                reasoning: Some(effort.into()),
            }),
        );
    }
    let Some((target, value)) = codex_config_verify_target_value(method) else {
        return vec![Event::new(Payload::TurnFailed(TurnFailed {
            turn_id: turn_id.into(),
            error: "codex config verification marker is invalid".into(),
            code: String::new(),
        }))];
    };
    if !codex_config_value_matches(result, target, value) {
        return vec![Event::new(Payload::TurnFailed(TurnFailed {
            turn_id: turn_id.into(),
            error: format!("codex config verification failed for {target}={value}"),
            code: String::new(),
        }))];
    }
    match target {
        "model" => command_result_events(
            turn_id,
            CommandResult::ModelChanged(ModelSelection {
                model: value.into(),
                reasoning: None,
            }),
        ),
        "permissions" => command_result_events(
            turn_id,
            CommandResult::PermissionsChanged(PermissionSelection { mode: value.into() }),
        ),
        _ => command_result_events(
            turn_id,
            CommandResult::Message(CommandMessage::text("Updated configuration.")),
        ),
    }
}

fn codex_config_verify_target_value(method: &str) -> Option<(&str, &str)> {
    let mut parts = method.splitn(4, '|');
    let _ = parts.next()?;
    if parts.next()? != "verify" {
        return None;
    }
    let target = parts.next()?;
    let value = parts.next()?;
    Some((target, value))
}

fn codex_model_with_reasoning_marker(method: &str) -> Option<(String, String)> {
    let mut parts = method.splitn(4, '|');
    let _ = parts.next()?;
    if parts.next()? != "model_with_reasoning" {
        return None;
    }
    Some((parts.next()?.to_string(), parts.next()?.to_string()))
}

fn codex_reasoning_after_model_marker(method: &str) -> Option<(String, String)> {
    let mut parts = method.splitn(4, '|');
    let _ = parts.next()?;
    if parts.next()? != "reasoning_after_model" {
        return None;
    }
    Some((parts.next()?.to_string(), parts.next()?.to_string()))
}

fn codex_verify_model_reasoning_marker(method: &str) -> Option<(String, String)> {
    let mut parts = method.splitn(5, '|');
    let _ = parts.next()?;
    if parts.next()? != "verify" || parts.next()? != "model_with_reasoning" {
        return None;
    }
    Some((parts.next()?.to_string(), parts.next()?.to_string()))
}

fn codex_config_value_matches(result: &Value, target: &str, value: &str) -> bool {
    match target {
        "model" => result.pointer("/config/model").and_then(Value::as_str) == Some(value),
        _ => false,
    }
}

fn codex_permission_modes_text(result: &Value, fallback_permission_preset: &str) -> String {
    let catalog = codex_permission_catalog(result, fallback_permission_preset);
    let mut lines = vec![
        "Permission modes:".to_string(),
        format!(
            "current: `{}`",
            catalog.current_mode.as_deref().unwrap_or("unknown")
        ),
    ];
    for (index, mode) in catalog.modes.iter().enumerate() {
        let mut line = format!("{}. `{}`", index + 1, mode.id);
        if let Some(name) = &mode.display_name {
            if name.as_str() != mode.id.as_str() {
                line.push_str(" — ");
                line.push_str(name);
            }
        }
        if let Some(description) = &mode.description {
            line.push_str(" — ");
            line.push_str(description);
        }
        lines.push(line);
    }
    lines.join("\n")
}

fn codex_permission_preset_from_result(result: &Value) -> Option<&'static CodexPermissionPreset> {
    let approval_policy = codex_result_str(result, "approval_policy", "approvalPolicy");
    let approvals_reviewer = codex_result_str(result, "approvals_reviewer", "approvalsReviewer");
    let sandbox_mode = codex_result_sandbox_mode(result);
    let has_permission_profile = codex_result_has_permission_profile(result);

    if codex_active_permission_profile_id(result) == Some(CODEX_BYPASS_PERMISSION_PROFILE_ID)
        || (!has_permission_profile
            && codex_result_default_permissions(result) == Some(CODEX_BYPASS_PERMISSION_PROFILE_ID))
    {
        return codex_permission_preset("bypass").ok();
    }
    if matches!(
        sandbox_mode,
        Some("danger-full-access" | "dangerFullAccess")
    ) || codex_permission_profile_is_disabled(result)
        || codex_permission_profile_is_full_access(result)
    {
        return codex_permission_preset("full-access").ok();
    }
    if matches!(
        approvals_reviewer,
        Some("guardian_subagent" | "auto-review" | "auto-reviewer" | "auto_review")
    ) {
        return codex_permission_preset("auto-review").ok();
    }
    match approval_policy {
        Some("never") if !has_permission_profile => codex_permission_preset("full-access").ok(),
        Some("untrusted") => codex_permission_preset("default").ok(),
        _ => None,
    }
}

fn codex_result_default_permissions(result: &Value) -> Option<&str> {
    string_at(result, &["defaultPermissions", "default_permissions"])
        .or_else(|| {
            result
                .pointer("/config/default_permissions")
                .and_then(Value::as_str)
        })
        .or_else(|| {
            result
                .pointer("/config/defaultPermissions")
                .and_then(Value::as_str)
        })
}

fn codex_active_permission_profile_id(result: &Value) -> Option<&str> {
    result
        .get("activePermissionProfile")
        .or_else(|| result.get("active_permission_profile"))
        .and_then(|profile| profile.get("id"))
        .and_then(Value::as_str)
}

fn codex_result_has_permission_profile(result: &Value) -> bool {
    result
        .get("permissionProfile")
        .or_else(|| result.get("permission_profile"))
        .is_some()
}

fn codex_permission_profile_is_disabled(result: &Value) -> bool {
    result
        .get("permissionProfile")
        .or_else(|| result.get("permission_profile"))
        .and_then(|profile| profile.get("type"))
        .and_then(Value::as_str)
        .is_some_and(|kind| matches!(kind, "disabled" | "Disabled"))
}

fn codex_result_str<'a>(result: &'a Value, snake: &str, camel: &str) -> Option<&'a str> {
    result
        .pointer(&format!("/config/{snake}"))
        .or_else(|| result.get(snake))
        .or_else(|| result.get(camel))
        .and_then(Value::as_str)
        .filter(|value| !value.trim().is_empty())
}

fn codex_result_sandbox_mode(result: &Value) -> Option<&str> {
    codex_result_str(result, "sandbox_mode", "sandboxPolicy")
        .or_else(|| codex_result_str(result, "sandbox", "sandbox"))
        .or_else(|| {
            result
                .get("sandbox")
                .or_else(|| result.get("sandboxPolicy"))
                .and_then(|sandbox| sandbox.get("type"))
                .and_then(Value::as_str)
        })
}

fn codex_permission_profile_is_full_access(result: &Value) -> bool {
    result
        .get("permissionProfile")
        .or_else(|| result.get("permission_profile"))
        .and_then(|profile| profile.get("network"))
        .and_then(|network| network.get("enabled"))
        .and_then(Value::as_bool)
        == Some(true)
}

fn codex_permission_preset_from_session_mode(mode: PermissionMode) -> &'static str {
    match mode {
        PermissionMode::Bypass => "bypass",
        PermissionMode::Full => "full-access",
        _ => "default",
    }
}

fn codex_permission_preset(raw: &str) -> Result<&'static CodexPermissionPreset> {
    let normalized = raw.trim().to_ascii_lowercase();
    let id = match normalized.as_str() {
        "default" | "auto" => "default",
        "auto-review" | "auto_review" | "autoreview" => "auto-review",
        "full-access" | "full_access" | "full" => "full-access",
        "bypass" | "danger-no-sandbox" | "danger_no_sandbox" => "bypass",
        _ => normalized.as_str(),
    };
    CODEX_PERMISSION_PRESETS
        .iter()
        .find(|preset| preset.id == id)
        .ok_or_else(|| {
            LucarneError::dialect(format!(
                "codex: invalid permission preset {raw:?}; expected one of {}",
                CODEX_PERMISSION_PRESETS
                    .iter()
                    .map(|preset| preset.id)
                    .collect::<Vec<_>>()
                    .join(", ")
            ))
        })
}

fn codex_permission_preset_write(
    preset: &CodexPermissionPreset,
    step: usize,
) -> Option<CodexPermissionPresetWrite> {
    match step {
        0 => Some(CodexPermissionPresetWrite::Single {
            key: "approval_policy",
            value: json!(preset.approval_policy),
        }),
        1 => Some(CodexPermissionPresetWrite::Single {
            key: "approvals_reviewer",
            value: json!(preset.approvals_reviewer),
        }),
        2 => Some(match preset.default_permissions {
            Some(permissions) => CodexPermissionPresetWrite::Batch {
                edits: vec![
                    ("sandbox_mode", Value::Null),
                    ("default_permissions", json!(permissions)),
                ],
            },
            None => CodexPermissionPresetWrite::Batch {
                edits: vec![
                    ("default_permissions", Value::Null),
                    ("sandbox_mode", json!(preset.sandbox_mode)),
                ],
            },
        }),
        _ => None,
    }
}

fn codex_permission_preset_write_marker(
    method: &str,
) -> Option<(&'static CodexPermissionPreset, usize)> {
    let mut parts = method.split('|');
    let _ = parts.next()?;
    if parts.next()? != "permissions_preset" {
        return None;
    }
    let preset = codex_permission_preset(parts.next()?).ok()?;
    let step = parts.next()?.parse().ok()?;
    Some((preset, step))
}

fn codex_permission_preset_verify_marker(method: &str) -> Option<&'static CodexPermissionPreset> {
    let mut parts = method.split('|');
    let _ = parts.next()?;
    if parts.next()? != "verify" || parts.next()? != "permissions_preset" {
        return None;
    }
    codex_permission_preset(parts.next()?).ok()
}

fn codex_permission_preset_matches(result: &Value, preset: &CodexPermissionPreset) -> bool {
    if codex_result_str(result, "approval_policy", "approvalPolicy") != Some(preset.approval_policy)
        || codex_result_str(result, "approvals_reviewer", "approvalsReviewer")
            != Some(preset.approvals_reviewer)
    {
        return false;
    }
    if let Some(default_permissions) = preset.default_permissions {
        return codex_active_permission_profile_id(result) == Some(default_permissions)
            || (!codex_result_has_permission_profile(result)
                && codex_result_default_permissions(result) == Some(default_permissions));
    }
    codex_result_sandbox_mode(result)
        .is_some_and(|mode| codex_sandbox_modes_match(mode, preset.sandbox_mode))
}

fn codex_sandbox_modes_match(actual: &str, expected: &str) -> bool {
    matches!(
        (actual, expected),
        ("workspace-write" | "workspaceWrite", "workspace-write")
            | (
                "danger-full-access" | "dangerFullAccess",
                "danger-full-access"
            )
            | ("read-only" | "readOnly", "read-only")
    )
}

fn codex_permission_preset_updated_events(
    turn_id: &str,
    preset: &CodexPermissionPreset,
) -> Vec<Event> {
    command_result_events(
        turn_id,
        CommandResult::PermissionsChanged(PermissionSelection {
            mode: preset.display_name.into(),
        }),
    )
}

fn pretty_json(value: &Value) -> String {
    serde_json::to_string_pretty(value).unwrap_or_else(|_| value.to_string())
}

fn codex_error_message(err: &Value, fallback: &str) -> String {
    err.get("message")
        .and_then(|value| value.as_str())
        .filter(|message| !message.trim().is_empty())
        .unwrap_or(fallback)
        .to_string()
}

fn normalize_command_name(raw: &str) -> &str {
    raw.trim().trim_start_matches('/')
}

fn codex_approval_policy(mode: PermissionMode) -> &'static str {
    match mode {
        PermissionMode::Default => "untrusted",
        PermissionMode::Write => "on-request",
        PermissionMode::ReadOnly
        | PermissionMode::Auto
        | PermissionMode::Full
        | PermissionMode::Bypass => "never",
    }
}

fn insert_codex_thread_permission_params(m: &mut Map<String, Value>, mode: PermissionMode) {
    if mode == PermissionMode::Bypass {
        m.insert("permissions".into(), codex_bypass_permissions_profile());
    } else {
        m.insert(
            "sandbox".into(),
            Value::String(codex_thread_sandbox(mode).into()),
        );
    }
}

fn insert_codex_turn_permission_params(m: &mut Map<String, Value>, mode: PermissionMode) {
    if mode == PermissionMode::Bypass {
        m.insert("permissions".into(), codex_bypass_permissions_profile());
    } else {
        m.insert("sandboxPolicy".into(), codex_turn_sandbox_policy(mode));
    }
}

fn codex_bypass_permissions_profile() -> Value {
    json!({
        "type": "profile",
        "id": CODEX_BYPASS_PERMISSION_PROFILE_ID,
    })
}

fn codex_thread_sandbox(mode: PermissionMode) -> &'static str {
    match mode {
        PermissionMode::Default | PermissionMode::Write | PermissionMode::Auto => "workspace-write",
        PermissionMode::ReadOnly => "read-only",
        PermissionMode::Full | PermissionMode::Bypass => "danger-full-access",
    }
}

fn codex_turn_sandbox_policy(mode: PermissionMode) -> Value {
    match mode {
        PermissionMode::Default | PermissionMode::Write | PermissionMode::Auto => {
            json!({"type": "workspaceWrite", "networkAccess": false})
        }
        PermissionMode::ReadOnly => json!({"type": "readOnly", "networkAccess": false}),
        PermissionMode::Full | PermissionMode::Bypass => json!({"type": "dangerFullAccess"}),
    }
}

fn is_raw_codex_notification(method: &str) -> bool {
    matches!(
        method,
        "turn/started"
            | "turn/completed"
            | "turn/failed"
            | "turn_aborted"
            | "thread/started"
            | "thread/status/changed"
            | "thread/tokenUsage/updated"
            | "error"
    ) || method.starts_with("item/")
}

fn normalize_item_type(s: &str) -> String {
    match s {
        "agentMessage" => "agent_message".into(),
        "commandExecution" => "command_execution".into(),
        "fileChange" => "file_change".into(),
        "imageGeneration" => "image_generation".into(),
        other => other.into(),
    }
}

fn is_file_change_approval_method(method: &str) -> bool {
    matches!(
        method,
        "item/fileChange/requestApproval" | "applyPatchApproval"
    )
}

fn codex_agent_message_phase_is_commentary(phase: Option<&Value>) -> bool {
    phase.and_then(Value::as_str) == Some("commentary")
}

#[derive(Debug, Clone, Default)]
struct CodexSubagentRow {
    thread_id: Option<String>,
    agent_id: Option<String>,
    nickname: Option<String>,
    role: Option<String>,
    model: Option<String>,
    prompt: Option<String>,
    status: Option<String>,
    message: Option<String>,
}

fn codex_is_subagent_item_type(raw: &str) -> bool {
    let normalized = raw.trim().to_ascii_lowercase().replace(['_', '-'], "");
    normalized == "collabagenttoolcall"
        || normalized == "collabtoolcall"
        || normalized.starts_with("collabagentspawn")
        || normalized.starts_with("collabwaiting")
        || normalized.starts_with("collabclose")
        || normalized.starts_with("collabresume")
        || normalized.starts_with("collabagentinteraction")
}

fn codex_subagent_tool_calls(item: &Value) -> Vec<ToolCall> {
    let raw_type = string_at(item, &["type"]).unwrap_or("");
    if !codex_is_subagent_item_type(raw_type) {
        return Vec::new();
    }

    let tool = string_at(item, &["tool", "name"])
        .map(ToString::to_string)
        .or_else(|| codex_subagent_tool_from_type(raw_type))
        .unwrap_or_else(|| "spawnAgent".into());
    let status = string_at(item, &["status"])
        .map(ToString::to_string)
        .unwrap_or_else(|| "in_progress".into());
    let prompt = string_at(item, &["prompt", "task", "message"]).map(ToString::to_string);
    let model = string_at(
        item,
        &[
            "model",
            "modelName",
            "model_name",
            "requestedModel",
            "requested_model",
        ],
    )
    .map(ToString::to_string);

    let mut rows = codex_subagent_rows(item);
    if rows.is_empty() {
        if prompt.is_none() && model.is_none() {
            return Vec::new();
        }
        rows.push(CodexSubagentRow {
            prompt: prompt.clone(),
            model: model.clone(),
            status: Some(status.clone()),
            ..Default::default()
        });
    }

    rows.into_iter()
        .map(|row| {
            let child_thread_id = row.thread_id.clone();
            let call = event::SubAgentCall {
                tool_name: tool.clone().into(),
                prompt: row.prompt.or_else(|| prompt.clone()).map(Into::into),
                requested_model: row.model.or_else(|| model.clone()).map(Into::into),
                child_session_ref: child_thread_id.clone().map(Into::into),
                child_thread_id: child_thread_id.map(Into::into),
                agent_id: row.agent_id.map(Into::into),
                nickname: row.nickname.map(Into::into),
                role: row.role.map(Into::into),
                status: row.status.or_else(|| Some(status.clone())).map(Into::into),
                message: row.message.map(Into::into),
                raw: item.clone(),
            };
            event::tool_call(
                "sub_agent",
                serde_json::to_value(call).expect("serialize sub-agent call"),
            )
        })
        .collect()
}

fn codex_subagent_tool_from_type(raw_type: &str) -> Option<String> {
    let normalized = raw_type.trim().to_ascii_lowercase().replace(['_', '-'], "");
    if normalized.contains("spawn") {
        Some("spawnAgent".into())
    } else if normalized.contains("waiting") || normalized.contains("wait") {
        Some("wait".into())
    } else if normalized.contains("close") {
        Some("closeAgent".into())
    } else if normalized.contains("resume") {
        Some("resumeAgent".into())
    } else if normalized.contains("sendinput") || normalized.contains("interaction") {
        Some("sendInput".into())
    } else {
        None
    }
}

fn codex_subagent_rows(item: &Value) -> Vec<CodexSubagentRow> {
    let receiver_thread_ids = codex_subagent_receiver_thread_ids(item);
    let mut rows = codex_subagent_receiver_agents(item, &receiver_thread_ids);
    for thread_id in receiver_thread_ids {
        if !rows
            .iter()
            .any(|row| row.thread_id.as_deref() == Some(thread_id.as_str()))
        {
            rows.push(CodexSubagentRow {
                thread_id: Some(thread_id),
                ..Default::default()
            });
        }
    }
    for state in codex_subagent_agent_states(item) {
        let Some(thread_id) = state.thread_id.as_deref() else {
            continue;
        };
        if let Some(existing) = rows
            .iter_mut()
            .find(|row| row.thread_id.as_deref() == Some(thread_id))
        {
            if existing.status.is_none() {
                existing.status = state.status;
            }
            if existing.message.is_none() {
                existing.message = state.message;
            }
        } else {
            rows.push(state);
        }
    }
    rows
}

fn codex_subagent_receiver_thread_ids(item: &Value) -> Vec<String> {
    let mut ids = Vec::new();
    for key in [
        "receiverThreadIds",
        "receiver_thread_ids",
        "threadIds",
        "thread_ids",
    ] {
        if let Some(items) = item.get(key).and_then(Value::as_array) {
            for value in items {
                if let Some(thread_id) = value.as_str().map(str::trim).filter(|s| !s.is_empty()) {
                    let thread_id = thread_id.to_string();
                    if !ids.contains(&thread_id) {
                        ids.push(thread_id);
                    }
                }
            }
        }
    }
    if !ids.is_empty() {
        return ids;
    }
    string_at(
        item,
        &[
            "receiverThreadId",
            "receiver_thread_id",
            "threadId",
            "thread_id",
            "newThreadId",
            "new_thread_id",
        ],
    )
    .map(|thread_id| vec![thread_id.to_string()])
    .unwrap_or_default()
}

fn codex_subagent_receiver_agents(
    item: &Value,
    fallback_thread_ids: &[String],
) -> Vec<CodexSubagentRow> {
    for key in ["receiverAgents", "receiver_agents", "agents"] {
        let Some(items) = item.get(key).and_then(Value::as_array) else {
            continue;
        };
        let rows = items
            .iter()
            .enumerate()
            .filter_map(|(idx, value)| {
                let _object = value.as_object()?;
                let fallback = fallback_thread_ids.get(idx).map(String::as_str);
                let thread_id = string_at(
                    value,
                    &[
                        "threadId",
                        "thread_id",
                        "receiverThreadId",
                        "receiver_thread_id",
                        "newThreadId",
                        "new_thread_id",
                    ],
                )
                .or(fallback)
                .map(ToString::to_string)?;
                Some(CodexSubagentRow {
                    thread_id: Some(thread_id),
                    agent_id: string_at(
                        value,
                        &[
                            "agentId",
                            "agent_id",
                            "receiverAgentId",
                            "receiver_agent_id",
                            "newAgentId",
                            "new_agent_id",
                            "id",
                        ],
                    )
                    .map(ToString::to_string),
                    nickname: string_at(
                        value,
                        &[
                            "agentNickname",
                            "agent_nickname",
                            "receiverAgentNickname",
                            "receiver_agent_nickname",
                            "newAgentNickname",
                            "new_agent_nickname",
                            "nickname",
                            "name",
                        ],
                    )
                    .map(ToString::to_string),
                    role: string_at(
                        value,
                        &[
                            "agentRole",
                            "agent_role",
                            "receiverAgentRole",
                            "receiver_agent_role",
                            "newAgentRole",
                            "new_agent_role",
                            "agentType",
                            "agent_type",
                        ],
                    )
                    .map(ToString::to_string),
                    model: string_at(
                        value,
                        &[
                            "modelProvider",
                            "model_provider",
                            "modelProviderId",
                            "model_provider_id",
                            "modelName",
                            "model_name",
                            "model",
                        ],
                    )
                    .map(ToString::to_string),
                    prompt: string_at(
                        value,
                        &["prompt", "instructions", "instruction", "task", "message"],
                    )
                    .map(ToString::to_string),
                    ..Default::default()
                })
            })
            .collect::<Vec<_>>();
        if !rows.is_empty() {
            return rows;
        }
    }

    let Some(thread_id) = fallback_thread_ids.first().cloned().or_else(|| {
        string_at(
            item,
            &[
                "receiverThreadId",
                "receiver_thread_id",
                "threadId",
                "thread_id",
                "newThreadId",
                "new_thread_id",
            ],
        )
        .map(ToString::to_string)
    }) else {
        return Vec::new();
    };

    vec![CodexSubagentRow {
        thread_id: Some(thread_id),
        agent_id: string_at(item, &["newAgentId", "new_agent_id", "agentId", "agent_id"])
            .map(ToString::to_string),
        nickname: string_at(
            item,
            &[
                "newAgentNickname",
                "new_agent_nickname",
                "agentNickname",
                "agent_nickname",
                "receiverAgentNickname",
                "receiver_agent_nickname",
            ],
        )
        .map(ToString::to_string),
        role: string_at(
            item,
            &[
                "receiverAgentRole",
                "receiver_agent_role",
                "newAgentRole",
                "new_agent_role",
                "agentRole",
                "agent_role",
                "agentType",
                "agent_type",
            ],
        )
        .map(ToString::to_string),
        model: string_at(
            item,
            &[
                "modelProvider",
                "model_provider",
                "modelProviderId",
                "model_provider_id",
                "modelName",
                "model_name",
                "model",
            ],
        )
        .map(ToString::to_string),
        prompt: string_at(
            item,
            &["prompt", "instructions", "instruction", "task", "message"],
        )
        .map(ToString::to_string),
        ..Default::default()
    }]
}

fn codex_subagent_agent_states(item: &Value) -> Vec<CodexSubagentRow> {
    for key in [
        "statuses",
        "agentsStates",
        "agents_states",
        "agentStates",
        "agent_states",
    ] {
        let Some(value) = item.get(key) else {
            continue;
        };
        if let Some(object) = value.as_object() {
            return object
                .iter()
                .filter_map(|(raw_thread_id, state)| {
                    let thread_id = raw_thread_id
                        .trim()
                        .is_empty()
                        .then(|| {
                            string_at(state, &["threadId", "thread_id"]).map(ToString::to_string)
                        })
                        .flatten()
                        .or_else(|| Some(raw_thread_id.to_string()))?;
                    Some(CodexSubagentRow {
                        thread_id: Some(thread_id),
                        agent_id: string_at(state, &["agentId", "agent_id"])
                            .map(ToString::to_string),
                        nickname: string_at(
                            state,
                            &[
                                "agentNickname",
                                "agent_nickname",
                                "receiverAgentNickname",
                                "receiver_agent_nickname",
                            ],
                        )
                        .map(ToString::to_string),
                        role: string_at(
                            state,
                            &[
                                "agentRole",
                                "agent_role",
                                "receiverAgentRole",
                                "receiver_agent_role",
                                "agentType",
                                "agent_type",
                            ],
                        )
                        .map(ToString::to_string),
                        status: string_at(state, &["status"]).map(ToString::to_string),
                        message: string_at(state, &["message", "text", "delta", "summary"])
                            .map(ToString::to_string),
                        ..Default::default()
                    })
                })
                .collect();
        }
        if let Some(items) = value.as_array() {
            return items
                .iter()
                .filter_map(|state| {
                    Some(CodexSubagentRow {
                        thread_id: string_at(state, &["threadId", "thread_id"])
                            .map(ToString::to_string),
                        agent_id: string_at(state, &["agentId", "agent_id"])
                            .map(ToString::to_string),
                        nickname: string_at(
                            state,
                            &[
                                "agentNickname",
                                "agent_nickname",
                                "receiverAgentNickname",
                                "receiver_agent_nickname",
                            ],
                        )
                        .map(ToString::to_string),
                        role: string_at(
                            state,
                            &[
                                "agentRole",
                                "agent_role",
                                "receiverAgentRole",
                                "receiver_agent_role",
                                "agentType",
                                "agent_type",
                            ],
                        )
                        .map(ToString::to_string),
                        status: string_at(state, &["status"]).map(ToString::to_string),
                        message: string_at(state, &["message", "text", "delta", "summary"])
                            .map(ToString::to_string),
                        ..Default::default()
                    })
                })
                .collect();
        }
    }
    Vec::new()
}

fn non_neg_delta(curr: i64, prev: i64) -> i64 {
    if curr <= prev {
        0
    } else {
        curr - prev
    }
}
fn non_neg_float_delta(curr: f64, prev: f64) -> f64 {
    if curr <= prev {
        0.0
    } else {
        curr - prev
    }
}

fn first_non_empty<'a>(xs: &[&'a str]) -> &'a str {
    for s in xs {
        if !s.is_empty() {
            return s;
        }
    }
    ""
}

#[derive(Debug, Clone, Default)]
struct FileChangeRec {
    path: String,
    kind: String,
    ty: String,
    unified_diff: String,
    diff: String,
    patch: String,
    content: String,
}

fn first_file_change(raw: Option<&Value>) -> Option<FileChangeRec> {
    let v = raw?;
    // Try array form
    if let Some(arr) = v.as_array() {
        let first = arr.first()?;
        return Some(file_change_from_obj(first));
    }
    // Wrapper { files: [...] }
    if let Some(files) = v.get("files") {
        if let Some(arr) = files.as_array() {
            let first = arr.first()?;
            return Some(file_change_from_obj(first));
        }
    }
    // Object map { "path": {fileChange} }
    if let Some(obj) = v.as_object() {
        if let Some((k, val)) = obj.iter().next() {
            if val.is_string() {
                return Some(FileChangeRec {
                    path: k.clone(),
                    diff: val.as_str().unwrap_or("").to_string(),
                    ..Default::default()
                });
            }
            let mut rec = file_change_from_obj(val);
            if rec.path.is_empty() {
                rec.path = k.clone();
            }
            return Some(rec);
        }
    }
    None
}

fn file_change_from_obj(v: &Value) -> FileChangeRec {
    let s = |k: &str| v.get(k).and_then(|x| x.as_str()).unwrap_or("").to_string();
    let path = {
        let p = s("path");
        if !p.is_empty() {
            p
        } else {
            let a = s("file_path");
            if !a.is_empty() {
                a
            } else {
                s("filePath")
            }
        }
    };
    let unified_diff = {
        let a = s("unified_diff");
        if !a.is_empty() {
            a
        } else {
            s("unifiedDiff")
        }
    };
    FileChangeRec {
        path,
        kind: s("kind"),
        ty: s("type"),
        unified_diff,
        diff: s("diff"),
        patch: s("patch"),
        content: s("content"),
    }
}

fn normalize_patch_envelope(s: &str) -> String {
    if !s.contains("*** Begin Patch") {
        return s.to_string();
    }
    let lines: Vec<&str> = s.trim().split('\n').collect();
    let mut out: Vec<&str> = Vec::with_capacity(lines.len());
    for line in lines {
        if line.starts_with("*** Begin Patch")
            || line.starts_with("*** End Patch")
            || line.starts_with("*** Update File: ")
            || line.starts_with("*** Add File: ")
            || line.starts_with("*** Delete File: ")
            || line.starts_with("*** Move to: ")
        {
            continue;
        }
        out.push(line);
    }
    out.join("\n")
}

fn looks_like_patch(s: &str) -> bool {
    s.contains("@@") || s.contains("--- ") || s.contains("*** Begin Patch")
}

fn file_delete_diff(path: &str, patch: &str, content: &str) -> String {
    if !patch.is_empty() {
        if looks_like_patch(patch) {
            return normalize_patch_envelope(patch);
        }
        let body = patch;
        return file_delete_diff_impl(path, body);
    }
    file_delete_diff_impl(path, content)
}

fn file_delete_diff_impl(path: &str, body: &str) -> String {
    let mut out = String::new();
    out.push_str(&format!("--- a/{}\n", path));
    out.push_str("+++ /dev/null\n");
    if !body.is_empty() {
        out.push_str("@@\n");
        let body = body.trim_end_matches('\n');
        for line in body.split('\n') {
            out.push('-');
            out.push_str(line);
            out.push('\n');
        }
    }
    out
}

fn truncate_canonical_diff(diff: &str) -> String {
    if diff.len() <= MAX_CANONICAL_DIFF_LEN {
        return diff.to_string();
    }
    let head_len = MAX_CANONICAL_DIFF_LEN / 2;
    let tail_len = MAX_CANONICAL_DIFF_LEN / 2;
    let truncated = diff.len().saturating_sub(head_len + tail_len);
    if truncated == 0 {
        return diff.to_string();
    }
    format!(
        "{}\n...[truncated {} chars]...\n{}",
        &diff[..head_len],
        truncated,
        &diff[diff.len() - tail_len..]
    )
}

fn decode_command_execution_command(raw: Option<&Value>) -> String {
    let Some(v) = raw else {
        return String::new();
    };
    if let Some(s) = v.as_str() {
        return unwrap_shell_wrapper_string(s);
    }
    if let Some(arr) = v.as_array() {
        let parts: Vec<String> = arr
            .iter()
            .map(|x| x.as_str().unwrap_or("").to_string())
            .collect();
        return unwrap_shell_wrapper_args(&parts);
    }
    String::new()
}

fn unwrap_shell_wrapper_args(parts: &[String]) -> String {
    if parts.len() >= 3 && is_shell_binary(&parts[0]) && parts[1] == "-lc" {
        return parts[2].trim().to_string();
    }
    parts.join(" ").trim().to_string()
}

fn unwrap_shell_wrapper_string(s: &str) -> String {
    let s = s.trim();
    for marker in [" -lc ", " -c "] {
        if let Some(idx) = s.find(marker) {
            if idx == 0 {
                continue;
            }
            let head = &s[..idx];
            let first = head.split_whitespace().next().unwrap_or("");
            if !is_shell_binary(first) {
                continue;
            }
            let tail = &s[idx + marker.len()..];
            return tail
                .trim()
                .trim_start_matches(['"', '\''])
                .trim_end_matches(['"', '\''])
                .to_string();
        }
    }
    s.to_string()
}

fn is_shell_binary(s: &str) -> bool {
    let base = std::path::Path::new(s.trim())
        .file_name()
        .map(|o| o.to_string_lossy().into_owned())
        .unwrap_or_default();
    matches!(base.as_str(), "sh" | "bash" | "zsh" | "fish")
}

fn clean_command_execution_output(output: &str) -> String {
    let output = output.trim();
    if output.is_empty() {
        return String::new();
    }
    if let Some(idx) = output.find("\nOutput:\n") {
        return output[idx + "\nOutput:\n".len()..].trim().to_string();
    }
    output.to_string()
}

fn risk_from_command(cmd: &str) -> Risk {
    for p in [
        "rm", "sudo", "chmod", "chown", "dd ", "mkfs", "kill", "pkill",
    ] {
        if cmd.starts_with(p) {
            return Risk::High;
        }
    }
    Risk::Medium
}

fn parse_usage(raw: &Value) -> Option<Usage> {
    if raw.is_null() {
        return None;
    }
    let g = |keys: &[&str]| -> i64 {
        for k in keys {
            if let Some(n) = raw.get(*k).and_then(|v| v.as_i64()) {
                if n != 0 {
                    return n;
                }
            }
        }
        0
    };
    let input = g(&["input_tokens", "inputTokens", "input", "prompt_tokens"]);
    let output = g(&[
        "output_tokens",
        "outputTokens",
        "output",
        "completion_tokens",
    ]);
    let cache_read = g(&[
        "cache_read_tokens",
        "cache_read_input_tokens",
        "cached_input_tokens",
        "cachedInputTokens",
    ]);
    let cache_write = g(&["cache_write_tokens", "cache_creation_input_tokens"]);
    let cost = raw.get("cost_usd").and_then(|v| v.as_f64()).unwrap_or(0.0);
    if input == 0 && output == 0 && cache_read == 0 && cache_write == 0 && cost == 0.0 {
        return None;
    }
    Some(Usage {
        input_tokens: input,
        output_tokens: output,
        cache_read_tokens: cache_read,
        cache_write_tokens: cache_write,
        cost_usd: cost,
    })
}

fn parse_token_usage(raw: &Value) -> Option<Usage> {
    if raw.is_null() {
        return None;
    }
    let last = raw.get("last")?;
    let input = last
        .get("inputTokens")
        .and_then(|v| v.as_i64())
        .unwrap_or(0);
    let output = last
        .get("outputTokens")
        .and_then(|v| v.as_i64())
        .unwrap_or(0);
    let cache_read = last
        .get("cachedInputTokens")
        .and_then(|v| v.as_i64())
        .unwrap_or(0);
    let total_a = last
        .get("totalTokens")
        .and_then(|v| v.as_i64())
        .unwrap_or(0);
    let total_b = last
        .get("total_tokens")
        .and_then(|v| v.as_i64())
        .unwrap_or(0);
    if input == 0 && output == 0 && cache_read == 0 && total_a == 0 && total_b == 0 {
        return None;
    }
    Some(Usage {
        input_tokens: input,
        output_tokens: output,
        cache_read_tokens: cache_read,
        ..Default::default()
    })
}

fn parse_questions(raw: Option<&Value>) -> Vec<CodexQuestion> {
    let Some(arr) = raw.and_then(|v| v.as_array()) else {
        return Vec::new();
    };
    arr.iter()
        .map(|q| CodexQuestion {
            id: q.get("id").and_then(|v| v.as_str()).unwrap_or("").into(),
            header: q
                .get("header")
                .and_then(|v| v.as_str())
                .unwrap_or("")
                .into(),
            question: q
                .get("question")
                .and_then(|v| v.as_str())
                .unwrap_or("")
                .into(),
            options: q
                .get("options")
                .and_then(|v| v.as_array())
                .map(|opts| {
                    opts.iter()
                        .map(|o| CodexQuestionOption {
                            label: o.get("label").and_then(|v| v.as_str()).unwrap_or("").into(),
                            description: o
                                .get("description")
                                .and_then(|v| v.as_str())
                                .unwrap_or("")
                                .into(),
                        })
                        .collect()
                })
                .unwrap_or_default(),
            multi_select: q
                .get("multiSelect")
                .and_then(|v| v.as_bool())
                .unwrap_or(false),
            is_other: q.get("isOther").and_then(|v| v.as_bool()).unwrap_or(false),
            is_secret: q.get("isSecret").and_then(|v| v.as_bool()).unwrap_or(false),
        })
        .collect()
}

fn project_questions(qs: &[CodexQuestion]) -> Vec<PermissionQuestion> {
    qs.iter()
        .map(|q| PermissionQuestion {
            id: q.id.clone(),
            header: q.header.clone(),
            question: q.question.clone(),
            options: q
                .options
                .iter()
                .map(|o| PermissionQuestionOption {
                    label: o.label.clone(),
                    description: o.description.clone(),
                })
                .collect(),
            multi_select: q.multi_select,
            is_other: q.is_other,
            is_secret: q.is_secret,
        })
        .collect()
}

fn encode_question_answers(
    questions: &[CodexQuestion],
    explicit: &BTreeMap<String, PermissionAnswer>,
) -> Value {
    let mut answers = Map::new();
    for q in questions {
        if q.id.is_empty() {
            continue;
        }
        if let Some((ans, _)) = lookup_question_answer(q, explicit) {
            if let Some(encoded) = encode_question_answer(q, &ans) {
                answers.insert(q.id.clone(), encoded);
            }
            continue;
        }
        let Some(first) = q.options.first() else {
            continue;
        };
        let label = first.label.trim();
        if label.is_empty() {
            continue;
        }
        answers.insert(q.id.clone(), json!({"answers": [label]}));
    }
    Value::Object(answers)
}

fn lookup_question_answer(
    q: &CodexQuestion,
    explicit: &BTreeMap<String, PermissionAnswer>,
) -> Option<(PermissionAnswer, String)> {
    if explicit.is_empty() {
        return None;
    }
    if let Some(a) = explicit.get(&q.id) {
        return Some((a.clone(), q.id.clone()));
    }
    let header = q.header.trim().to_string();
    if !header.is_empty() {
        if let Some(a) = explicit.get(&header) {
            return Some((a.clone(), header));
        }
    }
    None
}

fn encode_question_answer(q: &CodexQuestion, answer: &PermissionAnswer) -> Option<Value> {
    let mut values: Vec<String> = answer
        .answers
        .iter()
        .map(|s| s.trim().to_string())
        .filter(|s| !s.is_empty())
        .collect();
    if values.is_empty() {
        let text = answer.text.trim();
        if !text.is_empty() {
            if q.multi_select {
                for part in text.split(',') {
                    let p = part.trim();
                    if !p.is_empty() {
                        values.push(p.to_string());
                    }
                }
            } else {
                values.push(text.to_string());
            }
        }
    }
    if values.is_empty() {
        return None;
    }
    Some(json!({"answers": values}))
}

#[cfg(test)]
mod tests {
    use super::{codex_fork_choices, Codex};
    use crate::agent_runtime::{AgentCommandInvocation, AgentCommandSource, SessionRef};
    use crate::dialect::{
        CommandDispatch, Dialect, Input, OutFrame, PermissionMode, SessionParams,
    };
    use crate::event::{
        self, CommandResultData, Decision, Payload, PermissionResponse, TimelineType,
    };
    use serde_json::{json, Value};

    fn cfg(permission_mode: PermissionMode) -> SessionParams {
        SessionParams {
            cwd: "/tmp/codex-runtime".into(),
            permission_mode,
            ..Default::default()
        }
    }

    fn rpc_request_value(frame: &OutFrame) -> serde_json::Value {
        let OutFrame::Stdin(bytes) = frame else {
            panic!("expected rpc request frame: {frame:?}");
        };
        serde_json::from_slice(bytes).expect("rpc request json")
    }

    fn rpc_response_value(frame: &OutFrame) -> serde_json::Value {
        let OutFrame::Stdin(bytes) = frame else {
            panic!("expected rpc response frame: {frame:?}");
        };
        serde_json::from_slice(bytes).expect("rpc response json")
    }

    fn rpc_success_response(id: &serde_json::Value, result: serde_json::Value) -> Vec<u8> {
        serde_json::to_vec(&json!({
            "jsonrpc": "2.0",
            "id": id,
            "result": result,
        }))
        .expect("response json")
    }

    fn slash_test_invocation(name: &str) -> AgentCommandInvocation {
        let args = match name {
            "fast" => Some("status"),
            "ide" => Some("status"),
            "rename" => Some("test thread"),
            "goal" => Some("finish command dispatch"),
            "side" => Some("quick side question"),
            "resume" => Some("thread-abc"),
            "sandbox-add-read-dir" => Some("/tmp"),
            "mcp" => Some("verbose"),
            _ => None,
        };
        AgentCommandInvocation {
            name: name.into(),
            args: args.map(Into::into),
            values: serde_json::Value::Null,
            source: AgentCommandSource::ProviderNative,
        }
    }

    fn ready_dialect() -> Codex {
        let mut dialect = Codex::new();
        dialect.init(&cfg(PermissionMode::Default));
        dialect.thread_id = Some("thread-live".into());
        dialect.thread_ready = true;
        dialect
    }

    #[test]
    fn codex_new_thread_status_does_not_reuse_previous_thread_usage() {
        let mut dialect = Codex::new();
        dialect.init(&cfg(PermissionMode::Default));
        dialect.thread_id = Some("thread-old".into());
        dialect.thread_ready = true;

        let old_usage_frame = br#"{"method":"thread/tokenUsage/updated","params":{"threadId":"thread-old","tokenUsage":{"total":{"totalTokens":107,"inputTokens":100,"outputTokens":7},"last":{"totalTokens":77},"modelContextWindow":200}}}"#;
        let old_usage_events = dialect.translate(old_usage_frame);
        assert!(
            old_usage_events
                .iter()
                .any(|event| matches!(event.payload, Payload::UsageDelta(_))),
            "old thread usage should be accepted before /new"
        );

        let CommandDispatch::Deferred(new_frames) = dialect
            .handle_system_command(&AgentCommandInvocation {
                name: "new".into(),
                args: None,
                values: Value::Null,
                source: AgentCommandSource::AdapterMapped,
            })
            .expect("new dispatch")
        else {
            panic!("expected deferred /new dispatch");
        };
        let new_request = rpc_request_value(&new_frames[0]);
        assert_eq!(new_request["method"], json!("thread/start"));
        let new_events = dialect.translate(&rpc_success_response(
            &new_request["id"],
            json!({
                "thread": {
                    "id": "thread-new",
                    "cwd": "/tmp/codex-runtime",
                    "cliVersion": "0.130.0",
                    "turns": []
                },
                "model": "gpt-5.5",
                "reasoningEffort": "xhigh"
            }),
        ));
        assert!(new_events.iter().any(|event| {
            matches!(
                &event.payload,
                Payload::SessionStarted(started) if started.session_id == "thread-new"
            )
        }));

        let delayed_old_usage_events = dialect.translate(old_usage_frame);
        assert!(
            delayed_old_usage_events.is_empty(),
            "delayed old-thread usage must not affect the active new thread: {delayed_old_usage_events:?}"
        );

        let CommandDispatch::Deferred(status_frames) = dialect
            .handle_system_command(&AgentCommandInvocation {
                name: "status".into(),
                args: None,
                values: Value::Null,
                source: AgentCommandSource::AdapterMapped,
            })
            .expect("status dispatch")
        else {
            panic!("expected deferred /status dispatch");
        };
        let status_request = rpc_request_value(&status_frames[0]);
        assert_eq!(status_request["method"], json!("thread/read"));
        assert_eq!(status_request["params"]["threadId"], json!("thread-new"));

        let status_read_events = dialect.translate(&rpc_success_response(
            &status_request["id"],
            json!({
                "thread": {
                    "id": "thread-new",
                    "cwd": "/tmp/codex-runtime",
                    "cliVersion": "0.130.0",
                    "turns": []
                }
            }),
        ));
        assert!(status_read_events.is_empty());

        let config_frames = dialect.drain_out_frames();
        let config_request = rpc_request_value(&config_frames[0]);
        assert_eq!(config_request["method"], json!("config/read"));
        let status_events = dialect.translate(&rpc_success_response(
            &config_request["id"],
            json!({
                "config": {
                    "model": "gpt-5.5",
                    "model_reasoning_effort": "xhigh"
                }
            }),
        ));
        let status = status_events
            .iter()
            .find_map(|event| match &event.payload {
                Payload::CommandResult(result) => match &result.result {
                    CommandResultData::Status(status) => Some(status),
                    _ => None,
                },
                _ => None,
            })
            .expect("status command result");
        assert_eq!(status.session_id.as_deref(), Some("thread-new"));
        assert!(
            status.tokens.is_none(),
            "new empty thread must not inherit old token usage: {status:?}"
        );
        assert!(
            status.context.is_none(),
            "new empty thread must not inherit old context usage: {status:?}"
        );
    }

    #[test]
    fn codex_image_generation_completed_emits_attachment_without_prompt_caption() {
        use base64::Engine as _;

        let mut dialect = Codex::new();
        dialect.init(&SessionParams::default());
        let png = base64::engine::general_purpose::STANDARD.encode(b"\x89PNG\r\n\x1a\nbody");
        let frame = format!(
            r#"{{"method":"item/completed","params":{{"item":{{"type":"imageGeneration","id":"ig_abc123","status":"generating","revisedPrompt":"short caption","result":"{png}"}},"threadId":"t1","turnId":"turn-1","completedAtMs":1}}}}"#
        );

        let events = dialect.translate(frame.as_bytes());

        let attachment = events
            .into_iter()
            .find_map(|event| match event.payload {
                Payload::Attachment(attachment) => Some(attachment),
                _ => None,
            })
            .expect("attachment event");
        assert_eq!(attachment.id.as_str(), "ig_abc123");
        assert_eq!(attachment.filename.as_str(), "codex-image-ig_abc123.png");
        assert_eq!(attachment.media_type.as_str(), "image/png");
        assert_eq!(attachment.data_base64, png);
        assert_eq!(attachment.caption, None);
    }

    #[test]
    fn codex_image_generation_in_progress_without_result_emits_nothing() {
        let mut dialect = Codex::new();
        dialect.init(&SessionParams::default());
        let frame = br#"{"method":"item/started","params":{"item":{"type":"imageGeneration","id":"ig_empty","status":"in_progress","revisedPrompt":null,"result":""},"threadId":"t1","turnId":"turn-1","startedAtMs":1}}"#;

        let events = dialect.translate(frame);

        assert!(events
            .iter()
            .all(|event| !matches!(event.payload, Payload::Attachment(_))));
    }

    #[test]
    fn codex_native_slash_catalog_entries_have_dispatch_behavior() {
        let mut missing = Vec::new();
        for spec in super::CODEX_SLASH_COMMAND_SPECS {
            for name in std::iter::once(spec.name).chain(spec.aliases.iter().copied()) {
                let mut dialect = ready_dialect();

                let invocation = slash_test_invocation(name);
                if let Err(err) = dialect.handle_native_command(&invocation) {
                    let text = err.to_string();
                    if text.contains("unsupported command")
                        || text.contains("intentionally not mapped")
                    {
                        missing.push(format!("/{name}: {text}"));
                    }
                }
            }
        }

        assert!(
            missing.is_empty(),
            "catalog commands without dispatch behavior:\n{}",
            missing.join("\n")
        );
    }

    #[test]
    fn codex_command_catalog_exposes_only_provider_backed_commands() {
        let catalog = super::codex_feature_command_catalog(&Value::Null, 1);
        let names = catalog
            .commands
            .iter()
            .map(|command| command.name.as_str())
            .collect::<Vec<_>>();

        assert_eq!(
            names,
            vec![
                "fast", "hooks", "review", "rename", "compact", "goal", "mcp", "apps", "plugins",
                "stop"
            ]
        );
        for fixed in [
            "model",
            "permissions",
            "skills",
            "status",
            "new",
            "fork",
            "quit",
        ] {
            assert!(
                !names.contains(&fixed),
                "session-trait command /{fixed} must not be injected into Codex /commands"
            );
        }

        for not_direct in [
            "side", "plan", "init", "resume", "copy", "raw", "diff", "mention", "ide", "keymap",
            "vim", "clear", "rollout", "ps",
        ] {
            assert!(
                !names.contains(&not_direct),
                "non-direct command /{not_direct} must not appear in Codex /commands"
            );
        }
    }

    #[test]
    fn codex_non_direct_slash_commands_reject_instead_of_prompt_fallback() {
        for name in [
            "side", "plan", "init", "resume", "copy", "raw", "diff", "mention", "ide", "keymap",
            "vim", "clear", "rollout", "ps",
        ] {
            let mut dialect = ready_dialect();
            let err = dialect
                .handle_native_command(&slash_test_invocation(name))
                .unwrap_err();
            assert!(
                err.to_string().contains("unsupported command"),
                "unexpected error for /{name}: {err}"
            );
            assert!(
                dialect.pending_out.is_empty(),
                "/{name} must not enqueue a turn/start fallback"
            );
        }
    }

    #[test]
    fn codex_goal_slash_dispatches_goal_rpc() {
        let mut dialect = ready_dialect();

        let CommandDispatch::Deferred(frames) = dialect
            .handle_native_command(&AgentCommandInvocation {
                name: "goal".into(),
                args: Some("finish this refactor".into()),
                values: serde_json::Value::Null,
                source: AgentCommandSource::ProviderNative,
            })
            .expect("goal set dispatch")
        else {
            panic!("expected deferred goal set dispatch");
        };
        let request = rpc_request_value(&frames[0]);
        assert_eq!(request["method"], json!("thread/goal/set"));
        assert_eq!(request["params"]["threadId"], json!("thread-live"));
        assert_eq!(
            request["params"]["objective"],
            json!("finish this refactor")
        );
        assert_eq!(request["params"]["status"], json!("active"));

        let mut dialect = ready_dialect();
        let CommandDispatch::Deferred(frames) = dialect
            .handle_native_command(&AgentCommandInvocation {
                name: "goal".into(),
                args: Some("clear".into()),
                values: serde_json::Value::Null,
                source: AgentCommandSource::ProviderNative,
            })
            .expect("goal clear dispatch")
        else {
            panic!("expected deferred goal clear dispatch");
        };
        let request = rpc_request_value(&frames[0]);
        assert_eq!(request["method"], json!("thread/goal/clear"));
        assert_eq!(request["params"]["threadId"], json!("thread-live"));

        let mut dialect = ready_dialect();
        let CommandDispatch::Deferred(frames) = dialect
            .handle_native_command(&AgentCommandInvocation {
                name: "goal".into(),
                args: None,
                values: serde_json::Value::Null,
                source: AgentCommandSource::ProviderNative,
            })
            .expect("goal get dispatch")
        else {
            panic!("expected deferred goal get dispatch");
        };
        let request = rpc_request_value(&frames[0]);
        assert_eq!(request["method"], json!("thread/goal/get"));
        assert_eq!(request["params"]["threadId"], json!("thread-live"));
    }

    #[test]
    fn codex_hooks_and_apps_slash_commands_dispatch_to_app_server() {
        let mut dialect = ready_dialect();
        let CommandDispatch::Deferred(frames) = dialect
            .handle_native_command(&AgentCommandInvocation {
                name: "hooks".into(),
                args: None,
                values: serde_json::Value::Null,
                source: AgentCommandSource::ProviderNative,
            })
            .expect("hooks dispatch")
        else {
            panic!("expected deferred hooks dispatch");
        };
        let request = rpc_request_value(&frames[0]);
        assert_eq!(request["method"], json!("hooks/list"));
        assert_eq!(request["params"], json!({"cwds": ["/tmp/codex-runtime"]}));

        let mut dialect = ready_dialect();
        let CommandDispatch::Deferred(frames) = dialect
            .handle_native_command(&AgentCommandInvocation {
                name: "apps".into(),
                args: None,
                values: serde_json::Value::Null,
                source: AgentCommandSource::ProviderNative,
            })
            .expect("apps dispatch")
        else {
            panic!("expected deferred apps dispatch");
        };
        let request = rpc_request_value(&frames[0]);
        assert_eq!(request["method"], json!("app/list"));
        assert_eq!(request["params"]["threadId"], json!("thread-live"));
    }

    #[test]
    fn codex_stop_slash_dispatches_background_terminal_clean_rpc() {
        for name in ["stop", "clean"] {
            let mut dialect = ready_dialect();

            let CommandDispatch::Deferred(frames) = dialect
                .handle_native_command(&AgentCommandInvocation {
                    name: name.into(),
                    args: None,
                    values: serde_json::Value::Null,
                    source: AgentCommandSource::ProviderNative,
                })
                .unwrap_or_else(|err| panic!("{name} dispatch failed: {err}"))
            else {
                panic!("expected deferred {name} command");
            };

            let request = rpc_request_value(&frames[0]);
            assert_eq!(request["method"], json!("thread/backgroundTerminals/clean"));
            assert_eq!(request["params"]["threadId"], json!("thread-live"));
        }
    }

    #[test]
    fn raw_thread_started_notification_surfaces_provider_session_id() {
        let mut dialect = Codex::new();
        dialect.init(&cfg(PermissionMode::Default));

        let events = dialect.translate(
            br#"{"jsonrpc":"2.0","method":"thread/started","params":{"threadId":"thread-live"}}"#,
        );

        let started = events
            .iter()
            .find_map(|event| match &event.payload {
                Payload::SessionStarted(started) => Some(started),
                _ => None,
            })
            .expect("SessionStarted");
        assert_eq!(started.session_id, "thread-live");

        let frames = dialect
            .encode_user_message(&Input {
                text: "hello".into(),
                ..Default::default()
            })
            .expect("turn frame");
        let rpc = rpc_request_value(&frames[0]);
        assert_eq!(rpc["method"], "turn/start");
        assert_eq!(rpc["params"]["threadId"], "thread-live");
    }

    #[test]
    fn plain_assistant_questions_stay_messages() {
        let mut dialect = Codex::new();
        dialect.init(&SessionParams::default());

        let events = dialect.translate(
            br#"{"jsonrpc":"2.0","method":"item/completed","params":{"item":{"type":"agent_message","id":"msg-1","text":"Which mode should I follow after your answer: `literal` or `normal`?","phase":"completed"}}}"#,
        );

        assert!(
            events
                .iter()
                .all(|event| !matches!(event.payload, Payload::PermissionRequest(_))),
            "unexpected synthetic permission request: {events:?}"
        );
        assert!(
            events.iter().any(|event| {
                matches!(
                    &event.payload,
                    Payload::Timeline(timeline)
                        if timeline.item.ty == TimelineType::AssistantMessage
                )
            }),
            "assistant text must remain visible: {events:?}"
        );
    }

    #[test]
    fn raw_commentary_agent_message_projects_as_reasoning_not_assistant_output() {
        let mut dialect = Codex::new();
        dialect.init(&SessionParams::default());

        let events = dialect.translate(
            br#"{"jsonrpc":"2.0","method":"item/completed","params":{"item":{"type":"agent_message","id":"msg-1","text":"I am checking the current workspace first.","phase":"commentary"},"turnId":"turn-1"}}"#,
        );

        assert!(
            events.iter().any(|event| {
                matches!(
                    &event.payload,
                    Payload::Timeline(timeline) if timeline.item.ty == TimelineType::Reasoning
                )
            }),
            "commentary should remain visible as reasoning/progress: {events:?}"
        );
        assert!(
            events.iter().all(|event| {
                !matches!(
                    &event.payload,
                    Payload::Timeline(timeline)
                        if timeline.item.ty == TimelineType::AssistantMessage
                )
            }),
            "commentary must not be projected as final assistant output: {events:?}"
        );
    }

    #[test]
    fn legacy_commentary_agent_message_projects_as_reasoning_not_assistant_output() {
        let mut dialect = Codex::new();
        dialect.init(&SessionParams::default());

        let events = dialect.translate(
            br#"{"jsonrpc":"2.0","method":"codex/event","params":{"msg":{"type":"agent_message","message":"I am checking the current workspace first.","phase":"commentary"}}}"#,
        );

        assert!(
            events.iter().any(|event| {
                matches!(
                    &event.payload,
                    Payload::Timeline(timeline) if timeline.item.ty == TimelineType::Reasoning
                )
            }),
            "legacy commentary should remain visible as reasoning/progress: {events:?}"
        );
        assert!(
            events.iter().all(|event| {
                !matches!(
                    &event.payload,
                    Payload::Timeline(timeline)
                        if timeline.item.ty == TimelineType::AssistantMessage
                )
            }),
            "legacy commentary must not be projected as final assistant output: {events:?}"
        );
    }

    #[test]
    fn final_answer_item_does_not_complete_turn_before_native_turn_completed() {
        let mut dialect = Codex::new();
        dialect.init(&SessionParams::default());

        let item_events = dialect.translate(
            br#"{"jsonrpc":"2.0","method":"item/completed","params":{"item":{"type":"agent_message","id":"msg-1","text":"done","phase":"final_answer"},"turnId":"turn-1"}}"#,
        );

        assert!(
            item_events.iter().any(|event| {
                matches!(
                    &event.payload,
                    Payload::Timeline(timeline)
                        if timeline.item.ty == TimelineType::AssistantMessage
                )
            }),
            "final assistant text should remain visible: {item_events:?}"
        );
        assert!(
            item_events
                .iter()
                .all(|event| !matches!(event.payload, Payload::TurnCompleted(_))),
            "turn completion must come from native turn/completed, not final_answer item: {item_events:?}"
        );

        let complete_events = dialect.translate(
            br#"{"jsonrpc":"2.0","method":"turn/completed","params":{"turn":{"id":"turn-1","status":"completed"}}}"#,
        );

        assert!(
            complete_events
                .iter()
                .any(|event| matches!(event.payload, Payload::TurnCompleted(_))),
            "native turn/completed must complete the turn: {complete_events:?}"
        );
    }

    #[test]
    fn file_change_approval_includes_started_item_changes() {
        let mut dialect = Codex::new();
        dialect.init(&cfg(PermissionMode::Default));

        let started_events = dialect.translate(
            br#"{"jsonrpc":"2.0","method":"item/started","params":{"item":{"type":"fileChange","id":"call_file_started_1","changes":[{"path":"/tmp/repo/src/main.rs","kind":{"type":"update","move_path":null},"diff":"@@\n-old\n+new\n"}],"status":"inProgress"}}}"#,
        );
        assert!(started_events.is_empty());

        let events = dialect.translate(
            br#"{"jsonrpc":"2.0","id":7,"method":"item/fileChange/requestApproval","params":{"threadId":"thread-1","turnId":"turn-1","itemId":"call_file_started_1","reason":null,"grantRoot":null}}"#,
        );
        let request = events
            .iter()
            .find_map(|event| match &event.payload {
                Payload::PermissionRequest(request) => Some(request),
                _ => None,
            })
            .expect("server approval must surface as permission request");
        let input = request
            .input
            .as_ref()
            .and_then(Value::as_object)
            .expect("approval input");

        assert_eq!(request.tool, "item/fileChange/requestApproval");
        assert_eq!(
            input.get("approvalKind").and_then(Value::as_str),
            Some("file_change")
        );
        assert_eq!(
            input.get("path").and_then(Value::as_str),
            Some("/tmp/repo/src/main.rs")
        );
        assert!(
            input.get("diff").is_none(),
            "diffs should remain provider-owned change payload data, not mapped to approval input"
        );
        assert_eq!(
            input
                .get("changes")
                .and_then(Value::as_array)
                .and_then(|changes| changes.first())
                .and_then(|change| change.get("path"))
                .and_then(Value::as_str),
            Some("/tmp/repo/src/main.rs")
        );
    }

    #[test]
    fn bypass_mode_does_not_locally_interfere_with_server_approval_requests() {
        let mut dialect = Codex::new();
        dialect.init(&cfg(PermissionMode::Bypass));

        let events = dialect.translate(
            br#"{"jsonrpc":"2.0","id":7,"method":"item/commandExecution/requestApproval","params":{"tool":"shell","command":"touch /tmp/lucarne-approval-test"}}"#,
        );
        let request = events
            .iter()
            .find_map(|event| match &event.payload {
                Payload::PermissionRequest(request) => Some(request),
                _ => None,
            })
            .expect("server approval must surface as permission request");

        assert_eq!(request.tool, "shell");
        assert_eq!(
            request
                .input
                .as_ref()
                .and_then(|input| input.get("command"))
                .and_then(Value::as_str),
            Some("touch /tmp/lucarne-approval-test")
        );

        let frames = dialect
            .encode_permission_response(
                &request.req_id,
                &PermissionResponse::from_decision(Decision::Deny),
            )
            .expect("encode user decision");
        let response = rpc_response_value(&frames[0]);

        assert_eq!(response["id"], json!(7));
        assert_eq!(response["result"], json!({"decision": "reject"}));
    }

    #[test]
    fn thread_start_params_follow_canonical_permission_mode() {
        let mut dialect = Codex::new();
        dialect.init(&cfg(PermissionMode::Auto));

        assert_eq!(
            dialect.thread_start_params(),
            json!({
                "cwd": "/tmp/codex-runtime",
                "developerInstructions": null,
                "persistExtendedHistory": true,
                "approvalPolicy": "never",
                "sandbox": "workspace-write",
            })
        );
    }

    #[test]
    fn command_catalog_does_not_inject_adapter_commands() {
        let catalog = Codex::new().command_catalog();
        for name in ["model", "permissions", "skills", "new", "quit"] {
            assert!(
                !catalog
                    .commands
                    .iter()
                    .any(|command| command.name.as_str() == name),
                "injected /{name}"
            );
        }
    }

    #[test]
    fn codex_fork_choices_collapse_multiple_user_messages_in_same_turn() {
        let choices = codex_fork_choices(&json!({
            "thread": {
                "turns": [
                    {
                        "items": [
                            {"type": "userMessage", "id": "item-1", "text": "first replayed prompt"},
                            {"type": "agentMessage", "id": "item-2", "text": "first response"},
                            {"type": "userMessage", "id": "item-3", "text": "second replayed prompt"}
                        ]
                    },
                    {
                        "items": [
                            {"type": "userMessage", "id": "item-5", "text": "latest prompt"}
                        ]
                    }
                ]
            }
        }));

        assert_eq!(choices.len(), 2);
        assert_eq!(choices[0].target.id.as_str(), "item-3");
        assert_eq!(
            choices[0].target.label.as_deref(),
            Some("second replayed prompt")
        );
        assert_eq!(
            choices[0].target.description.as_deref(),
            Some("turn 1 · 2 user messages")
        );
        assert_eq!(choices[0].rollback_turns, 1);
        assert_eq!(choices[1].target.id.as_str(), "item-5");
        assert_eq!(choices[1].rollback_turns, 0);
    }

    #[test]
    fn selected_fork_target_emits_forked_command_result() {
        let mut dialect = Codex::new();
        dialect.init(&cfg(PermissionMode::Default));
        dialect.thread_id = Some("thread-source".into());
        dialect.thread_ready = true;
        dialect.fork_target_rollbacks.insert("msg-2".into(), 1);

        let CommandDispatch::Deferred(frames) = dialect.fork(Some("msg-2")).expect("fork dispatch")
        else {
            panic!("expected deferred fork dispatch");
        };
        let request = rpc_request_value(&frames[0]);
        assert_eq!(request["method"], json!("thread/fork"));

        let events = dialect.translate(&rpc_success_response(
            &request["id"],
            json!({"threadId": "thread-fork"}),
        ));
        assert!(
            events.is_empty(),
            "rollback fork should wait for rollback response: {events:?}"
        );

        let rollback_frames = dialect.drain_out_frames();
        let rollback = rpc_request_value(&rollback_frames[0]);
        assert_eq!(rollback["method"], json!("thread/rollback"));

        let events = dialect.translate(&rpc_success_response(
            &rollback["id"],
            json!({"threadId": "thread-fork"}),
        ));
        let forked = events
            .iter()
            .find_map(|event| match &event.payload {
                Payload::CommandResult(result) => match &result.result {
                    CommandResultData::Forked(forked) => Some(forked),
                    _ => None,
                },
                _ => None,
            })
            .expect("fork command result");
        assert_eq!(forked.session_ref, Some(SessionRef("thread-fork".into())));
        assert_eq!(
            forked.source_session_ref,
            Some(SessionRef("thread-source".into()))
        );
    }

    #[test]
    fn collab_subagent_item_emits_child_thread_link_metadata() {
        let mut dialect = Codex::new();
        dialect.init(&cfg(PermissionMode::Default));
        dialect.thread_id = Some("thread-parent".into());
        dialect.thread_ready = true;

        let events = dialect.translate(
            br#"{"jsonrpc":"2.0","method":"item/completed","params":{"threadId":"thread-parent","turnId":"turn-1","item":{"id":"collab-1","type":"collabAgentSpawn","tool":"spawn_agent","status":"running","prompt":"Inspect parser","model":"gpt-5.4","receiverAgents":[{"threadId":"thread-child","agentId":"agent-1","nickname":"Parser","agentRole":"explorer","prompt":"Inspect parser child"}]}}}"#,
        );

        let sub_agent = events
            .iter()
            .find_map(|event| match &event.payload {
                Payload::Timeline(timeline) if timeline.item.ty == TimelineType::ToolCall => {
                    timeline.item.tool_call.as_ref().and_then(|tool| {
                        if tool.call.name == "sub_agent" {
                            serde_json::from_value::<event::SubAgentCall>(tool.call.input.clone())
                                .ok()
                        } else {
                            None
                        }
                    })
                }
                _ => None,
            })
            .expect("subagent tool call");

        assert_eq!(sub_agent.tool_name.as_str(), "spawn_agent");
        assert_eq!(sub_agent.child_thread_id.as_deref(), Some("thread-child"));
        assert_eq!(sub_agent.child_session_ref.as_deref(), Some("thread-child"));
        assert_eq!(sub_agent.agent_id.as_deref(), Some("agent-1"));
        assert_eq!(sub_agent.nickname.as_deref(), Some("Parser"));
        assert_eq!(sub_agent.role.as_deref(), Some("explorer"));
        assert_eq!(sub_agent.requested_model.as_deref(), Some("gpt-5.4"));
        assert_eq!(sub_agent.prompt.as_deref(), Some("Inspect parser child"));
    }

    #[test]
    fn collab_subagent_status_updates_emit_one_tool_call_per_child() {
        let mut dialect = Codex::new();
        dialect.init(&cfg(PermissionMode::Default));
        dialect.thread_id = Some("thread-parent".into());
        dialect.thread_ready = true;

        let events = dialect.translate(
            br#"{"jsonrpc":"2.0","method":"item/completed","params":{"threadId":"thread-parent","turnId":"turn-1","item":{"id":"collab-2","type":"collabWaiting","tool":"wait","status":"waiting","agentStates":{"thread-a":{"status":"waiting","message":"reading"},"thread-b":{"status":"completed","message":"done"}}}}}"#,
        );

        let mut subagents = events
            .iter()
            .filter_map(|event| match &event.payload {
                Payload::Timeline(timeline) if timeline.item.ty == TimelineType::ToolCall => {
                    timeline.item.tool_call.as_ref().map(|tool| &tool.call)
                }
                _ => None,
            })
            .filter(|call| call.name == "sub_agent")
            .filter_map(|call| {
                serde_json::from_value::<event::SubAgentCall>(call.input.clone()).ok()
            })
            .collect::<Vec<_>>();
        subagents.sort_by_key(|call| call.child_thread_id.clone().unwrap_or_default());

        assert_eq!(subagents.len(), 2);
        assert_eq!(subagents[0].tool_name.as_str(), "wait");
        assert_eq!(subagents[0].child_thread_id.as_deref(), Some("thread-a"));
        assert_eq!(subagents[0].status.as_deref(), Some("waiting"));
        assert_eq!(subagents[0].message.as_deref(), Some("reading"));
        assert_eq!(subagents[1].child_thread_id.as_deref(), Some("thread-b"));
        assert_eq!(subagents[1].status.as_deref(), Some("completed"));
        assert_eq!(subagents[1].message.as_deref(), Some("done"));
    }

    #[test]
    fn list_skills_uses_workspace_cwds_param() {
        let mut dialect = Codex::new();
        dialect.init(&cfg(PermissionMode::Default));

        let CommandDispatch::Deferred(frames) =
            dialect.list_skills().expect("list skills dispatch")
        else {
            panic!("expected deferred list skills dispatch");
        };

        let request = rpc_request_value(&frames[0]);
        assert_eq!(request["method"], json!("skills/list"));
        assert_eq!(request["params"], json!({"cwds": ["/tmp/codex-runtime"]}));
    }

    #[test]
    fn list_skills_retries_legacy_cwd_param_when_cwds_is_rejected() {
        let mut dialect = Codex::new();
        dialect.init(&cfg(PermissionMode::Default));
        dialect.pending.insert(123, "skills/list|cwds".into());

        let events = dialect.translate(
            br#"{"jsonrpc":"2.0","id":123,"error":{"code":-32602,"message":"unknown field `cwds`"}}"#,
        );

        assert!(
            events.is_empty(),
            "fallback should not fail the turn: {events:?}"
        );
        let frames = dialect.drain_out_frames();
        assert_eq!(frames.len(), 1);
        let request = rpc_request_value(&frames[0]);
        assert_eq!(request["method"], json!("skills/list"));
        assert_eq!(request["params"], json!({"cwd": "/tmp/codex-runtime"}));
    }

    #[test]
    fn skill_catalog_accepts_flat_data_shape_and_prefers_enabled_deduped_skill() {
        let catalog = super::codex_skill_catalog(&json!({
            "data": [
                {
                    "name": "review",
                    "description": "Disabled copy",
                    "path": "/disabled/SKILL.md",
                    "scope": "project",
                    "enabled": false
                },
                {
                    "name": "Review",
                    "description": "Enabled copy",
                    "path": "/enabled/SKILL.md",
                    "scope": "global",
                    "enabled": true
                },
                {
                    "name": "alpha",
                    "description": "First alphabetically",
                    "enabled": true
                }
            ]
        }));

        assert_eq!(
            catalog
                .skills
                .iter()
                .map(|skill| skill.name.as_str())
                .collect::<Vec<_>>(),
            vec!["alpha", "Review"]
        );
        let review = &catalog.skills[1];
        assert_eq!(review.description.as_deref(), Some("Enabled copy"));
        assert_eq!(review.path.as_deref(), Some("/enabled/SKILL.md"));
        assert_eq!(review.scope.as_deref(), Some("global"));
        assert_eq!(review.enabled, Some(true));
    }

    #[test]
    fn thread_resume_params_follow_canonical_permission_mode() {
        let mut dialect = Codex::new();
        dialect.init(&cfg(PermissionMode::Full));

        assert_eq!(
            dialect.thread_resume_params("thread-123"),
            json!({
                "threadId": "thread-123",
                "cwd": "/tmp/codex-runtime",
                "developerInstructions": null,
                "approvalPolicy": "never",
                "sandbox": "danger-full-access",
                "persistExtendedHistory": true,
            })
        );
    }

    #[test]
    fn bypass_permission_mode_uses_codex_danger_no_sandbox_profile() {
        let input = Input {
            text: "hello".into(),
            ..Default::default()
        };
        let mut dialect = Codex::new();
        dialect.init(&cfg(PermissionMode::Bypass));

        let start = dialect.thread_start_params();
        assert_eq!(start["approvalPolicy"], json!("never"));
        assert_eq!(
            start["permissions"],
            json!({"type": "profile", "id": ":danger-no-sandbox"})
        );
        assert!(start.get("sandbox").is_none());

        let resume = dialect.thread_resume_params("thread-123");
        assert_eq!(resume["approvalPolicy"], json!("never"));
        assert_eq!(
            resume["permissions"],
            json!({"type": "profile", "id": ":danger-no-sandbox"})
        );
        assert!(resume.get("sandbox").is_none());

        let fork = dialect.thread_fork_params("thread-123");
        assert_eq!(fork["approvalPolicy"], json!("never"));
        assert_eq!(
            fork["permissions"],
            json!({"type": "profile", "id": ":danger-no-sandbox"})
        );
        assert!(fork.get("sandbox").is_none());

        let turn = dialect.turn_start_params("thread-123", &input);
        assert_eq!(turn["approvalPolicy"], json!("never"));
        assert_eq!(
            turn["permissions"],
            json!({"type": "profile", "id": ":danger-no-sandbox"})
        );
        assert!(turn.get("sandboxPolicy").is_none());
    }

    #[test]
    fn turn_start_params_follow_canonical_permission_mode() {
        let input = Input {
            text: "hello".into(),
            ..Default::default()
        };
        let cases = [
            (
                PermissionMode::Default,
                "untrusted",
                json!({"type": "workspaceWrite", "networkAccess": false}),
            ),
            (
                PermissionMode::Write,
                "on-request",
                json!({"type": "workspaceWrite", "networkAccess": false}),
            ),
            (
                PermissionMode::ReadOnly,
                "never",
                json!({"type": "readOnly", "networkAccess": false}),
            ),
            (
                PermissionMode::Auto,
                "never",
                json!({"type": "workspaceWrite", "networkAccess": false}),
            ),
            (
                PermissionMode::Full,
                "never",
                json!({"type": "dangerFullAccess"}),
            ),
        ];

        for (mode, approval_policy, sandbox_policy) in cases {
            let mut dialect = Codex::new();
            dialect.init(&cfg(mode));
            let params = dialect.turn_start_params("thread-abc", &input);
            assert_eq!(
                params["approvalPolicy"],
                json!(approval_policy),
                "mode={mode:?}"
            );
            assert!(
                params.get("model").is_none(),
                "empty model must not be sent on turn/start"
            );
            assert_eq!(params["sandboxPolicy"], sandbox_policy, "mode={mode:?}");
        }
    }

    #[test]
    fn permission_catalog_uses_codex_model_permission_presets() {
        let catalog = super::codex_permission_catalog(&json!({}), "default");

        assert_eq!(catalog.current_mode.as_deref(), Some("default"));
        assert_eq!(
            catalog
                .modes
                .iter()
                .map(|mode| mode.id.as_str())
                .collect::<Vec<_>>(),
            vec!["default", "auto-review", "full-access", "bypass"]
        );
        assert!(catalog.modes.iter().any(|mode| {
            mode.id.as_str() == "bypass"
                && mode
                    .description
                    .as_deref()
                    .is_some_and(|description| description.contains(":danger-no-sandbox"))
        }));
        assert!(catalog.modes.iter().any(|mode| {
            mode.id.as_str() == "auto-review"
                && mode
                    .description
                    .as_deref()
                    .is_some_and(|description| description.contains("auto-reviewer"))
        }));
    }

    #[test]
    fn permission_preset_write_keeps_full_access_legacy_and_bypass_profiles() {
        let full = super::codex_permission_preset("full-access").expect("full preset");
        assert_eq!(
            super::codex_permission_preset_write(full, 2),
            Some(super::CodexPermissionPresetWrite::Batch {
                edits: vec![
                    ("default_permissions", Value::Null),
                    ("sandbox_mode", json!("danger-full-access")),
                ],
            })
        );

        let bypass = super::codex_permission_preset("bypass").expect("bypass preset");
        assert_eq!(
            super::codex_permission_preset_write(bypass, 2),
            Some(super::CodexPermissionPresetWrite::Batch {
                edits: vec![
                    ("sandbox_mode", Value::Null),
                    ("default_permissions", json!(":danger-no-sandbox")),
                ],
            })
        );
    }

    #[test]
    fn status_prefers_session_model_over_global_config() {
        let status = super::codex_status(
            &json!({"thread": {"id": "thread-abc", "cwd": "/tmp/project"}}),
            "gpt-5.4",
            Some("xhigh"),
            "default",
            None,
            None,
            Some(&json!({"config": {
                "model": "gpt-5.5",
                "model_reasoning_effort": "medium"
            }})),
        );

        assert_eq!(status.model.as_deref(), Some("gpt-5.4"));
        assert_eq!(status.reasoning.as_deref(), Some("xhigh"));
        assert_eq!(status.permissions.as_deref(), Some("Default"));
    }

    #[test]
    fn permission_preset_reader_accepts_thread_start_shape() {
        assert_eq!(
            super::codex_permission_preset_from_result(&json!({
                "approvalPolicy": "untrusted",
                "approvalsReviewer": "user",
                "sandbox": {"type": "workspaceWrite"}
            }))
            .map(|preset| preset.id),
            Some("default")
        );
        assert_eq!(
            super::codex_permission_preset_from_result(&json!({
                "config": {
                    "approval_policy": "on-request",
                    "approvals_reviewer": "guardian_subagent",
                    "sandbox_mode": "workspace-write"
                }
            }))
            .map(|preset| preset.id),
            Some("auto-review")
        );
        assert_eq!(
            super::codex_permission_preset_from_result(&json!({
                "approvalPolicy": "never",
                "permissionProfile": {"type": "disabled"},
                "activePermissionProfile": {"id": ":danger-no-sandbox"}
            }))
            .map(|preset| preset.id),
            Some("bypass")
        );
        assert_eq!(
            super::codex_permission_preset_from_result(&json!({
                "approvalPolicy": "never",
                "permissionProfile": {"type": "disabled"}
            }))
            .map(|preset| preset.id),
            Some("full-access")
        );
        assert_eq!(
            super::codex_permission_preset_from_result(&json!({
                "permissionProfile": {"type": "managed"},
                "config": {
                    "approval_policy": "never",
                    "default_permissions": ":danger-no-sandbox"
                }
            }))
            .map(|preset| preset.id),
            None
        );
        assert_eq!(
            super::codex_permission_preset_from_result(&json!({
                "config": {
                    "approval_policy": "never",
                    "default_permissions": ":danger-no-sandbox"
                }
            }))
            .map(|preset| preset.id),
            Some("bypass")
        );
        assert_eq!(
            super::codex_permission_preset_from_result(&json!({
                "config": {
                    "approval_policy": "never",
                    "sandbox_mode": "danger-full-access"
                }
            }))
            .map(|preset| preset.id),
            Some("full-access")
        );
    }

    #[test]
    fn turn_start_params_include_multimodal_user_inputs() {
        let mut dialect = Codex::new();
        dialect.init(&cfg(PermissionMode::Default));

        let params = dialect.turn_start_params(
            "thread-abc",
            &Input {
                text: "read the token".into(),
                images: vec![crate::dialect::ImageRef {
                    media_type: "image/png".into(),
                    data: vec![1, 2, 3],
                }],
            },
        );

        assert_eq!(
            params["input"],
            json!([
                {"type": "text", "text": "[Image #1]"},
                {"type": "image", "url": "data:image/png;base64,AQID"},
                {"type": "text", "text": "read the token"},
            ])
        );
    }
}
