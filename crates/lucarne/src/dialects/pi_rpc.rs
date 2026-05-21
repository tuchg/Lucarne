//! Pi RPC dialect — translates `pi --mode rpc` newline-delimited JSON
//! into canonical events. Stateful per-session.
//!
//! Wire overview:
//!
//! ```text
//! us  -> {"type":"prompt","id":"p1","message":"..."}
//! pi  -> {"id":"p1","type":"response","command":"prompt","success":true}
//! pi  -> {"type":"agent_start",...}
//! pi  -> {"type":"message_update","assistantMessageEvent":{"type":"text_delta","delta":"..."}}
//! pi  -> {"type":"tool_execution_start","toolCallId":"t1","toolName":"bash",...}
//! pi  -> {"type":"tool_execution_end","toolCallId":"t1","result":"..."}
//! pi  -> {"type":"turn_end","message":{...},"toolResults":[...]}
//! pi  -> {"type":"usage","usage":{...}}
//! ```
//!
//! Responses are correlated via `pending: HashMap<String, PendingKind>`.
//! The initial `get_state` response supplies `sessionFile` which becomes
//! the resume token (matching the old one-shot dialect's shape).

use smol_str::SmolStr;

use crate::{
    agent_runtime::{
        AgentCommand, AgentCommandCatalog, AgentCommandCompletion, AgentCommandInput,
        AgentCommandInvocation, AgentCommandSource, AgentContextUsage, AgentForkResult,
        AgentForkTarget, AgentForkTargetCatalog, AgentModelCatalog, AgentModelOption,
        AgentPermissionCatalog, AgentReasoningOption, AgentSkillCatalog, AgentSkillSummary,
        AgentStatus, AgentTokenUsage, SessionRef,
    },
    dialect::{
        command_result_events, fork_name, model_args, normalize_agent_command_name, permission_arg,
        CommandDispatch, CommandMessage, CommandResult, Conversation, Dialect, Input,
        ModelSelection, OutFrame, SessionParams,
    },
    error::{LucarneError, Result},
    event::{
        self, Decision, Event, LogLine, Payload, PermissionQuestion, PermissionQuestionOption,
        PermissionRequest, PermissionResponse, ResumeHandle, Risk, SessionClosed, SessionStarted,
        Timeline, TimelineItem, ToolResult, TurnCompleted, TurnFailed, Usage, UsageDelta,
    },
};
use serde::Deserialize;
use serde_json::{json, Value};
use std::{
    collections::{BTreeMap, HashMap, VecDeque},
    path::Path,
};
use tracing::{debug, warn};

// ---------------------------------------------------------------------------
// PiRpc — stateful RPC-mode dialect
// ---------------------------------------------------------------------------

pub struct PiRpc {
    cfg: SessionParams,
    seq: u64,
    /// Request id → what we're waiting for.
    pending: HashMap<String, PendingKind>,
    /// Outbound frames queued for the runtime to drain.
    pending_out: Vec<OutFrame>,
    /// User prompts received before the handshake completes.
    pending_prompts: VecDeque<Input>,
    /// Session lifecycle state.
    state: SessionState,
    /// Whether SessionStarted has been emitted for this RPC process.
    session_started: bool,
    /// Last session ref surfaced to the runtime as a provider-native session id.
    announced_session_ref: Option<String>,
    /// sessionId from get_state response.
    session_id: Option<String>,
    /// sessionFile from get_state response — becomes resume token.
    session_file: Option<String>,
    /// Reverse-RPC extension UI requests we haven't answered yet.
    extension_requests: HashMap<String, ExtensionRequestKind>,
    /// Cached command catalog (populated by get_commands).
    command_catalog: AgentCommandCatalog,
    /// Cached model catalog (populated by get_available_models).
    model_catalog: AgentModelCatalog,
    /// Two-phase status builder: get_state → get_session_stats.
    pending_status: Option<PendingStatusBuild>,
    /// Whether a turn is currently active.
    turn_active: bool,
    /// Pi streams thinking as token deltas. Buffer them so public
    /// reasoning events match the completed-block semantics used by
    /// Codex/Claude instead of leaking fragments into channel UIs.
    thinking_delta_buffer: String,
    /// Whether this turn already emitted a reasoning block from the
    /// buffered delta stream.
    turn_emitted_reasoning: bool,
    cli_version: Option<String>,
}

#[derive(Debug, Clone)]
enum PendingKind {
    GetState { initial: bool },
    GetAvailableModels,
    SetModel { thinking: Option<String> },
    SetThinkingLevel { model: String, thinking: String },
    Fork,
    ForkGetState { source_session_ref: Option<String> },
    GetCommands,
    GetSkills,
    GetSessionStats,
    NewSession,
    GetForkMessages,
    SetAutoRetry,
    AbortRetry,
    ProviderCommand { name: String },
    Generic(String),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum SessionState {
    Initializing,
    Idle,
    Streaming,
}

#[derive(Debug, Clone)]
struct ExtensionRequestKind {
    id: String,
    method: ExtensionMethod,
    question_id: Option<String>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ExtensionMethod {
    Confirm,
    Select,
    Other,
}

#[derive(Debug, Clone, Default)]
struct PendingStatusBuild {
    state: Value,
}

// ---------------------------------------------------------------------------
// Envelope — unified deserialization for responses + events
// ---------------------------------------------------------------------------

#[derive(Deserialize, Default)]
struct Envelope {
    #[serde(default, rename = "type")]
    r#type: String,
    // Response fields
    #[serde(default)]
    id: Option<String>,
    #[serde(default)]
    command: Option<String>,
    #[serde(default)]
    success: Option<bool>,
    #[serde(default)]
    data: Option<Value>,
    #[serde(default)]
    error: Option<String>,
    // Event fields (pi's AgentSessionEvent schema)
    #[serde(default, rename = "assistantMessageEvent")]
    assistant_message_event: Option<Value>,
    #[serde(default, rename = "toolCallId")]
    tool_call_id: Option<String>,
    #[serde(default, rename = "toolName")]
    tool_name: Option<String>,
    #[serde(default)]
    args: Option<Value>,
    #[serde(default, rename = "partialResult")]
    partial_result: Option<Value>,
    #[serde(default)]
    result: Option<Value>,
    #[serde(default, rename = "isError")]
    is_error: Option<bool>,
    #[serde(default)]
    message: Option<Value>,
    #[serde(default, rename = "toolResults")]
    tool_results: Option<Value>,
    #[serde(default)]
    usage: Option<Value>,
    #[serde(default, rename = "finalError")]
    final_error: Option<String>,
    // Extension UI request
    #[serde(default)]
    method: Option<String>,
}

impl Envelope {
    fn is_response(&self) -> bool {
        self.r#type == "response"
    }

    fn is_extension_ui_request(&self) -> bool {
        self.r#type == "extension_ui_request" && self.id.is_some() && self.method.is_some()
    }
}

// ---------------------------------------------------------------------------
// PiRpc impls
// ---------------------------------------------------------------------------

impl PiRpc {
    pub fn new() -> Self {
        Self::with_cli_version(None)
    }

    pub fn with_cli_version(cli_version: Option<String>) -> Self {
        Self {
            cfg: SessionParams::default(),
            seq: 0,
            pending: HashMap::new(),
            pending_out: Vec::new(),
            pending_prompts: VecDeque::new(),
            state: SessionState::Initializing,
            session_started: false,
            announced_session_ref: None,
            session_id: None,
            session_file: None,
            extension_requests: HashMap::new(),
            command_catalog: AgentCommandCatalog::default(),
            model_catalog: AgentModelCatalog::default(),
            pending_status: None,
            turn_active: false,
            thinking_delta_buffer: String::new(),
            turn_emitted_reasoning: false,
            cli_version,
        }
    }

    fn next_id(&mut self) -> String {
        let id = self.seq.to_string();
        self.seq = self.seq.wrapping_add(1);
        id
    }

    fn enqueue_rpc(&mut self, cmd: Value, kind: PendingKind) -> Result<()> {
        let id = cmd["id"].as_str().unwrap_or("").to_string();
        self.pending.insert(id, kind);
        let mut bytes = serde_json::to_vec(&cmd)?;
        bytes.push(b'\n');
        self.pending_out.push(OutFrame::stdin(bytes));
        Ok(())
    }

    fn flush_pending_prompts(&mut self) {
        while let Some(input) = self.pending_prompts.pop_front() {
            if let Ok(frames) = self.encode_user_message(&input) {
                self.pending_out.extend(frames);
            }
        }
    }

    fn set_auto_retry(&mut self, enabled: bool) -> Result<CommandDispatch> {
        let id = self.next_id();
        let cmd = json!({"id": id, "type": "set_auto_retry", "enabled": enabled});
        self.enqueue_rpc(cmd, PendingKind::SetAutoRetry)?;
        Ok(CommandDispatch::deferred(std::mem::take(
            &mut self.pending_out,
        )))
    }

    fn abort_retry(&mut self) -> Result<CommandDispatch> {
        let id = self.next_id();
        let cmd = json!({"id": id, "type": "abort_retry"});
        self.enqueue_rpc(cmd, PendingKind::AbortRetry)?;
        Ok(CommandDispatch::deferred(std::mem::take(
            &mut self.pending_out,
        )))
    }

    fn invoke_provider_slash_command(
        &mut self,
        name: &str,
        args: Option<&str>,
    ) -> Result<CommandDispatch> {
        let mut message = format!("/{name}");
        if let Some(args) = args.map(str::trim).filter(|args| !args.is_empty()) {
            message.push(' ');
            message.push_str(args);
        }
        let id = self.next_id();
        let cmd = json!({"id": id, "type": "prompt", "message": message});
        self.enqueue_rpc(
            cmd,
            PendingKind::ProviderCommand {
                name: name.to_string(),
            },
        )?;
        self.turn_active = true;
        Ok(CommandDispatch::deferred(std::mem::take(
            &mut self.pending_out,
        )))
    }

    fn catalog_command_name(&self, name: &str) -> Option<String> {
        self.command_catalog
            .commands
            .iter()
            .find(|command| {
                command.name.as_str() == name
                    || command.aliases.iter().any(|alias| alias.as_str() == name)
            })
            .map(|command| command.name.to_string())
    }
}

impl PiRpc {
    fn list_commands(&mut self) -> Result<CommandDispatch> {
        if !self.command_catalog.commands.is_empty() {
            return Ok(CommandDispatch::ready(CommandResult::Commands(
                self.command_catalog.clone(),
            )));
        }
        let id = self.next_id();
        let cmd = json!({"id": id, "type": "get_commands"});
        self.enqueue_rpc(cmd, PendingKind::GetCommands)?;
        Ok(CommandDispatch::deferred(std::mem::take(
            &mut self.pending_out,
        )))
    }

    fn list_models(&mut self) -> Result<CommandDispatch> {
        // Serve from cache if already populated
        if !self.model_catalog.models.is_empty() {
            return Ok(CommandDispatch::ready(CommandResult::Models(
                self.model_catalog.clone(),
            )));
        }
        let id = self.next_id();
        let cmd = json!({"id": id, "type": "get_available_models"});
        self.enqueue_rpc(cmd, PendingKind::GetAvailableModels)?;
        Ok(CommandDispatch::deferred(std::mem::take(
            &mut self.pending_out,
        )))
    }

    fn set_model(&mut self, model: &str, reasoning: Option<&str>) -> Result<CommandDispatch> {
        let (model_without_thinking, suffix_thinking) = split_model_thinking(model);
        let thinking = reasoning
            .map(|r| r.trim().to_string())
            .filter(|r| !r.is_empty())
            .or(suffix_thinking);
        let (provider, model_id) = split_model(&model_without_thinking);
        let id = self.next_id();
        let cmd = json!({"id": id, "type": "set_model", "provider": provider, "modelId": model_id});
        self.enqueue_rpc(
            cmd,
            PendingKind::SetModel {
                thinking: thinking.clone(),
            },
        )?;
        self.cfg.model = model_without_thinking;
        Ok(CommandDispatch::deferred(std::mem::take(
            &mut self.pending_out,
        )))
    }

    fn status(&mut self) -> Result<CommandDispatch> {
        self.pending_status = Some(PendingStatusBuild::default());
        let id = self.next_id();
        let cmd = json!({"id": id, "type": "get_state"});
        self.enqueue_rpc(cmd, PendingKind::GetState { initial: false })?;
        Ok(CommandDispatch::deferred(std::mem::take(
            &mut self.pending_out,
        )))
    }

    fn list_permissions(&mut self) -> Result<CommandDispatch> {
        // Pi has no permission-mode concept via RPC; permission
        // decisions happen per-tool through extension_ui_request.
        Ok(CommandDispatch::ready(CommandResult::Permissions(
            AgentPermissionCatalog {
                current_mode: None,
                modes: vec![],
            },
        )))
    }

    fn set_permissions(&mut self, _mode: &str) -> Result<CommandDispatch> {
        Err(LucarneError::dialect(
            "pi: set_permissions not supported in RPC mode",
        ))
    }

    fn list_skills(&mut self) -> Result<CommandDispatch> {
        let id = self.next_id();
        let cmd = json!({"id": id, "type": "get_commands"});
        self.enqueue_rpc(cmd, PendingKind::GetSkills)?;
        Ok(CommandDispatch::deferred(std::mem::take(
            &mut self.pending_out,
        )))
    }

    fn list_fork(&mut self) -> Result<CommandDispatch> {
        let id = self.next_id();
        let cmd = json!({"id": id, "type": "get_fork_messages"});
        self.enqueue_rpc(cmd, PendingKind::GetForkMessages)?;
        Ok(CommandDispatch::deferred(std::mem::take(
            &mut self.pending_out,
        )))
    }

    fn fork(&mut self, target: Option<&str>) -> Result<CommandDispatch> {
        let entry_id =
            target.ok_or_else(|| LucarneError::dialect("pi: fork requires a target entry id"))?;
        let id = self.next_id();
        let cmd = json!({"id": id, "type": "fork", "entryId": entry_id});
        self.enqueue_rpc(cmd, PendingKind::Fork)?;
        Ok(CommandDispatch::deferred(std::mem::take(
            &mut self.pending_out,
        )))
    }

    fn new_session_command(&mut self) -> Result<CommandDispatch> {
        let id = self.next_id();
        let cmd = json!({"id": id, "type": "new_session"});
        self.enqueue_rpc(cmd, PendingKind::NewSession)?;
        Ok(CommandDispatch::deferred(std::mem::take(
            &mut self.pending_out,
        )))
    }

    fn quit(&mut self) -> Result<CommandDispatch> {
        Ok(CommandDispatch::ready(CommandResult::Quit))
    }
}

impl Dialect for PiRpc {
    fn name(&self) -> &'static str {
        "pi"
    }

    fn init(&mut self, cfg: &SessionParams) -> Vec<OutFrame> {
        self.cfg = cfg.clone();
        self.state = SessionState::Initializing;
        self.session_started = false;
        self.announced_session_ref = None;
        debug!(
            target: "lucarne::dialects::pi_rpc",
            model = cfg.model.as_str(),
            cwd = cfg.cwd.as_str(),
            "pi rpc dialect initialized"
        );
        // Send initial get_state to discover sessionId/sessionFile.
        // Must return the frame directly (not via pending_out) so the
        // runtime writes it before the stdin/stdout pump loop starts.
        let id = self.next_id();
        let cmd = json!({"id": id, "type": "get_state"});
        self.pending
            .insert(id.clone(), PendingKind::GetState { initial: true });
        let mut bytes = match serde_json::to_vec(&cmd) {
            Ok(bytes) => bytes,
            Err(err) => {
                self.pending.remove(&id);
                debug!(
                    target: "lucarne::dialects::pi_rpc",
                    error = %err,
                    "failed to serialize initial pi rpc get_state request"
                );
                return Vec::new();
            }
        };
        bytes.push(b'\n');
        vec![OutFrame::stdin(bytes)]
    }

    fn translate(&mut self, frame: &[u8]) -> Vec<Event> {
        let trimmed = frame
            .iter()
            .position(|b| !b.is_ascii_whitespace())
            .map(|i| &frame[i..])
            .unwrap_or(frame);
        if trimmed.first() != Some(&b'{') {
            return Vec::new();
        }
        let env: Envelope = match serde_json::from_slice(frame) {
            Ok(v) => v,
            Err(err) => {
                debug!(
                    target: "lucarne::dialects::pi_rpc",
                    error = %err,
                    "pi-rpc frame skipped"
                );
                return Vec::new();
            }
        };

        if env.is_response() {
            let id = env.id.unwrap_or_default();
            let command = env.command.unwrap_or_default();
            let success = env.success.unwrap_or(true);
            return self.handle_response(
                &id,
                &command,
                success,
                env.data.as_ref(),
                env.error.as_deref().unwrap_or(""),
            );
        }

        if env.is_extension_ui_request() {
            let id = env.id.as_deref().unwrap_or("");
            let method = env.method.as_deref().unwrap_or("");
            return self.handle_extension_ui_request(id, method, &env);
        }

        self.handle_event(&env)
    }

    fn encode_user_message(&mut self, input: &Input) -> Result<Vec<OutFrame>> {
        if self.state == SessionState::Initializing {
            self.pending_prompts.push_back(input.clone());
            return Ok(Vec::new());
        }
        let cmd_type = if self.state == SessionState::Streaming || self.turn_active {
            "follow_up"
        } else {
            "prompt"
        };
        let mut cmd = json!({
            "id": self.next_id(),
            "type": cmd_type,
            "message": input.text,
        });
        if !input.images.is_empty() {
            let images: Vec<Value> = input
                .images
                .iter()
                .map(|img| {
                    json!({"type": "image", "source": {"type": "base64", "media_type": img.media_type, "data": base64_display(&img.data)}})
                })
                .collect();
            cmd["images"] = json!(images);
        }
        let mut bytes = serde_json::to_vec(&cmd)?;
        bytes.push(b'\n');
        self.turn_active = true;
        Ok(vec![OutFrame::stdin(bytes)])
    }

    fn encode_permission_response(
        &mut self,
        req_id: &str,
        r: &PermissionResponse,
    ) -> Result<Vec<OutFrame>> {
        let ext = self
            .extension_requests
            .remove(req_id)
            .ok_or_else(|| LucarneError::dialect("pi: unknown permission request id"))?;
        let body = match (ext.method, r.decision) {
            (ExtensionMethod::Confirm, Decision::Allow) => {
                json!({"type":"extension_ui_response","id":ext.id,"confirmed":true})
            }
            (ExtensionMethod::Confirm, Decision::Deny) => {
                json!({"type":"extension_ui_response","id":ext.id,"confirmed":false})
            }
            (ExtensionMethod::Select, Decision::Allow) => {
                let val = ext
                    .question_id
                    .as_deref()
                    .and_then(|question_id| r.answers.get(question_id))
                    .map(|answer| {
                        answer
                            .answers
                            .first()
                            .cloned()
                            .filter(|s| !s.is_empty())
                            .unwrap_or_else(|| answer.text.clone())
                    })
                    .unwrap_or_default();
                json!({"type":"extension_ui_response","id":ext.id,"value": val})
            }
            (ExtensionMethod::Select, Decision::Deny) => {
                json!({"type":"extension_ui_response","id":ext.id,"cancelled":true})
            }
            (ExtensionMethod::Other, _) => {
                json!({"type":"extension_ui_response","id":ext.id,"cancelled":true})
            }
        };
        let mut bytes = serde_json::to_vec(&body)?;
        bytes.push(b'\n');
        Ok(vec![OutFrame::stdin(bytes)])
    }

    fn encode_interrupt(&mut self) -> Result<Vec<OutFrame>> {
        let cmd = json!({"id": self.next_id(), "type": "abort"});
        let mut bytes = serde_json::to_vec(&cmd)?;
        bytes.push(b'\n');
        Ok(vec![OutFrame::stdin(bytes)])
    }

    fn drain_out_frames(&mut self) -> Vec<OutFrame> {
        std::mem::take(&mut self.pending_out)
    }

    fn command_catalog(&self) -> AgentCommandCatalog {
        self.command_catalog.clone()
    }

    fn handle_system_command(
        &mut self,
        command: &AgentCommandInvocation,
    ) -> Result<CommandDispatch> {
        match normalize_agent_command_name(command.name.as_str()) {
            "model" => match model_args(command) {
                Some((model, reasoning)) => self.set_model(&model, reasoning.as_deref()),
                None => self.list_models(),
            },
            "permissions" => match permission_arg(command) {
                Some(mode) => self.set_permissions(&mode),
                None => self.list_permissions(),
            },
            "skills" => self.list_skills(),
            "status" => self.status(),
            "new" => self.new_session_command(),
            "quit" => self.quit(),
            "fork" => match fork_name(command) {
                Some(target) => self.fork(Some(&target)),
                None => self.list_fork(),
            },
            "list_commands" => self.list_commands(),
            "list_models" => self.list_models(),
            name => Err(LucarneError::dialect(format!(
                "pi: unsupported system command {name:?}"
            ))),
        }
    }

    fn handle_native_command(
        &mut self,
        command: &AgentCommandInvocation,
    ) -> Result<CommandDispatch> {
        let name = normalize_agent_command_name(command.name.as_str());
        match name {
            "steer" => {
                let message = command.args.as_deref().unwrap_or("");
                let id = self.next_id();
                let cmd = json!({"id": id, "type": "steer", "message": message});
                self.enqueue_rpc(cmd, PendingKind::Generic("steer".into()))?;
                Ok(CommandDispatch::deferred(std::mem::take(
                    &mut self.pending_out,
                )))
            }
            "abort" => {
                let id = self.next_id();
                let cmd = json!({"id": id, "type": "abort"});
                self.enqueue_rpc(cmd, PendingKind::Generic("abort".into()))?;
                Ok(CommandDispatch::deferred(std::mem::take(
                    &mut self.pending_out,
                )))
            }
            "set_auto_retry" => self.set_auto_retry(true),
            "abort_retry" => self.abort_retry(),
            _ => {
                if let Some(catalog_name) = self.catalog_command_name(name) {
                    return self.invoke_provider_slash_command(
                        catalog_name.as_str(),
                        command.args.as_deref(),
                    );
                }
                Err(LucarneError::dialect(format!(
                    "pi: unknown command: {name}"
                )))
            }
        }
    }

    fn on_exit(&mut self, exit_code: i32, err: Option<String>) -> Vec<Event> {
        let reason = err.unwrap_or_else(|| {
            if exit_code != 0 {
                format!("pi exited with code {exit_code}")
            } else {
                String::new()
            }
        });
        let resume = self.session_file.clone().map(|path| ResumeHandle {
            version: 1,
            data: {
                let mut m = BTreeMap::new();
                m.insert("session_path".into(), Value::String(path));
                if !self.cfg.cwd.is_empty() {
                    m.insert("cwd".into(), Value::String(self.cfg.cwd.clone()));
                }
                m
            },
        });
        vec![Event::new(Payload::SessionClosed(SessionClosed {
            reason,
            resume,
        }))]
    }
}

// ---------------------------------------------------------------------------
// Response handler
// ---------------------------------------------------------------------------

impl PiRpc {
    fn fail_pending(
        &mut self,
        id: &str,
        kind: Option<PendingKind>,
        command: &str,
        error: &str,
    ) -> Vec<Event> {
        let label = kind
            .as_ref()
            .map(|k| match k {
                PendingKind::Generic(s) => s.as_str(),
                PendingKind::ProviderCommand { name } => name.as_str(),
                _ => command,
            })
            .unwrap_or(command);
        let error_text = error.trim();
        let message = if error_text.is_empty() {
            format!("pi rpc {label} failed")
        } else {
            format!("pi rpc {label} failed: {error_text}")
        };
        debug!(
            target: "lucarne::dialects::pi_rpc",
            id, command, label, error = error_text,
            "pi rpc command failed"
        );

        let initial_get_state_failed =
            matches!(kind, Some(PendingKind::GetState { initial: true }));
        let mut events = command_result_events(
            "adapter-command",
            CommandResult::Message(CommandMessage::text(message.clone())),
        );
        if initial_get_state_failed {
            events.push(Event::new(Payload::SessionClosed(SessionClosed {
                reason: message,
                resume: None,
            })));
        }
        events
    }

    fn handle_response(
        &mut self,
        id: &str,
        command: &str,
        success: bool,
        data: Option<&Value>,
        error: &str,
    ) -> Vec<Event> {
        let kind = self.pending.remove(id);
        if !success {
            return self.fail_pending(id, kind, command, error);
        }

        let data = data.cloned().unwrap_or(Value::Null);
        match kind {
            Some(PendingKind::GetState { initial: true }) => {
                self.update_session_identity(&data);
                self.state = SessionState::Idle;
                self.flush_pending_prompts();
                Vec::new()
            }
            Some(PendingKind::GetState { initial: false }) => {
                self.update_session_identity(&data);
                if let Some(status) = self.pending_status.as_mut() {
                    status.state = data.clone();
                    let id = self.next_id();
                    let cmd = json!({"id": id, "type": "get_session_stats"});
                    if let Err(err) = self.enqueue_rpc(cmd, PendingKind::GetSessionStats) {
                        warn!(
                            target: "lucarne::dialects::pi_rpc",
                            error = %err,
                            "pi rpc status stats request enqueue failed"
                        );
                        self.pending_status = None;
                    }
                }
                Vec::new()
            }
            Some(PendingKind::GetAvailableModels) => {
                self.model_catalog = build_model_catalog(&data);
                command_result_events(
                    "adapter-command",
                    CommandResult::Models(self.model_catalog.clone()),
                )
            }
            Some(PendingKind::SetModel { thinking }) => {
                if let Some(thinking) = thinking {
                    let id = self.next_id();
                    let cmd = json!({"id": id, "type": "set_thinking_level", "level": thinking});
                    if let Err(err) = self.enqueue_rpc(
                        cmd,
                        PendingKind::SetThinkingLevel {
                            model: self.cfg.model.clone(),
                            thinking,
                        },
                    ) {
                        warn!(
                            target: "lucarne::dialects::pi_rpc",
                            error = %err,
                            "pi rpc thinking level request enqueue failed"
                        );
                        let selection = ModelSelection {
                            model: self.cfg.model.clone().into(),
                            reasoning: None,
                        };
                        return command_result_events(
                            "adapter-command",
                            CommandResult::ModelChanged(selection),
                        );
                    }
                    Vec::new()
                } else {
                    let selection = ModelSelection {
                        model: self.cfg.model.clone().into(),
                        reasoning: None,
                    };
                    command_result_events("adapter-command", CommandResult::ModelChanged(selection))
                }
            }
            Some(PendingKind::SetThinkingLevel { model, thinking }) => {
                let selection = ModelSelection {
                    model: model.into(),
                    reasoning: Some(thinking.into()),
                };
                command_result_events("adapter-command", CommandResult::ModelChanged(selection))
            }
            Some(PendingKind::GetCommands) => {
                self.command_catalog = build_command_catalog(&data);
                command_result_events(
                    "adapter-command",
                    CommandResult::Commands(self.command_catalog.clone()),
                )
            }
            Some(PendingKind::GetSkills) => {
                self.command_catalog = build_command_catalog(&data);
                command_result_events(
                    "adapter-command",
                    CommandResult::Skills(build_skill_catalog(&data)),
                )
            }
            Some(PendingKind::GetForkMessages) => {
                let targets: Vec<AgentForkTarget> = data
                    .get("messages")
                    .and_then(|v| v.as_array())
                    .map(|arr| {
                        arr.iter()
                            .filter_map(|m| {
                                Some(AgentForkTarget {
                                    id: m.get("entryId")?.as_str()?.into(),
                                    label: m
                                        .get("text")
                                        .and_then(|s| s.as_str())
                                        .map(|s| s.to_string().into()),
                                    description: None,
                                })
                            })
                            .collect()
                    })
                    .unwrap_or_default();
                command_result_events(
                    "adapter-command",
                    CommandResult::ForkTargets(AgentForkTargetCatalog { targets }),
                )
            }
            Some(PendingKind::Fork { .. }) => {
                if data
                    .get("cancelled")
                    .and_then(|v| v.as_bool())
                    .unwrap_or(false)
                {
                    command_result_events(
                        "adapter-command",
                        CommandResult::Message(CommandMessage::text("fork cancelled")),
                    )
                } else {
                    let source_session_ref = fork_response_session_ref(&data)
                        .map(|r| r.0.to_string())
                        .or_else(|| self.current_session_ref());
                    let id = self.next_id();
                    let cmd = json!({"id": id, "type": "get_state"});
                    if let Err(err) =
                        self.enqueue_rpc(cmd, PendingKind::ForkGetState { source_session_ref })
                    {
                        warn!(
                            target: "lucarne::dialects::pi_rpc",
                            error = %err,
                            "pi rpc fork state refresh enqueue failed"
                        );
                        return command_result_events(
                            "adapter-command",
                            CommandResult::Message(CommandMessage::text(
                                "pi fork failed: could not refresh session state",
                            )),
                        );
                    }
                    Vec::new()
                }
            }
            Some(PendingKind::ForkGetState { source_session_ref }) => {
                self.update_session_identity(&data);
                let session_ref = self
                    .durable_current_session_ref()
                    .map(|session_ref| SessionRef(session_ref.into()));
                let source_session_ref = source_session_ref.map(|s| SessionRef(s.into()));
                let mut events = Vec::new();
                if let Some(session_ref) = session_ref.as_ref() {
                    if let Some(event) = self.announce_session_ref(session_ref.0.as_str()) {
                        events.push(event);
                    }
                }
                events.extend(command_result_events(
                    "adapter-command",
                    CommandResult::Forked(AgentForkResult {
                        session_ref,
                        source_session_ref,
                    }),
                ));
                events
            }
            Some(PendingKind::NewSession) => {
                self.update_session_identity(&data);
                let session_ref = self.current_session_ref();
                let mut events = Vec::new();
                if let Some(session_ref) = session_ref.as_deref() {
                    events.push(session_started_event(&self.cfg, session_ref));
                }
                events.extend(command_result_events(
                    "adapter-command",
                    CommandResult::NewConversation(Conversation {
                        session_ref: session_ref.map(|s| SessionRef(s.into())),
                    }),
                ));
                events
            }
            Some(PendingKind::GetSessionStats) => {
                if let Some(status_build) = self.pending_status.take() {
                    let session_ref = self
                        .session_file
                        .as_deref()
                        .or(self.session_id.as_deref())
                        .map(str::trim)
                        .filter(|s| !s.is_empty());
                    let status = build_status(
                        &self.cfg,
                        self.cli_version.as_deref(),
                        session_ref,
                        &status_build.state,
                        &data,
                    );
                    command_result_events("adapter-command", CommandResult::Status(status))
                } else {
                    Vec::new()
                }
            }
            Some(PendingKind::SetAutoRetry) => Vec::new(),
            Some(PendingKind::AbortRetry) => Vec::new(),
            Some(PendingKind::ProviderCommand { name }) => {
                debug!(
                    target: "lucarne::dialects::pi_rpc",
                    command = name,
                    "pi provider slash command accepted"
                );
                Vec::new()
            }
            Some(PendingKind::Generic(_)) => Vec::new(),
            None => Vec::new(),
        }
    }
}

// ---------------------------------------------------------------------------
// Extension UI request handler (permission intercept)
// ---------------------------------------------------------------------------

impl PiRpc {
    fn handle_extension_ui_request(
        &mut self,
        id: &str,
        method: &str,
        env: &Envelope,
    ) -> Vec<Event> {
        let ext_method = match method {
            "confirm" => ExtensionMethod::Confirm,
            "select" => ExtensionMethod::Select,
            _ => ExtensionMethod::Other,
        };

        let question_id = if ext_method == ExtensionMethod::Select {
            Some(select_question_id(env))
        } else {
            None
        };

        if matches!(
            ext_method,
            ExtensionMethod::Confirm | ExtensionMethod::Select
        ) {
            self.extension_requests.insert(
                id.to_string(),
                ExtensionRequestKind {
                    id: id.to_string(),
                    method: ext_method,
                    question_id: question_id.clone(),
                },
            );
        }

        match ext_method {
            ExtensionMethod::Confirm => {
                let question_text = env
                    .message
                    .as_ref()
                    .and_then(|v| v.as_str())
                    .unwrap_or("Allow this operation?");
                let questions = vec![PermissionQuestion {
                    question: question_text.to_string(),
                    options: vec![
                        PermissionQuestionOption {
                            label: "Allow".into(),
                            description: "Approve".into(),
                        },
                        PermissionQuestionOption {
                            label: "Deny".into(),
                            description: "Reject".into(),
                        },
                    ],
                    ..Default::default()
                }];
                vec![Event::new(Payload::PermissionRequest(PermissionRequest {
                    req_id: id.to_string(),
                    tool: String::new(),
                    input: env.args.clone(),
                    risk: env
                        .args
                        .as_ref()
                        .and_then(|a| a.get("risk"))
                        .and_then(|v| v.as_str())
                        .and_then(|s| match s {
                            "high" => Some(Risk::High),
                            "medium" => Some(Risk::Medium),
                            _ => Some(Risk::Low),
                        })
                        .unwrap_or_default(),
                    questions,
                }))]
            }
            ExtensionMethod::Select => {
                let question_text = env
                    .message
                    .as_ref()
                    .and_then(|v| v.as_str())
                    .unwrap_or("Select an option")
                    .to_string();
                let options: Vec<PermissionQuestionOption> = env
                    .args
                    .as_ref()
                    .and_then(|v| v.get("options"))
                    .and_then(|v| v.as_array())
                    .map(|arr| {
                        arr.iter()
                            .filter_map(|opt| {
                                Some(PermissionQuestionOption {
                                    label: opt.get("label")?.as_str()?.to_string(),
                                    description: opt
                                        .get("description")
                                        .and_then(|s| s.as_str())
                                        .unwrap_or("")
                                        .to_string(),
                                })
                            })
                            .collect()
                    })
                    .unwrap_or_default();
                let questions = vec![PermissionQuestion {
                    id: question_id.unwrap_or_else(|| "q".to_string()),
                    question: question_text,
                    options,
                    ..Default::default()
                }];
                vec![Event::new(Payload::PermissionRequest(PermissionRequest {
                    req_id: id.to_string(),
                    tool: String::new(),
                    input: env.args.clone(),
                    risk: Risk::Low,
                    questions,
                }))]
            }
            ExtensionMethod::Other => {
                // Auto-cancel unknown extension requests to avoid hanging pi.
                let cancel = json!({"type":"extension_ui_response","id":id,"cancelled":true});
                let mut bytes = serde_json::to_vec(&cancel).unwrap_or_default();
                bytes.push(b'\n');
                self.pending_out.push(OutFrame::stdin(bytes));
                Vec::new()
            }
        }
    }
}

fn select_question_id(env: &Envelope) -> String {
    env.args
        .as_ref()
        .and_then(|v| {
            v.get("questionId")
                .or_else(|| v.get("question_id"))
                .or_else(|| v.get("id"))
        })
        .and_then(|v| v.as_str())
        .map(str::trim)
        .filter(|s| !s.is_empty())
        .unwrap_or("q")
        .to_string()
}

// ---------------------------------------------------------------------------
// Event handler — ported from dialects/pi.rs
// ---------------------------------------------------------------------------

impl PiRpc {
    fn reset_turn_buffers(&mut self) {
        self.thinking_delta_buffer.clear();
        self.turn_emitted_reasoning = false;
    }

    fn flush_thinking_delta_buffer(&mut self, out: &mut Vec<Event>) -> bool {
        let text = std::mem::take(&mut self.thinking_delta_buffer);
        let text = text.trim();
        if text.is_empty() {
            return false;
        }
        out.push(tl(event::new_timeline_reasoning("", text)));
        self.turn_emitted_reasoning = true;
        true
    }

    fn translate_assistant_event(&mut self, raw: Option<&Value>) -> Vec<Event> {
        let Some(v) = raw else {
            return Vec::new();
        };
        let ty = v.get("type").and_then(|x| x.as_str()).unwrap_or("");
        let delta = v.get("delta").and_then(|x| x.as_str()).unwrap_or("");
        if delta.is_empty() {
            return Vec::new();
        }
        match ty {
            "text_delta" => {
                let mut evs = Vec::new();
                self.flush_thinking_delta_buffer(&mut evs);
                evs.push(tl(event::new_timeline_assistant("", delta, true)));
                evs
            }
            "thinking_delta" => {
                self.thinking_delta_buffer.push_str(delta);
                Vec::new()
            }
            _ => Vec::new(),
        }
    }

    fn update_session_identity(&mut self, data: &Value) {
        if let Some(session_id) = data
            .get("sessionId")
            .or_else(|| data.get("session_id"))
            .and_then(|v| v.as_str())
            .map(str::trim)
            .filter(|s| !s.is_empty())
        {
            self.session_id = Some(session_id.to_string());
        }
        if let Some(session_file) = data
            .get("sessionFile")
            .or_else(|| data.get("session_file"))
            .and_then(|v| v.as_str())
            .map(str::trim)
            .filter(|s| !s.is_empty())
        {
            self.session_file = Some(session_file.to_string());
        }
    }

    fn current_session_ref(&self) -> Option<String> {
        pi_session_ref(self.session_id.as_deref(), self.session_file.as_deref())
    }

    fn durable_current_session_ref(&self) -> Option<String> {
        let session_file = self
            .session_file
            .as_deref()
            .map(str::trim)
            .filter(|path| !path.is_empty())?;
        if !Path::new(session_file).is_file() {
            return None;
        }
        pi_session_ref(self.session_id.as_deref(), Some(session_file))
    }

    fn announce_session_ref(&mut self, session_ref: &str) -> Option<Event> {
        if session_ref.is_empty() || self.announced_session_ref.as_deref() == Some(session_ref) {
            return None;
        }
        self.announced_session_ref = Some(session_ref.to_string());
        Some(session_started_event(&self.cfg, session_ref))
    }

    fn announce_durable_session_ref(&mut self) -> Option<Event> {
        let session_ref = self.durable_current_session_ref()?;
        self.announce_session_ref(&session_ref)
    }

    fn handle_event(&mut self, env: &Envelope) -> Vec<Event> {
        match env.r#type.as_str() {
            "agent_start" => {
                let first_start = !self.session_started;
                self.session_started = true;
                if self.state == SessionState::Initializing {
                    self.state = if self.turn_active {
                        SessionState::Streaming
                    } else {
                        SessionState::Idle
                    };
                }
                debug!(
                    target: "lucarne::dialects::pi_rpc",
                    session_id = %self.current_session_ref().unwrap_or_default(),
                    model = self.cfg.model.as_str(),
                    state = ?self.state,
                    "pi agent started"
                );
                if first_start {
                    let session_ref = self.current_session_ref().unwrap_or_default();
                    if !session_ref.is_empty() {
                        self.announced_session_ref = Some(session_ref.clone());
                    }
                    vec![session_started_event(&self.cfg, &session_ref)]
                } else {
                    Vec::new()
                }
            }

            "turn_start" => match self.state {
                SessionState::Idle | SessionState::Initializing => {
                    self.reset_turn_buffers();
                    self.state = SessionState::Streaming;
                    self.turn_active = true;
                    vec![Event::new(Payload::TurnStarted(event::TurnStarted {
                        turn_id: String::new(),
                    }))]
                }
                SessionState::Streaming => {
                    debug!(
                        target: "lucarne::dialects::pi_rpc",
                        state = ?self.state,
                        "pi turn_start ignored while already streaming"
                    );
                    Vec::new()
                }
            },

            "message_update" => {
                self.translate_assistant_event(env.assistant_message_event.as_ref())
            }

            "tool_execution_start" => {
                let tool_call_id = env.tool_call_id.as_deref().unwrap_or("");
                let tool_name = env.tool_name.as_deref().unwrap_or("");
                let mut evs = Vec::new();
                self.flush_thinking_delta_buffer(&mut evs);
                evs.push(tl(event::new_timeline_tool_call(
                    tool_call_id,
                    event::tool_call(tool_name, env.args.clone().unwrap_or(Value::Null)),
                )));
                evs
            }

            "tool_execution_update" => {
                let tool_call_id = env.tool_call_id.as_deref().unwrap_or("");
                let text = decode_pi_string(env.partial_result.as_ref().or(env.result.as_ref()));
                if text.is_empty() {
                    Vec::new()
                } else {
                    let mut evs = Vec::new();
                    self.flush_thinking_delta_buffer(&mut evs);
                    let result = ToolResult {
                        output: text,
                        partial: true,
                        ..Default::default()
                    };
                    evs.push(tl(event::new_timeline_tool_result(
                        "",
                        tool_call_id,
                        result,
                    )));
                    evs
                }
            }

            "tool_execution_end" => {
                let tool_call_id = env.tool_call_id.as_deref().unwrap_or("");
                let text = decode_pi_tool_result_text(env.result.as_ref());
                let mut evs = Vec::new();
                self.flush_thinking_delta_buffer(&mut evs);
                let mut result = ToolResult {
                    output: text.clone(),
                    ..Default::default()
                };
                let is_error = env.is_error.unwrap_or(false);
                if is_error {
                    result.error = text;
                    result.output = String::new();
                }
                evs.push(tl(event::new_timeline_tool_result(
                    "",
                    tool_call_id,
                    result,
                )));
                if !is_error {
                    evs.extend(decode_pi_image_attachments(
                        tool_call_id,
                        env.result.as_ref(),
                    ));
                }
                evs
            }

            "turn_end" => {
                let terminal = turn_end_is_terminal(env.message.as_ref());
                let mut evs = Vec::new();
                if terminal {
                    if let Some(event) = self.announce_durable_session_ref() {
                        evs.push(event);
                    }
                }
                self.flush_thinking_delta_buffer(&mut evs);
                evs.extend(translate_turn_end(
                    env.message.as_ref(),
                    env.tool_results.as_ref(),
                    terminal,
                    !self.turn_emitted_reasoning,
                ));
                if terminal && self.state == SessionState::Streaming {
                    self.state = SessionState::Idle;
                    self.turn_active = false;
                    // Flush any buffered prompts now that the turn completed.
                    self.flush_pending_prompts();
                } else if !terminal {
                    debug!(
                        target: "lucarne::dialects::pi_rpc",
                        state = ?self.state,
                        "pi non-terminal turn_end ignored"
                    );
                } else {
                    debug!(
                        target: "lucarne::dialects::pi_rpc",
                        state = ?self.state,
                        "pi turn_end processed without streaming state"
                    );
                }
                evs
            }

            "usage" => match decode_pi_usage(env.usage.as_ref()) {
                Some(u) => vec![Event::new(Payload::UsageDelta(UsageDelta { delta: u }))],
                None => Vec::new(),
            },

            "error" => {
                let msg = decode_pi_string(env.message.as_ref()).trim().to_string();
                if msg.is_empty() {
                    Vec::new()
                } else {
                    vec![Event::new(Payload::TurnFailed(TurnFailed {
                        error: msg,
                        ..Default::default()
                    }))]
                }
            }

            "auto_retry_end" => {
                if env.success.unwrap_or(false) {
                    return Vec::new();
                }
                let err = env.final_error.as_deref().unwrap_or("").trim();
                let err = if err.is_empty() {
                    "pi exhausted automatic retries".to_string()
                } else {
                    err.to_string()
                };
                vec![Event::new(Payload::TurnFailed(TurnFailed {
                    error: err,
                    ..Default::default()
                }))]
            }

            "queue_update" => vec![pi_log("pi: turn queued")],
            "compaction_start" => vec![pi_log("pi: context compaction started")],
            "compaction_end" => vec![pi_log("pi: context compaction complete")],
            "auto_retry_start" => vec![pi_log("pi: auto-retry started")],
            "agent_end" => vec![pi_log("pi: agent ended")],

            // No-op events.
            "message_start" | "message_end" => Vec::new(),

            _ => Vec::new(),
        }
    }
}

// ---------------------------------------------------------------------------
// Translation helpers — shared with the old pi.rs dialect
// ---------------------------------------------------------------------------

fn tl(item: TimelineItem) -> Event {
    Event::new(Payload::Timeline(Timeline { item }))
}

fn pi_log(text: impl Into<String>) -> Event {
    Event::new(Payload::Log(LogLine {
        level: "info".into(),
        stream: "pi".into(),
        text: text.into(),
    }))
}

fn session_started_event(cfg: &SessionParams, session_ref: &str) -> Event {
    Event::new(Payload::SessionStarted(SessionStarted {
        session_id: session_ref.to_string(),
        model: cfg.model.clone(),
    }))
}

fn translate_turn_end(
    msg_raw: Option<&Value>,
    tool_results_raw: Option<&Value>,
    terminal: bool,
    include_reasoning: bool,
) -> Vec<Event> {
    let mut evs = Vec::new();
    let usage = msg_raw.and_then(|m| decode_pi_usage(m.get("usage")));
    let failure_reason = terminal.then(|| turn_end_failure_reason(msg_raw)).flatten();
    if let Some(m) = msg_raw {
        if include_reasoning {
            let reasoning = decode_pi_reasoning(m.get("content"));
            if !reasoning.is_empty() {
                evs.push(tl(event::new_timeline_reasoning("", &reasoning)));
            }
        }
        let text = decode_pi_content(m.get("content"));
        if !text.is_empty() {
            evs.push(tl(event::new_timeline_assistant("", &text, false)));
        }
    }
    evs.extend(decode_pi_tool_results(tool_results_raw));
    if terminal {
        if let Some(error) = failure_reason {
            evs.push(Event::new(Payload::TurnFailed(TurnFailed {
                error,
                ..Default::default()
            })));
        } else {
            evs.push(Event::new(Payload::TurnCompleted(TurnCompleted {
                turn_id: String::new(),
                usage,
            })));
        }
    }
    evs
}

fn turn_end_is_terminal(msg_raw: Option<&Value>) -> bool {
    let Some(m) = msg_raw else {
        return false;
    };
    let stop_reason = pi_stop_reason(m);
    if matches!(stop_reason, "toolUse" | "tool_use" | "function_call") {
        return false;
    }
    if !stop_reason.is_empty() {
        return true;
    }
    !decode_pi_content(m.get("content")).is_empty()
}

fn turn_end_failure_reason(msg_raw: Option<&Value>) -> Option<String> {
    let m = msg_raw?;
    let stop_reason = pi_stop_reason(m);
    if !matches!(stop_reason, "error" | "aborted" | "cancelled" | "canceled") {
        return None;
    }
    let message = m
        .get("errorMessage")
        .or_else(|| m.get("error_message"))
        .and_then(Value::as_str)
        .map(str::trim)
        .filter(|s| !s.is_empty())
        .unwrap_or(stop_reason);
    Some(message.to_string())
}

fn pi_stop_reason(message: &Value) -> &str {
    message
        .get("stopReason")
        .or_else(|| message.get("stop_reason"))
        .and_then(Value::as_str)
        .map(str::trim)
        .unwrap_or("")
}

fn decode_pi_tool_results(raw: Option<&Value>) -> Vec<Event> {
    let Some(arr) = raw.and_then(|v| v.as_array()) else {
        return Vec::new();
    };
    let mut out = Vec::new();
    for tr in arr {
        let id = tr.get("toolCallId").and_then(|v| v.as_str()).unwrap_or("");
        if id.is_empty() {
            continue;
        }
        let text = decode_pi_string(tr.get("content"));
        let is_error = tr.get("isError").and_then(|v| v.as_bool()).unwrap_or(false);
        let mut result = ToolResult {
            output: text.clone(),
            ..Default::default()
        };
        if is_error {
            result.error = text;
            result.output = String::new();
        }
        out.push(tl(event::new_timeline_tool_result("", id, result)));
    }
    out
}

fn decode_pi_tool_result_text(raw: Option<&Value>) -> String {
    let Some(value) = raw else {
        return String::new();
    };
    if let Some(content) = value.get("content") {
        return decode_pi_tool_content_text(content);
    }
    decode_pi_string(raw)
}

fn decode_pi_tool_content_text(value: &Value) -> String {
    if let Some(text) = value.as_str() {
        return text.to_string();
    }
    if let Some(items) = value.as_array() {
        let mut out = String::new();
        for item in items {
            let text = decode_pi_tool_content_text(item);
            if !text.is_empty() {
                out.push_str(&text);
            }
        }
        return out;
    }
    if let Some(obj) = value.as_object() {
        if obj.get("type").and_then(Value::as_str) == Some("image") {
            return String::new();
        }
        for key in ["text", "content", "output"] {
            if let Some(child) = obj.get(key) {
                let text = decode_pi_tool_content_text(child);
                if !text.is_empty() {
                    return text;
                }
            }
        }
    }
    String::new()
}

fn decode_pi_image_attachments(tool_call_id: &str, raw: Option<&Value>) -> Vec<Event> {
    let Some(value) = raw else {
        return Vec::new();
    };
    let details = value.get("details");
    let Some(content) = value.get("content") else {
        return Vec::new();
    };
    let Some(items) = content.as_array() else {
        return Vec::new();
    };
    let image_items = items
        .iter()
        .filter(|item| item.get("type").and_then(Value::as_str) == Some("image"))
        .collect::<Vec<_>>();
    if image_items.is_empty() {
        return Vec::new();
    }

    let image_count = image_items.len();
    image_items
        .into_iter()
        .enumerate()
        .filter_map(|(idx, item)| {
            let data = item.get("data").and_then(Value::as_str)?.trim();
            if data.is_empty() {
                return None;
            }
            let media_type = item
                .get("mimeType")
                .or_else(|| item.get("mediaType"))
                .or_else(|| item.get("media_type"))
                .and_then(Value::as_str)
                .map(str::trim)
                .filter(|text| !text.is_empty())
                .unwrap_or("image/png");
            let id = pi_image_attachment_id(details, tool_call_id, idx, image_count);
            let filename = pi_image_attachment_filename(details, &id, media_type);
            let data_base64 = strip_data_url_prefix(data);
            Some(Event::new(Payload::Attachment(event::Attachment {
                id: SmolStr::from(id),
                filename: SmolStr::from(filename),
                media_type: SmolStr::from(media_type),
                data_base64: data_base64.to_string(),
                caption: None,
            })))
        })
        .collect()
}

fn pi_image_attachment_id(
    details: Option<&Value>,
    tool_call_id: &str,
    idx: usize,
    image_count: usize,
) -> String {
    let base = details
        .and_then(|details| {
            details
                .get("imageGenerationId")
                .or_else(|| details.get("image_generation_id"))
                .and_then(Value::as_str)
        })
        .map(str::trim)
        .filter(|text| !text.is_empty())
        .map(ToString::to_string)
        .or_else(|| {
            details
                .and_then(|details| {
                    details
                        .get("savedPath")
                        .or_else(|| details.get("saved_path"))
                })
                .and_then(Value::as_str)
                .and_then(|path| Path::new(path).file_stem())
                .and_then(|stem| stem.to_str())
                .map(str::trim)
                .filter(|text| !text.is_empty())
                .map(ToString::to_string)
        })
        .or_else(|| {
            let tool_call_id = tool_call_id.trim();
            (!tool_call_id.is_empty()).then(|| format!("{tool_call_id}-image"))
        })
        .unwrap_or_else(|| "image".to_string());

    if image_count > 1 {
        format!("{base}-{idx}")
    } else {
        base
    }
}

fn pi_image_attachment_filename(details: Option<&Value>, id: &str, media_type: &str) -> String {
    if let Some(filename) = details
        .and_then(|details| {
            details
                .get("savedPath")
                .or_else(|| details.get("saved_path"))
        })
        .and_then(Value::as_str)
        .and_then(|path| Path::new(path).file_name())
        .and_then(|name| name.to_str())
        .map(str::trim)
        .filter(|text| !text.is_empty())
    {
        return filename.to_string();
    }

    let ext = media_extension(media_type);
    format!("pi-image-{id}.{ext}")
}

fn media_extension(media_type: &str) -> &'static str {
    match media_type.trim().to_ascii_lowercase().as_str() {
        "image/jpeg" | "image/jpg" => "jpg",
        "image/gif" => "gif",
        "image/webp" => "webp",
        _ => "png",
    }
}

fn strip_data_url_prefix(data: &str) -> &str {
    data.split_once(",").map_or(data, |(_, payload)| payload)
}

fn decode_pi_content(raw: Option<&Value>) -> String {
    let Some(v) = raw else {
        return String::new();
    };
    if let Some(s) = v.as_str() {
        return s.to_string();
    }
    if let Some(arr) = v.as_array() {
        let mut out = String::new();
        for it in arr {
            if it.get("type").and_then(|x| x.as_str()) == Some("text") {
                if let Some(t) = it.get("text").and_then(|x| x.as_str()) {
                    out.push_str(t);
                }
            }
        }
        return out;
    }
    String::new()
}

fn decode_pi_reasoning(raw: Option<&Value>) -> String {
    let Some(v) = raw else {
        return String::new();
    };
    if let Some(arr) = v.as_array() {
        let mut out = String::new();
        for it in arr {
            if it.get("type").and_then(|x| x.as_str()) == Some("thinking") {
                if let Some(t) = it
                    .get("thinking")
                    .or_else(|| it.get("text"))
                    .and_then(|x| x.as_str())
                {
                    out.push_str(t);
                }
            }
        }
        return out.trim().to_string();
    }
    String::new()
}

fn decode_pi_string(raw: Option<&Value>) -> String {
    let Some(v) = raw else {
        return String::new();
    };
    if let Some(s) = v.as_str() {
        return s.to_string();
    }
    if let Some(arr) = v.as_array() {
        let mut out = String::new();
        for item in arr {
            let text = decode_pi_string(Some(item));
            if !text.is_empty() {
                out.push_str(&text);
            }
        }
        if !out.is_empty() {
            return out;
        }
    }
    if let Some(obj) = v.as_object() {
        for key in ["text", "content", "output"] {
            let text = decode_pi_string(obj.get(key));
            if !text.is_empty() {
                return text;
            }
        }
    }
    serde_json::to_string(v).unwrap_or_default()
}

fn decode_pi_usage(raw: Option<&Value>) -> Option<Usage> {
    let v = raw?;
    let get_i = |keys: &[&str]| -> i64 {
        for k in keys {
            if let Some(n) = v.get(*k).and_then(|x| x.as_i64()) {
                if n != 0 {
                    return n;
                }
            }
        }
        0
    };
    let mut cost = v.get("costUsd").and_then(|x| x.as_f64()).unwrap_or(0.0);
    if cost == 0.0 {
        if let Some(total) = v.pointer("/cost/total").and_then(|x| x.as_f64()) {
            cost = total;
        }
    }
    Some(Usage {
        input_tokens: get_i(&["inputTokens", "input"]),
        output_tokens: get_i(&["outputTokens", "output"]),
        cache_read_tokens: get_i(&["cachedInputTokens", "cacheRead"]),
        cache_write_tokens: get_i(&["cacheWrite", "cachedOutputTokens"]),
        cost_usd: cost,
    })
}

fn pi_session_ref(session_id: Option<&str>, session_file: Option<&str>) -> Option<String> {
    session_id
        .and_then(normalize_pi_session_ref)
        .or_else(|| session_file.and_then(normalize_pi_session_ref))
}

fn normalize_pi_session_ref(raw: &str) -> Option<String> {
    let raw = raw.trim();
    if raw.is_empty() {
        return None;
    }
    if raw.contains('/') || raw.ends_with(".jsonl") {
        let stem = Path::new(raw)
            .file_stem()
            .and_then(|stem| stem.to_str())
            .unwrap_or(raw)
            .trim();
        if let Some((_, suffix)) = stem.rsplit_once('_') {
            if looks_like_uuid(suffix) {
                return Some(suffix.to_string());
            }
        }
        return (!stem.is_empty()).then(|| stem.to_string());
    }
    Some(raw.to_string())
}

fn looks_like_uuid(s: &str) -> bool {
    let groups = [8, 4, 4, 4, 12];
    let mut parts = s.split('-');
    for expected in groups {
        let Some(part) = parts.next() else {
            return false;
        };
        if part.len() != expected || !part.chars().all(|ch| ch.is_ascii_hexdigit()) {
            return false;
        }
    }
    parts.next().is_none()
}

fn fork_response_session_ref(data: &Value) -> Option<SessionRef> {
    let source_file = data
        .get("sourceSessionFile")
        .or_else(|| data.get("source_session_file"))
        .and_then(Value::as_str);
    let source_id = data
        .get("sourceSessionId")
        .or_else(|| data.get("source_session_id"))
        .and_then(Value::as_str);
    pi_session_ref(source_id, source_file).map(|s| SessionRef(s.into()))
}

/// Base64-encode bytes for display in JSON (standard alphabet with padding).
fn base64_display(data: &[u8]) -> String {
    const TABLE: &[u8; 64] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";
    let mut out = String::with_capacity((data.len() + 2) / 3 * 4);
    for chunk in data.chunks(3) {
        let b0 = chunk[0] as u32;
        let b1 = if chunk.len() > 1 { chunk[1] as u32 } else { 0 };
        let b2 = if chunk.len() > 2 { chunk[2] as u32 } else { 0 };
        let n = (b0 << 16) | (b1 << 8) | b2;
        out.push(TABLE[(n >> 18 & 63) as usize] as char);
        out.push(TABLE[(n >> 12 & 63) as usize] as char);
        if chunk.len() > 1 {
            out.push(TABLE[(n >> 6 & 63) as usize] as char);
        } else {
            out.push('=');
        }
        if chunk.len() > 2 {
            out.push(TABLE[(n & 63) as usize] as char);
        } else {
            out.push('=');
        }
    }
    out
}

// ---------------------------------------------------------------------------
// Catalog builders
// ---------------------------------------------------------------------------

fn split_model_thinking(s: &str) -> (String, Option<String>) {
    let s = s.trim();
    let Some((model, thinking)) = s.rsplit_once(':') else {
        return (s.to_string(), None);
    };
    let model = model.trim();
    let thinking = thinking.trim();
    if model.is_empty() || thinking.is_empty() {
        return (s.to_string(), None);
    }
    (model.to_string(), Some(thinking.to_string()))
}

fn split_model(s: &str) -> (String, String) {
    let s = s.trim();
    if s.is_empty() {
        return (String::new(), String::new());
    }
    match s.split_once('/') {
        Some((provider, model)) => (provider.trim().to_string(), model.trim().to_string()),
        None => (String::new(), s.to_string()),
    }
}

fn build_model_catalog(data: &Value) -> AgentModelCatalog {
    let models: Vec<AgentModelOption> = data
        .get("models")
        .and_then(|v| v.as_array())
        .map(|arr| {
            arr.iter()
                .filter_map(|m| {
                    Some(AgentModelOption {
                        id: format!(
                            "{}/{}",
                            m.get("provider")?.as_str()?,
                            m.get("id")?.as_str()?
                        )
                        .into(),
                        display_name: Some(
                            format!(
                                "{}/{}",
                                m.get("provider")?.as_str()?,
                                m.get("id")?.as_str()?
                            )
                            .into(),
                        ),
                        description: m
                            .get("description")
                            .and_then(|s| s.as_str())
                            .map(|s| s.to_string().into()),
                        supported_reasoning: if m
                            .get("supportsReasoning")
                            .and_then(|v| v.as_bool())
                            .unwrap_or(false)
                        {
                            vec![AgentReasoningOption {
                                value: "high".into(),
                                description: Some("High reasoning".into()),
                                ..Default::default()
                            }]
                        } else {
                            vec![]
                        },
                    })
                })
                .collect()
        })
        .unwrap_or_default();
    AgentModelCatalog {
        models,
        ..Default::default()
    }
}

fn build_command_catalog(data: &Value) -> AgentCommandCatalog {
    let commands: Vec<AgentCommand> = data
        .get("commands")
        .and_then(|v| v.as_array())
        .map(|arr| {
            arr.iter()
                .filter(|c| !pi_command_is_skill(c))
                .filter_map(|c| {
                    let name = c.get("name")?.as_str()?;
                    let hint: SmolStr = c
                        .get("argumentHint")
                        .or_else(|| c.get("sourceInfo"))
                        .and_then(|s| s.as_str())
                        .unwrap_or("")
                        .into();
                    Some(AgentCommand {
                        name: name.into(),
                        description: c
                            .get("description")
                            .and_then(|s| s.as_str())
                            .map(|s| s.to_string().into()),
                        aliases: Vec::new(),
                        source: AgentCommandSource::ProviderNative,
                        input: if hint.is_empty() {
                            AgentCommandInput::None
                        } else {
                            AgentCommandInput::Text {
                                label: hint,
                                required: false,
                            }
                        },
                        completion: match name {
                            "set_auto_retry" | "abort_retry" => AgentCommandCompletion::NoOutputAck,
                            _ => AgentCommandCompletion::TurnCompleted,
                        },
                    })
                })
                .collect()
        })
        .unwrap_or_default();
    AgentCommandCatalog {
        commands,
        complete: true,
        revision: 0,
    }
}

fn build_skill_catalog(data: &Value) -> AgentSkillCatalog {
    let skills: Vec<AgentSkillSummary> = data
        .get("commands")
        .and_then(|v| v.as_array())
        .map(|arr| {
            arr.iter()
                .filter(|c| pi_command_is_skill(c))
                .filter_map(|c| {
                    Some(AgentSkillSummary {
                        name: c.get("name")?.as_str()?.into(),
                        display_name: c
                            .get("displayName")
                            .and_then(|s| s.as_str())
                            .map(|s| s.to_string().into()),
                        description: c
                            .get("description")
                            .and_then(|s| s.as_str())
                            .map(|s| s.to_string().into()),
                        source: c
                            .get("source")
                            .and_then(|s| s.as_str())
                            .map(|s| s.to_string().into()),
                        ..Default::default()
                    })
                })
                .collect()
        })
        .unwrap_or_default();
    AgentSkillCatalog { skills }
}

fn pi_command_is_skill(command: &Value) -> bool {
    command
        .get("source")
        .and_then(|s| s.as_str())
        .map(|s| s == "skill")
        .unwrap_or(false)
}

fn build_status(
    cfg: &SessionParams,
    cli_version: Option<&str>,
    session_ref: Option<&str>,
    state: &Value,
    stats: &Value,
) -> AgentStatus {
    let model = pi_status_model(state, cfg);
    let model_detail = pi_status_model_detail(state, model.as_deref());
    AgentStatus {
        version: pi_string_at(state, &["version", "cliVersion", "cli_version"])
            .or_else(|| cli_version.map(str::to_string))
            .map(Into::into),
        session_id: session_ref.map(|s| s.into()),
        directory: pi_string_at(
            state,
            &[
                "cwd",
                "directory",
                "currentWorkingDirectory",
                "current_working_directory",
            ],
        )
        .or_else(|| (!cfg.cwd.is_empty()).then(|| cfg.cwd.clone()))
        .map(Into::into),
        model: model.map(Into::into),
        model_detail: model_detail.map(Into::into),
        reasoning: pi_string_at(
            state,
            &[
                "thinkingLevel",
                "thinking_level",
                "reasoning",
                "reasoningEffort",
                "reasoning_effort",
            ],
        )
        .map(Into::into),
        permissions: pi_string_at(state, &["permissionMode", "permission_mode", "permissions"])
            .map(Into::into),
        account: pi_string_at(state, &["account", "apiKeySource", "api_key_source"])
            .map(Into::into),
        base_url: pi_string_at(state, &["baseUrl", "base_url"])
            .or_else(|| {
                state
                    .get("model")
                    .and_then(|model| pi_string_at(model, &["baseUrl", "base_url"]))
            })
            .map(Into::into),
        proxy: pi_string_at(state, &["proxy", "httpProxy", "http_proxy"]).map(Into::into),
        setting_sources: pi_string_at(state, &["settingSources", "setting_sources"])
            .map(Into::into),
        tokens: pi_token_usage(stats),
        context: pi_context_usage(stats),
        compactions: pi_u64_at(
            stats,
            &["compactions", "compactionCount", "compaction_count"],
        ),
        ..Default::default()
    }
}

fn pi_status_model(state: &Value, cfg: &SessionParams) -> Option<String> {
    pi_string_at(state, &["model", "currentModel", "current_model"])
        .or_else(|| {
            let model = state.get("model")?;
            let provider = pi_string_at(model, &["provider"])?;
            let id = pi_string_at(model, &["id", "modelId", "model_id"])?;
            Some(format!("{provider}/{id}"))
        })
        .or_else(|| {
            let provider = pi_string_at(state, &["provider"])?;
            let id = pi_string_at(state, &["modelId", "model_id"])?;
            Some(format!("{provider}/{id}"))
        })
        .or_else(|| pi_string_at(state, &["modelId", "model_id"]))
        .or_else(|| (!cfg.model.is_empty()).then(|| cfg.model.clone()))
}

fn pi_status_model_detail(state: &Value, model: Option<&str>) -> Option<String> {
    let detail = state
        .get("model")
        .and_then(|model| pi_string_at(model, &["name", "displayName", "display_name", "id"]))
        .or_else(|| {
            pi_string_at(
                state,
                &["modelId", "model_id", "modelDetail", "model_detail"],
            )
        })?;
    if model == Some(detail.as_str()) {
        None
    } else {
        Some(detail)
    }
}

fn pi_token_usage(stats: &Value) -> Option<AgentTokenUsage> {
    let token_root = stats
        .get("tokens")
        .or_else(|| stats.get("usage"))
        .unwrap_or(stats);
    let usage = AgentTokenUsage {
        input_tokens: pi_u64_at(token_root, &["inputTokens", "input_tokens", "input"]),
        output_tokens: pi_u64_at(token_root, &["outputTokens", "output_tokens", "output"]),
        total_tokens: pi_u64_at(token_root, &["totalTokens", "total_tokens", "total"]),
    };
    (usage.input_tokens.is_some() || usage.output_tokens.is_some() || usage.total_tokens.is_some())
        .then_some(usage)
}

fn pi_context_usage(stats: &Value) -> Option<AgentContextUsage> {
    let context = stats
        .get("context")
        .or_else(|| stats.get("contextUsage"))
        .or_else(|| stats.get("context_usage"))?;
    let used_tokens = pi_u64_at(context, &["usedTokens", "used_tokens", "tokens", "used"]);
    let max_tokens = pi_u64_at(
        context,
        &["maxTokens", "max_tokens", "contextWindow", "context_window"],
    );
    let percent_used = pi_u64_at(context, &["percentUsed", "percent_used", "percent"])
        .map(|percent| percent.min(100) as u8)
        .or_else(|| match (used_tokens, max_tokens) {
            (Some(used), Some(max)) if max > 0 => Some(
                ((used as f64 / max as f64) * 100.0)
                    .round()
                    .clamp(0.0, 100.0) as u8,
            ),
            _ => None,
        });
    if used_tokens.is_none() && max_tokens.is_none() && percent_used.is_none() {
        return None;
    }
    Some(AgentContextUsage {
        used_tokens,
        max_tokens,
        percent_used,
    })
}

fn pi_string_at(value: &Value, keys: &[&str]) -> Option<String> {
    keys.iter().find_map(|key| {
        value
            .get(*key)
            .and_then(Value::as_str)
            .map(str::trim)
            .filter(|s| !s.is_empty())
            .map(str::to_string)
    })
}

fn pi_u64_at(value: &Value, keys: &[&str]) -> Option<u64> {
    keys.iter().find_map(|key| {
        let value = value.get(*key)?;
        value
            .as_u64()
            .or_else(|| value.as_i64().and_then(|n| u64::try_from(n).ok()))
            .or_else(|| {
                value
                    .as_f64()
                    .filter(|n| n.is_finite() && *n >= 0.0)
                    .map(|n| n.round() as u64)
            })
    })
}

// ---------------------------------------------------------------------------
// Unit tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use crate::event::Kind;

    fn mk() -> PiRpc {
        PiRpc::new()
    }

    fn cfg() -> SessionParams {
        SessionParams {
            model: "xai/grok-4".into(),
            ..Default::default()
        }
    }

    fn setup() -> PiRpc {
        let mut d = mk();
        d.init(&cfg());
        d
    }

    #[test]
    fn init_sends_get_state() {
        let mut d = mk();
        let frames = d.init(&cfg());
        // init returns the get_state frame directly (not via pending_out).
        assert_eq!(frames.len(), 1);
        let OutFrame::Stdin(ref bytes) = frames[0] else {
            panic!("expected Stdin frame");
        };
        let v: Value = serde_json::from_slice(bytes).unwrap();
        assert_eq!(v["type"], "get_state");
        assert!(v["id"].is_string());
        // pending should have the entry for response correlation
        assert_eq!(d.pending.len(), 1);
    }

    #[test]
    fn prompt_encode_user_message() {
        let mut d = setup();
        // simulate handshake completed
        d.state = SessionState::Idle;
        let input = Input {
            text: "hello".into(),
            ..Default::default()
        };
        let frames = d.encode_user_message(&input).unwrap();
        assert_eq!(frames.len(), 1);
        let OutFrame::Stdin(ref bytes) = frames[0] else {
            panic!("expected Stdin");
        };
        let v: Value = serde_json::from_slice(bytes).unwrap();
        assert_eq!(v["type"], "prompt");
        assert_eq!(v["message"], "hello");
    }

    #[test]
    fn follow_up_when_streaming() {
        let mut d = setup();
        d.state = SessionState::Streaming;
        let input = Input {
            text: "more work".into(),
            ..Default::default()
        };
        let frames = d.encode_user_message(&input).unwrap();
        let OutFrame::Stdin(ref bytes) = frames[0] else {
            panic!("expected Stdin");
        };
        let v: Value = serde_json::from_slice(bytes).unwrap();
        assert_eq!(v["type"], "follow_up");
    }

    #[test]
    fn interrupt_emits_abort() {
        let mut d = setup();
        let frames = d.encode_interrupt().unwrap();
        let OutFrame::Stdin(ref bytes) = frames[0] else {
            panic!("expected Stdin");
        };
        let v: Value = serde_json::from_slice(bytes).unwrap();
        assert_eq!(v["type"], "abort");
    }

    #[test]
    fn deferred_prompt_flushes_after_session_ready() {
        let mut d = mk();
        d.init(&cfg());
        // send a prompt while initializing
        d.encode_user_message(&Input {
            text: "hi".into(),
            ..Default::default()
        })
        .unwrap();
        assert_eq!(d.pending_prompts.len(), 1);
        // Simulate the get_state response
        let resp = json!({"id":"0","type":"response","command":"get_state","success":true,"data":{"sessionId":"s1","sessionFile":"/tmp/s.jsonl"}});
        let bytes = serde_json::to_vec(&resp).unwrap();
        let evs = d.translate(&bytes);
        assert!(evs.is_empty());
        // prompt should have been flushed
        assert!(d.pending_prompts.is_empty());
        let agent_start = json!({"type":"agent_start"});
        let evs = d.translate(&serde_json::to_vec(&agent_start).unwrap());
        assert!(evs.iter().any(|e| e.kind() == Kind::SessionStarted));
    }

    #[test]
    fn translate_agent_start_emits_session_started_once() {
        let mut d = setup();
        d.session_file = Some("/tmp/pi-sess.jsonl".into());
        d.state = SessionState::Idle;
        let line = json!({"type":"agent_start"});
        let bytes = serde_json::to_vec(&line).unwrap();
        let evs = d.translate(&bytes);
        assert!(evs.iter().any(|e| e.kind() == Kind::SessionStarted));
        let evs = d.translate(&bytes);
        assert!(
            evs.is_empty(),
            "agent_start should only emit SessionStarted once"
        );
    }

    #[test]
    fn session_started_uses_stable_pi_session_id_not_session_file_path() {
        let mut d = setup();
        d.session_id = Some("019e1a99-5f61-774c-af31-f5e856f22f86".into());
        d.session_file = Some("/Users/era/.pi/agent/sessions/project/2026-05-12T05-11-59-586Z_019e1a99-5f61-774c-af31-f5e856f22f86.jsonl".into());
        d.state = SessionState::Idle;

        let evs = d.translate(&serde_json::to_vec(&json!({"type":"agent_start"})).unwrap());
        let session_id = evs
            .iter()
            .find_map(|event| match &event.payload {
                Payload::SessionStarted(started) => Some(started.session_id.as_str()),
                _ => None,
            })
            .expect("session started");

        assert_eq!(session_id, "019e1a99-5f61-774c-af31-f5e856f22f86");
    }

    #[test]
    fn pi_session_ref_falls_back_to_uuid_suffix_from_session_file() {
        assert_eq!(
            pi_session_ref(
                None,
                Some("/tmp/2026-05-12T05-11-59-586Z_019e1a99-5f61-774c-af31-f5e856f22f86.jsonl"),
            )
            .as_deref(),
            Some("019e1a99-5f61-774c-af31-f5e856f22f86")
        );
    }

    #[test]
    fn translate_turn_start() {
        let mut d = setup();
        d.state = SessionState::Idle;
        let line = json!({"type":"turn_start"});
        let bytes = serde_json::to_vec(&line).unwrap();
        let evs = d.translate(&bytes);
        assert_eq!(d.state, SessionState::Streaming);
        assert!(d.turn_active);
        assert!(evs.iter().any(|e| e.kind() == Kind::TurnStarted));
    }

    #[test]
    fn translate_turn_end_basic() {
        let mut d = setup();
        d.state = SessionState::Streaming;
        let line = json!({"type":"turn_end","message":{"role":"assistant","content":[{"type":"text","text":"Done"}],"usage":{"input":10,"output":5}}});
        let bytes = serde_json::to_vec(&line).unwrap();
        let evs = d.translate(&bytes);
        assert_eq!(d.state, SessionState::Idle);
        assert!(evs.iter().any(|e| e.kind() == Kind::TurnCompleted));
    }

    #[test]
    fn error_turn_end_fails_turn_instead_of_completing_without_text() {
        let mut d = setup();
        d.state = SessionState::Streaming;
        d.turn_active = true;
        let line = json!({"type":"turn_end","message":{"role":"assistant","content":[],"stopReason":"error","errorMessage":"fetch failed"},"toolResults":[]});
        let evs = d.translate(&serde_json::to_vec(&line).unwrap());

        assert_eq!(d.state, SessionState::Idle);
        assert!(
            evs.iter()
                .any(|e| matches!(e.payload, Payload::TurnFailed(_))),
            "error turn_end should surface as TurnFailed"
        );
        assert!(
            evs.iter().all(|e| e.kind() != Kind::TurnCompleted),
            "error turn_end must not look like a successful completion"
        );
        let error = evs.iter().find_map(|e| match &e.payload {
            Payload::TurnFailed(failed) => Some(failed.error.as_str()),
            _ => None,
        });
        assert_eq!(error, Some("fetch failed"));
    }

    #[test]
    fn tool_use_turn_end_does_not_complete_turn() {
        let mut d = setup();
        d.state = SessionState::Streaming;
        d.turn_active = true;

        let intermediate = json!({"type":"turn_end","message":{"role":"assistant","stopReason":"toolUse","content":[{"type":"thinking","thinking":"Need date."},{"type":"toolCall","id":"call_1","name":"bash","arguments":{"command":"date"}}]}});
        let evs = d.translate(&serde_json::to_vec(&intermediate).unwrap());
        assert!(
            evs.iter().all(|event| event.kind() != Kind::TurnCompleted),
            "tool-use handoff without a final assistant message must not complete the turn"
        );
        assert_eq!(d.state, SessionState::Streaming);
        assert!(d.turn_active);

        let final_turn = json!({"type":"turn_end","message":{"role":"assistant","content":[{"type":"text","text":"下午 1:17，2026年5月12日周二。"}],"usage":{"input":10,"output":5}}});
        let evs = d.translate(&serde_json::to_vec(&final_turn).unwrap());
        assert_eq!(d.state, SessionState::Idle);
        assert!(evs.iter().any(|event| event.kind() == Kind::TurnCompleted));
    }

    #[test]
    fn thinking_deltas_flush_as_one_reasoning_event_at_text_boundary() {
        let mut d = setup();
        d.state = SessionState::Streaming;
        d.turn_active = true;

        let first = json!({"type":"message_update","assistantMessageEvent":{"type":"thinking_delta","delta":"da"}});
        let second = json!({"type":"message_update","assistantMessageEvent":{"type":"thinking_delta","delta":"ta"}});
        assert!(
            d.translate(&serde_json::to_vec(&first).unwrap()).is_empty(),
            "Pi thinking deltas are token fragments and must not be public reasoning events"
        );
        assert!(
            d.translate(&serde_json::to_vec(&second).unwrap())
                .is_empty(),
            "Pi thinking deltas should be buffered until a semantic boundary"
        );

        let text = json!({"type":"message_update","assistantMessageEvent":{"type":"text_delta","delta":"Done"}});
        let evs = d.translate(&serde_json::to_vec(&text).unwrap());
        let reasoning = evs
            .iter()
            .filter_map(|ev| match &ev.payload {
                Payload::Timeline(timeline) => timeline.item.reasoning.as_ref(),
                _ => None,
            })
            .map(|reasoning| reasoning.text.as_str())
            .collect::<Vec<_>>();
        let assistant = evs
            .iter()
            .filter_map(|ev| match &ev.payload {
                Payload::Timeline(timeline) => timeline.item.assistant_message.as_ref(),
                _ => None,
            })
            .map(|message| message.text.as_str())
            .collect::<Vec<_>>();

        assert_eq!(reasoning, ["data"]);
        assert_eq!(assistant, ["Done"]);
    }

    #[test]
    fn thinking_deltas_flush_before_tool_call_boundary() {
        let mut d = setup();
        d.state = SessionState::Streaming;
        d.turn_active = true;

        for delta in ["Need ", "date"] {
            let line = json!({"type":"message_update","assistantMessageEvent":{"type":"thinking_delta","delta":delta}});
            assert!(d.translate(&serde_json::to_vec(&line).unwrap()).is_empty());
        }

        let start = json!({"type":"tool_execution_start","toolCallId":"t1","toolName":"bash","args":{"command":"date"}});
        let evs = d.translate(&serde_json::to_vec(&start).unwrap());
        let reasoning = evs.iter().find_map(|ev| match &ev.payload {
            Payload::Timeline(timeline) => timeline.item.reasoning.as_ref(),
            _ => None,
        });
        let tool_call = evs.iter().find_map(|ev| match &ev.payload {
            Payload::Timeline(timeline) => timeline.item.tool_call.as_ref(),
            _ => None,
        });

        assert_eq!(reasoning.map(|r| r.text.as_str()), Some("Need date"));
        assert!(tool_call.is_some(), "tool call should still be emitted");
    }

    #[test]
    fn translate_tool_execution() {
        let mut d = setup();
        let start = json!({"type":"tool_execution_start","toolCallId":"t1","toolName":"read","args":{"path":"/tmp/x"}});
        let evs = d.translate(&serde_json::to_vec(&start).unwrap());
        assert_eq!(evs.len(), 1);
        assert_eq!(evs[0].kind(), Kind::Timeline);

        let end = json!({"type":"tool_execution_end","toolCallId":"t1","result":"contents","isError":false});
        let evs2 = d.translate(&serde_json::to_vec(&end).unwrap());
        assert_eq!(evs2.len(), 1);
        assert_eq!(evs2[0].kind(), Kind::Timeline);
    }

    #[test]
    fn tool_execution_end_with_image_content_emits_attachment() {
        let mut d = setup();
        let png = "iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAYAAAAfFcSJ";
        let end = json!({
            "type": "tool_execution_end",
            "toolCallId": "call_img",
            "toolName": "codex_generate_image",
            "result": {
                "content": [
                    {"type": "text", "text": "Saved image to: /tmp/ig_abc.png"},
                    {"type": "image", "data": png, "mimeType": "image/png"}
                ],
                "details": {
                    "imageGenerationId": "ig_abc",
                    "savedPath": "/tmp/ig_abc.png",
                    "outputFormat": "png"
                }
            },
            "isError": false
        });

        let evs = d.translate(&serde_json::to_vec(&end).unwrap());

        let tool_output = evs
            .iter()
            .find_map(|ev| match &ev.payload {
                Payload::Timeline(timeline) => timeline
                    .item
                    .tool_result
                    .as_ref()
                    .map(|result| result.result.output.as_str()),
                _ => None,
            })
            .expect("tool result text");
        assert!(tool_output.contains("Saved image to"));
        assert!(!tool_output.contains(png));
        assert!(!tool_output.contains("\"type\":\"image\""));

        let attachment = evs
            .iter()
            .find_map(|ev| match &ev.payload {
                Payload::Attachment(attachment) => Some(attachment),
                _ => None,
            })
            .expect("image attachment");
        assert_eq!(attachment.id.as_str(), "ig_abc");
        assert_eq!(attachment.filename.as_str(), "ig_abc.png");
        assert_eq!(attachment.media_type.as_str(), "image/png");
        assert_eq!(attachment.data_base64, png);
        assert_eq!(attachment.caption, None);
    }

    #[test]
    fn apply_patch_tool_passthrough_keeps_name_and_input() {
        let mut d = setup();
        let start = json!({
            "type": "tool_execution_start",
            "toolCallId": "t1",
            "toolName": "apply_patch",
            "args": {"patch": "*** Begin Patch\n*** End Patch"}
        });

        let evs = d.translate(&serde_json::to_vec(&start).unwrap());
        let tool_call = evs
            .iter()
            .find_map(|ev| match &ev.payload {
                crate::event::Payload::Timeline(timeline) => {
                    timeline.item.tool_call.as_ref().map(|item| &item.call)
                }
                _ => None,
            })
            .expect("tool call");

        assert_eq!(tool_call.name.as_str(), "apply_patch");
        assert_eq!(
            tool_call.input.get("patch").and_then(|v| v.as_str()),
            Some("*** Begin Patch\n*** End Patch")
        );
    }

    #[test]
    fn on_exit_resume_handle() {
        let mut d = setup();
        d.session_file = Some("/tmp/pi.jsonl".into());
        let evs = d.on_exit(0, None);
        assert_eq!(evs.len(), 1);
        if let Payload::SessionClosed(sc) = &evs[0].payload {
            let rh = sc.resume.as_ref().expect("resume handle");
            assert_eq!(rh.data.get("session_path").unwrap(), "/tmp/pi.jsonl");
        } else {
            panic!("expected SessionClosed");
        }
    }

    #[test]
    fn handle_response_get_state_initial() {
        let mut d = mk();
        d.init(&cfg());
        let resp = json!({"id":"0","type":"response","command":"get_state","success":true,"data":{"sessionId":"s1","sessionFile":"/tmp/p.jsonl"}});
        let bytes = serde_json::to_vec(&resp).unwrap();
        let evs = d.translate(&bytes);
        assert_eq!(d.state, SessionState::Idle);
        assert_eq!(d.session_file.as_deref(), Some("/tmp/p.jsonl"));
        assert!(evs.is_empty());
        let agent_start = json!({"type":"agent_start"});
        let evs = d.translate(&serde_json::to_vec(&agent_start).unwrap());
        assert!(evs.iter().any(|e| e.kind() == Kind::SessionStarted));
    }

    #[test]
    fn handle_response_get_available_models() {
        let mut d = setup();
        d.pending
            .insert("1".into(), PendingKind::GetAvailableModels);
        let resp = json!({"id":"1","type":"response","command":"get_available_models","success":true,"data":{"models":[{"provider":"openai","id":"gpt-5","supportsReasoning":false}]}});
        let bytes = serde_json::to_vec(&resp).unwrap();
        let evs = d.translate(&bytes);
        // Now emits CommandResult::Models events
        assert!(!evs.is_empty());
        assert!(evs.iter().any(|e| e.kind() == Kind::CommandResult));
        assert_eq!(d.model_catalog.models.len(), 1);
        assert_eq!(d.model_catalog.models[0].id.as_str(), "openai/gpt-5");
    }

    #[test]
    fn permission_request_round_trip() {
        let mut d = setup();
        // Pi sends extension_ui_request for confirm
        let line = json!({"type":"extension_ui_request","id":"ext-1","method":"confirm","message":"Allow bash?"});
        let bytes = serde_json::to_vec(&line).unwrap();
        let evs = d.translate(&bytes);
        assert_eq!(evs.len(), 1);
        let req_id = if let Payload::PermissionRequest(pr) = &evs[0].payload {
            assert!(pr
                .questions
                .iter()
                .any(|q| q.question.contains("Allow bash")));
            pr.req_id.clone()
        } else {
            panic!("expected PermissionRequest");
        };

        // Answer Allow
        let frames = d
            .encode_permission_response(
                &req_id,
                &PermissionResponse::from_decision(Decision::Allow),
            )
            .unwrap();
        let OutFrame::Stdin(ref bytes) = frames[0] else {
            panic!("expected Stdin");
        };
        let v: Value = serde_json::from_slice(bytes).unwrap();
        assert_eq!(v["type"], "extension_ui_response");
        assert_eq!(v["id"], "ext-1");
        assert_eq!(v["confirmed"], true);
    }

    #[test]
    fn select_permission_response_uses_question_id() {
        let mut d = setup();
        let line = json!({
            "type":"extension_ui_request",
            "id":"ext-1",
            "method":"select",
            "message":"Pick one",
            "args":{"questionId":"choice","options":[{"label":"A"},{"label":"B"}]}
        });
        let evs = d.translate(&serde_json::to_vec(&line).unwrap());
        let req_id = if let Payload::PermissionRequest(pr) = &evs[0].payload {
            assert_eq!(pr.questions[0].id, "choice");
            pr.req_id.clone()
        } else {
            panic!("expected PermissionRequest");
        };

        let mut answers = BTreeMap::new();
        answers.insert(
            "wrong".into(),
            event::PermissionAnswer {
                answers: vec!["wrong-value".into()],
                ..Default::default()
            },
        );
        answers.insert(
            "choice".into(),
            event::PermissionAnswer {
                answers: vec!["B".into()],
                ..Default::default()
            },
        );
        let frames = d
            .encode_permission_response(
                &req_id,
                &PermissionResponse {
                    decision: Decision::Allow,
                    answers,
                },
            )
            .unwrap();
        let OutFrame::Stdin(ref bytes) = frames[0] else {
            panic!("expected Stdin");
        };
        let v: Value = serde_json::from_slice(bytes).unwrap();
        assert_eq!(v["value"], "B");
    }

    #[test]
    fn set_status_extension_request_is_cancelled_without_permission_state() {
        let mut d = setup();
        let line = json!({
            "type":"extension_ui_request",
            "id":"status-1",
            "method":"setStatus",
            "statusKey":"sub-bar",
            "statusText":""
        });

        let evs = d.translate(&serde_json::to_vec(&line).unwrap());

        assert!(evs.is_empty());
        assert!(d.extension_requests.is_empty());
        let frames = d.drain_out_frames();
        assert_eq!(frames.len(), 1);
        let OutFrame::Stdin(ref bytes) = frames[0] else {
            panic!("expected Stdin");
        };
        let v: Value = serde_json::from_slice(bytes).unwrap();
        assert_eq!(v["type"], "extension_ui_response");
        assert_eq!(v["id"], "status-1");
        assert_eq!(v["cancelled"], true);
    }

    #[test]
    fn set_model_suffix_chains_thinking_level() {
        let mut d = setup();
        let dispatch = d.set_model("xai/grok-4:high", None).unwrap();
        let CommandDispatch::Deferred(frames) = dispatch else {
            panic!("expected deferred dispatch");
        };
        let OutFrame::Stdin(ref bytes) = frames[0] else {
            panic!("expected Stdin");
        };
        let set_model: Value = serde_json::from_slice(bytes).unwrap();
        assert_eq!(set_model["type"], "set_model");
        assert_eq!(set_model["modelId"], "grok-4");
        assert_eq!(d.cfg.model, "xai/grok-4");

        let resp = json!({"id":set_model["id"],"type":"response","command":"set_model","success":true,"data":{}});
        let evs = d.translate(&serde_json::to_vec(&resp).unwrap());
        assert!(evs.is_empty());
        let frames = d.drain_out_frames();
        let OutFrame::Stdin(ref bytes) = frames[0] else {
            panic!("expected Stdin");
        };
        let set_thinking: Value = serde_json::from_slice(bytes).unwrap();
        assert_eq!(set_thinking["type"], "set_thinking_level");
        assert_eq!(set_thinking["level"], "high");

        let resp = json!({"id":set_thinking["id"],"type":"response","command":"set_thinking_level","success":true,"data":{}});
        let evs = d.translate(&serde_json::to_vec(&resp).unwrap());
        assert!(evs.iter().any(|e| e.kind() == Kind::CommandResult));
    }

    #[test]
    fn system_skills_dispatches_to_list_skills() {
        let mut d = setup();
        let dispatch = d
            .handle_system_command(&AgentCommandInvocation {
                name: "skills".into(),
                args: None,
                values: Value::Null,
                source: AgentCommandSource::AdapterMapped,
            })
            .unwrap();
        let CommandDispatch::Deferred(frames) = dispatch else {
            panic!("expected deferred dispatch");
        };
        let OutFrame::Stdin(ref bytes) = frames[0] else {
            panic!("expected Stdin");
        };
        let v: Value = serde_json::from_slice(bytes).unwrap();
        assert_eq!(v["type"], "get_commands");
    }

    #[test]
    fn basic_flow() {
        let mut d = mk();
        d.init(&cfg());
        // Initial get_state response
        let resp = json!({"id":"0","type":"response","command":"get_state","success":true,"data":{"sessionId":"s1","sessionFile":"/tmp/pi-sess.jsonl"}});
        let evs = d.translate(&serde_json::to_vec(&resp).unwrap());
        assert!(evs.is_empty());
        assert_eq!(d.state, SessionState::Idle);

        let lines = vec![
            json!({"type":"agent_start"}),
            json!({"type":"message_update","assistantMessageEvent":{"type":"thinking_delta","delta":"Thinking "}}),
            json!({"type":"message_update","assistantMessageEvent":{"type":"text_delta","delta":"Hello "}}),
            json!({"type":"tool_execution_start","toolCallId":"t1","toolName":"read","args":{"path":"/tmp/x"}}),
            json!({"type":"tool_execution_end","toolCallId":"t1","result":"contents"}),
            json!({"type":"turn_end","message":{"role":"assistant","content":[{"type":"text","text":"Hello from Pi"}],"usage":{"input":10,"output":5,"cacheRead":2,"cost":{"total":0.001}}}}),
        ];
        let mut evs = Vec::new();
        for line in &lines {
            evs.extend(d.translate(&serde_json::to_vec(line).unwrap()));
        }

        let kinds: Vec<_> = evs.iter().map(|e| e.kind()).collect();
        assert!(kinds.contains(&Kind::SessionStarted));
        assert!(kinds.contains(&Kind::TurnCompleted));
        let timeline_count = kinds.iter().filter(|k| **k == Kind::Timeline).count();
        assert_eq!(timeline_count, 5); // thinking, text, tool_call, tool_result, turn_end assistant
    }
}
