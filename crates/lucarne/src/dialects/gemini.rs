//! Gemini ACP dialect — JSON-RPC state machine over stdio.
//!
//! Wire flow:
//!
//! ```text
//! us  -> initialize                            (id=1)
//! agt -> result {protocolVersion, agentCapabilities.loadSession}
//! us  -> session/new | session/load            (id=2)
//! agt -> result {sessionId, modes, models}
//! us  -> session/prompt                        (id=3)
//! agt -> session/update notifications*
//! agt -> session/request_permission            (server-initiated, id=N)
//! us  -> response to id=N { outcome: { outcome:selected, optionId:"..." }}
//! agt -> result {stopReason, usage}            (for id=3)
//! ```
//!
//! Cancellation: `session/cancel` request. When the agent returns
//! `stopReason=cancelled`, we emit `TurnFailed{code="cancelled"}`.

use crate::{
    agent_runtime::{
        AgentCommand, AgentCommandCatalog, AgentCommandCompletion, AgentCommandInput,
        AgentCommandInvocation, AgentCommandSource, AgentModelCatalog, AgentModelOption,
        AgentPermissionCatalog, AgentPermissionOption, AgentSkillCatalog, AgentStatus,
    },
    dialect::{
        command_result_events, model_args, normalize_agent_command_name, permission_arg,
        CommandDispatch, CommandResult, Conversation, Dialect, Input, ModelSelection, OutFrame,
        PermissionMode, PermissionSelection, SessionParams,
    },
    error::{LucarneError, Result},
    event::{
        self, Event, LogLine, Payload, PermissionRequest, PermissionResponse, ResumeHandle, Risk,
        SessionClosed, SessionStarted, Timeline, TimelineItem, ToolCall, ToolResult, TurnCompleted,
        TurnFailed, Usage, UsageDelta,
    },
};
use serde_json::{json, Map, Value};
use std::collections::{BTreeMap, HashMap, HashSet};

const ACP_PROTOCOL_VERSION: i64 = 1;

const M_INIT: &str = "initialize";
const M_NEW: &str = "session/new";
const M_LOAD: &str = "session/load";
const M_PROMPT: &str = "session/prompt";
const M_CANCEL: &str = "session/cancel";
const M_SET_MODE: &str = "session/set_mode";
const M_SET_MODEL: &str = "session/set_model";
const M_UPDATE: &str = "session/update";
const M_PERMISSION: &str = "session/request_permission";
#[derive(Clone, Debug)]
struct PermissionOption {
    option_id: String,
    kind: String,
    name: String,
}

#[derive(Clone, Debug)]

struct ServerApproval {
    rpc_id: Value,
    options: Vec<PermissionOption>,
}

#[derive(Clone, Debug, Default)]
struct GeminiModelInfo {
    id: String,
    name: String,
    description: String,
}

#[derive(Clone, Debug, Default)]
struct GeminiModeInfo {
    id: String,
    name: String,
    description: String,
}

pub struct Gemini {
    cfg: SessionParams,
    seq_id: i64,
    session_id: String,
    resume_session_id: String,
    session_ready: bool,
    pending: HashMap<i64, String>,
    pending_out: Vec<OutFrame>,
    server_approvals: HashMap<String, ServerApproval>,
    seen_tool_calls: HashSet<String>,
    seen_tool_results: HashSet<String>,
    load_session_supported: bool,
    resume_fallback_queued: bool,
    pending_prompt: Option<Input>,
    session_failed: bool,
    turn_has_non_message_activity: bool,
    pending_assistant_text: String,
    command_catalog: AgentCommandCatalog,
    current_model: String,
    models: Vec<GeminiModelInfo>,
    current_mode: String,
    modes: Vec<GeminiModeInfo>,
}

impl Gemini {
    fn list_commands(&mut self) -> Result<CommandDispatch> {
        Ok(CommandDispatch::ready(CommandResult::Commands(
            self.command_catalog.clone(),
        )))
    }

    fn list_models(&mut self) -> Result<CommandDispatch> {
        Ok(CommandDispatch::ready(CommandResult::Models(
            gemini_model_catalog(&self.current_model, &self.models),
        )))
    }

    fn set_model(&mut self, model: &str, reasoning: Option<&str>) -> Result<CommandDispatch> {
        if let Some(reasoning) = reasoning {
            return Err(LucarneError::dialect(format!(
                "gemini: reasoning effort {reasoning:?} is not configurable through ACP session/set_model"
            )));
        }
        let model = model.to_string();
        if model.trim().is_empty() {
            return Err(LucarneError::dialect("gemini: /model requires a model id"));
        }
        if !self.models.is_empty() && !self.models.iter().any(|item| item.id == model) {
            return Err(LucarneError::dialect(format!(
                "gemini: unknown model {model:?}"
            )));
        }
        if self.session_id.is_empty() {
            return Err(LucarneError::dialect("gemini: session is not ready"));
        }
        self.queue_request_with_pending(
            M_SET_MODEL,
            json!({"sessionId": self.session_id, "modelId": model}),
            format!("{M_SET_MODEL}|command|{model}"),
        )?;
        Ok(CommandDispatch::deferred(std::mem::take(
            &mut self.pending_out,
        )))
    }

    fn list_permissions(&mut self) -> Result<CommandDispatch> {
        Ok(CommandDispatch::ready(CommandResult::Permissions(
            gemini_permission_catalog(&self.current_mode, &self.modes),
        )))
    }

    fn list_skills(&mut self) -> Result<CommandDispatch> {
        Ok(CommandDispatch::ready(CommandResult::Skills(
            AgentSkillCatalog::default(),
        )))
    }

    fn status(&mut self) -> Result<CommandDispatch> {
        Ok(CommandDispatch::ready(CommandResult::Status(AgentStatus {
            session_id: (!self.session_id.trim().is_empty())
                .then(|| self.session_id.as_str().into()),
            directory: (!self.cfg.cwd.trim().is_empty()).then(|| self.cfg.cwd.as_str().into()),
            model: (!self.current_model.trim().is_empty())
                .then(|| self.current_model.as_str().into()),
            permissions: (!self.current_mode.trim().is_empty())
                .then(|| self.current_mode.as_str().into()),
            ..Default::default()
        })))
    }

    fn set_permissions(&mut self, mode: &str) -> Result<CommandDispatch> {
        let mode = mode.to_string();
        if mode.trim().is_empty() {
            return Err(LucarneError::dialect(
                "gemini: /permissions requires a mode id",
            ));
        }
        if !self.modes.is_empty() && !self.modes.iter().any(|item| item.id == mode) {
            return Err(LucarneError::dialect(format!(
                "gemini: unknown permission mode {mode:?}"
            )));
        }
        if self.session_id.is_empty() {
            return Err(LucarneError::dialect("gemini: session is not ready"));
        }
        self.queue_request_with_pending(
            M_SET_MODE,
            json!({"sessionId": self.session_id, "modeId": mode}),
            format!("{M_SET_MODE}|command|{mode}"),
        )?;
        Ok(CommandDispatch::deferred(std::mem::take(
            &mut self.pending_out,
        )))
    }

    fn new_conversation_command(&mut self) -> Result<CommandDispatch> {
        self.pending_prompt = None;
        self.session_failed = false;
        self.session_ready = false;
        self.reset_turn_state();
        Ok(CommandDispatch::ready(CommandResult::NewConversation(
            Conversation { session_ref: None },
        )))
    }

    fn quit(&mut self) -> Result<CommandDispatch> {
        Ok(CommandDispatch::ready(CommandResult::Quit))
    }
}

impl Gemini {
    pub fn new() -> Self {
        tracing::debug!(
            target: "lucarne::dialects::gemini",
            "gemini dialect created"
        );
        Self {
            cfg: SessionParams::default(),
            seq_id: 0,
            session_id: String::new(),
            resume_session_id: String::new(),
            session_ready: false,
            pending: HashMap::new(),
            pending_out: Vec::new(),
            server_approvals: HashMap::new(),
            seen_tool_calls: HashSet::new(),
            seen_tool_results: HashSet::new(),
            load_session_supported: false,
            resume_fallback_queued: false,
            pending_prompt: None,
            session_failed: false,
            turn_has_non_message_activity: false,
            pending_assistant_text: String::new(),
            command_catalog: AgentCommandCatalog::default(),
            current_model: String::new(),
            models: Vec::new(),
            current_mode: String::new(),
            modes: Vec::new(),
        }
    }

    fn next_id(&mut self) -> i64 {
        self.seq_id += 1;
        self.seq_id
    }

    fn queue_request(&mut self, method: &str, params: Value) -> Result<()> {
        self.queue_request_with_pending(method, params, method)
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
}

impl Default for Gemini {
    fn default() -> Self {
        Self::new()
    }
}

impl Dialect for Gemini {
    fn name(&self) -> &'static str {
        "gemini"
    }

    fn init(&mut self, cfg: &SessionParams) -> Vec<OutFrame> {
        self.cfg = cfg.clone();
        tracing::debug!(
            target: "lucarne::dialects::gemini",
            has_resume_data = cfg.resume_data().is_some(),
            "gemini dialect initializing"
        );
        self.resume_session_id = cfg
            .resume_data()
            .and_then(|d| d.get("session_id"))
            .and_then(|v| v.as_str())
            .unwrap_or("")
            .to_string();
        let params = json!({
            "protocolVersion": ACP_PROTOCOL_VERSION,
            "clientInfo": {"name": "lucarne", "title": "lucarne", "version": "0.1.0"},
            "clientCapabilities": {
                "auth": {"terminal": false},
                "fs": {"readTextFile": false, "writeTextFile": false},
                "terminal": false,
            },
        });
        if self.queue_request(M_INIT, params).is_err() {
            return Vec::new();
        }
        std::mem::take(&mut self.pending_out)
    }

    fn drain_out_frames(&mut self) -> Vec<OutFrame> {
        std::mem::take(&mut self.pending_out)
    }

    fn encode_user_message(&mut self, input: &Input) -> Result<Vec<OutFrame>> {
        self.reset_turn_state();
        if self.session_failed {
            return Err(LucarneError::dialect(
                "gemini: session initialization failed",
            ));
        }
        if !self.session_ready || self.session_id.is_empty() {
            // Defer until session becomes ready.
            self.pending_prompt = Some(input.clone());
            return Ok(Vec::new());
        }
        let params = prompt_params(&self.session_id, input);
        self.queue_request(M_PROMPT, params)?;
        Ok(std::mem::take(&mut self.pending_out))
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
            "new" => self.new_conversation_command(),
            "quit" => self.quit(),
            "list_commands" => self.list_commands(),
            name => Err(LucarneError::dialect(format!(
                "gemini: unsupported system command {name:?}"
            ))),
        }
    }

    fn handle_native_command(
        &mut self,
        command: &AgentCommandInvocation,
    ) -> Result<CommandDispatch> {
        let text = build_native_slash_command(&self.command_catalog, command, self.name())?;
        self.encode_user_message(&Input {
            text,
            images: Vec::new(),
        })
        .map(CommandDispatch::deferred)
    }

    fn encode_permission_response(
        &mut self,
        req_id: &str,
        resp: &PermissionResponse,
    ) -> Result<Vec<OutFrame>> {
        if !resp.answers.is_empty() {
            return Err(LucarneError::dialect(
                "gemini: structured permission answers are not supported by ACP",
            ));
        }
        let approval = self.server_approvals.remove(req_id).ok_or_else(|| {
            LucarneError::dialect(format!("gemini: unknown permission request {:?}", req_id))
        })?;
        let outcome = match choose_permission_option_id(&approval.options, resp.decision) {
            Some(opt) => json!({"outcome": "selected", "optionId": opt}),
            None => json!({"outcome": "cancelled"}),
        };
        let rpc = json!({
            "jsonrpc": "2.0",
            "id": approval.rpc_id,
            "result": {"outcome": outcome},
        });
        let mut bytes = serde_json::to_vec(&rpc)?;
        bytes.push(b'\n');
        Ok(vec![OutFrame::rpc_response(bytes)])
    }

    fn encode_interrupt(&mut self) -> Result<Vec<OutFrame>> {
        if self.session_id.is_empty() {
            return Ok(vec![OutFrame::signal("SIGINT")]);
        }
        self.queue_request(M_CANCEL, json!({"sessionId": self.session_id}))?;
        Ok(std::mem::take(&mut self.pending_out))
    }

    fn translate(&mut self, raw: &[u8]) -> Vec<Event> {
        let Ok(v) = serde_json::from_slice::<Value>(raw) else {
            return vec![log_warn(format!(
                "gemini: invalid json-rpc frame: {:?}",
                String::from_utf8_lossy(raw)
            ))];
        };
        let obj = match v.as_object() {
            Some(o) => o,
            None => return Vec::new(),
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
        let mut payload = SessionClosed {
            reason: String::new(),
            resume: None,
        };
        if !self.session_id.is_empty() {
            let mut data: BTreeMap<String, Value> = BTreeMap::new();
            data.insert("session_id".into(), Value::String(self.session_id.clone()));
            if !self.cfg.cwd.is_empty() {
                data.insert("cwd".into(), Value::String(self.cfg.cwd.clone()));
            }
            payload.resume = Some(ResumeHandle { version: 1, data });
        }
        if exit_code != 0 || err.is_some() {
            let mut msg = format!("gemini exited with code {}", exit_code);
            if let Some(e) = err {
                msg.push_str(": ");
                msg.push_str(&e);
            }
            payload.reason = msg;
        }
        vec![Event::new(Payload::SessionClosed(payload))]
    }
}

// ——— response / notification / server-request handlers ———

impl Gemini {
    fn handle_response(&mut self, obj: &Map<String, Value>) -> Vec<Event> {
        let id = match obj.get("id").and_then(|v| v.as_i64()) {
            Some(i) => i,
            None => return Vec::new(),
        };
        let method = match self.pending.remove(&id) {
            Some(m) => m,
            None => return Vec::new(),
        };
        if let Some(err) = obj.get("error") {
            return self.handle_response_error(&method, err);
        }
        let result = obj.get("result").cloned().unwrap_or(Value::Null);
        match method.as_str() {
            M_INIT => self.handle_init_response(&result),
            M_NEW => self.handle_session_response(&result, ""),
            M_LOAD => {
                let fallback = self.resume_session_id.clone();
                self.handle_session_response(&result, &fallback)
            }
            M_PROMPT => self.handle_prompt_response(&result),
            M_CANCEL => self.handle_cancel_response(&result),
            method if method.starts_with(&format!("{M_SET_MODEL}|command|")) => {
                let model = method.rsplit('|').next().unwrap_or("").to_string();
                self.current_model = model.clone();
                command_result_events(
                    "gemini-command-model-set",
                    CommandResult::ModelChanged(ModelSelection {
                        model: model.into(),
                        reasoning: None,
                    }),
                )
            }
            method if method.starts_with(&format!("{M_SET_MODE}|command|")) => {
                let mode = method.rsplit('|').next().unwrap_or("").to_string();
                self.current_mode = mode.clone();
                command_result_events(
                    "gemini-command-permissions-set",
                    CommandResult::PermissionsChanged(PermissionSelection { mode: mode.into() }),
                )
            }
            M_SET_MODE | M_SET_MODEL => Vec::new(),
            _ => Vec::new(),
        }
    }

    fn handle_init_response(&mut self, result: &Value) -> Vec<Event> {
        self.load_session_supported = result
            .pointer("/agentCapabilities/loadSession")
            .and_then(|v| v.as_bool())
            .unwrap_or(false);
        let (method, mut params_map) =
            if !self.resume_session_id.is_empty() && self.load_session_supported {
                let mut m = Map::new();
                m.insert(
                    "sessionId".into(),
                    Value::String(self.resume_session_id.clone()),
                );
                (M_LOAD, m)
            } else {
                (M_NEW, Map::new())
            };
        params_map.insert(
            "cwd".into(),
            Value::String(if self.cfg.cwd.is_empty() {
                ".".into()
            } else {
                self.cfg.cwd.clone()
            }),
        );
        if let Err(e) = self.queue_request(method, Value::Object(params_map)) {
            return vec![log_warn(format!("gemini: queue {}: {}", method, e))];
        }
        Vec::new()
    }

    fn handle_session_response(&mut self, result: &Value, fallback: &str) -> Vec<Event> {
        let session_id = result
            .get("sessionId")
            .and_then(|v| v.as_str())
            .map(|s| s.trim().to_string())
            .filter(|s| !s.is_empty())
            .unwrap_or_else(|| fallback.trim().to_string());
        if session_id.is_empty() {
            return self
                .session_init_failed("gemini session initialization failed: missing sessionId");
        }
        self.session_id = session_id.clone();
        self.session_ready = true;

        let mut out = Vec::new();

        // Mode/model tuning — queue any set_mode/set_model to out_frames.
        if let Some(modes) = result.get("modes") {
            let current_mode = modes
                .get("currentModeId")
                .and_then(|v| v.as_str())
                .unwrap_or("")
                .trim()
                .to_string();
            self.current_mode = current_mode.clone();
            self.modes = parse_gemini_modes(modes);
            let available: Vec<String> = modes
                .get("availableModes")
                .and_then(|v| v.as_array())
                .map(|arr| {
                    arr.iter()
                        .filter_map(|m| {
                            m.get("id")
                                .and_then(|v| v.as_str())
                                .map(|s| s.trim().to_string())
                        })
                        .collect()
                })
                .unwrap_or_default();
            let want = desired_mode(&available, self.cfg.permission_mode);
            if !want.is_empty() && want != current_mode {
                let _ = self
                    .queue_request(M_SET_MODE, json!({"sessionId": session_id, "modeId": want}));
            }
        }
        let current_model = result
            .pointer("/models/currentModelId")
            .and_then(|v| v.as_str())
            .unwrap_or("")
            .trim()
            .to_string();
        self.current_model = current_model.clone();
        self.models = parse_gemini_models(result.get("models"));
        let desired_model = self.cfg.model.trim().to_string();
        if !desired_model.is_empty() && desired_model != current_model {
            let _ = self.queue_request(
                M_SET_MODEL,
                json!({"sessionId": session_id, "modelId": desired_model}),
            );
        }

        out.push(Event::new(Payload::SessionStarted(SessionStarted {
            session_id: session_id.clone(),
            model: if !desired_model.is_empty() {
                desired_model
            } else {
                current_model
            },
        })));

        // Queue any deferred prompt.
        if let Some(prompt) = self.pending_prompt.take() {
            if let Err(e) = self.queue_request(M_PROMPT, prompt_params(&session_id, &prompt)) {
                tracing::warn!("gemini: failed to send deferred prompt: {}", e);
            }
        }
        out
    }

    fn handle_prompt_response(&mut self, result: &Value) -> Vec<Event> {
        let stop_reason = gemini_stop_reason(result);
        if stop_reason == "cancelled" {
            return self.cancelled_turn_events(stop_reason);
        }
        let usage = parse_prompt_usage(result.get("usage"), result.get("_meta"));
        self.reset_turn_state();
        vec![Event::new(Payload::TurnCompleted(TurnCompleted {
            turn_id: String::new(),
            usage,
        }))]
    }

    fn handle_cancel_response(&mut self, result: &Value) -> Vec<Event> {
        let stop_reason = gemini_stop_reason(result);
        self.cancelled_turn_events(if stop_reason.is_empty() {
            "cancelled".into()
        } else {
            stop_reason
        })
    }

    fn cancelled_turn_events(&mut self, stop_reason: String) -> Vec<Event> {
        self.reset_turn_state();
        vec![Event::new(Payload::TurnFailed(TurnFailed {
            turn_id: String::new(),
            error: stop_reason.clone().into(),
            code: stop_reason.into(),
        }))]
    }

    fn handle_response_error(&mut self, method: &str, err: &Value) -> Vec<Event> {
        let message = err
            .get("message")
            .and_then(|v| v.as_str())
            .unwrap_or("gemini request failed")
            .to_string();
        match method {
            M_INIT | M_NEW => self.session_init_failed(&message),
            M_LOAD => {
                if !self.resume_session_id.is_empty() && !self.resume_fallback_queued {
                    self.resume_fallback_queued = true;
                    if let Err(e) = self.queue_request(
                        M_NEW,
                        json!({
                            "cwd": if self.cfg.cwd.is_empty() { ".".into() } else { self.cfg.cwd.clone() }
                        }),
                    ) {
                        return self
                            .session_init_failed(&format!("gemini: queue session/new fallback: {}", e));
                    }
                    return Vec::new();
                }
                self.session_init_failed(&message)
            }
            M_PROMPT => {
                vec![Event::new(Payload::TurnFailed(TurnFailed {
                    turn_id: String::new(),
                    error: message,
                    code: String::new(),
                }))]
            }
            _ => vec![log_warn(format!("gemini: {} failed: {}", method, message))],
        }
    }

    fn session_init_failed(&mut self, reason: &str) -> Vec<Event> {
        self.session_failed = true;
        vec![Event::new(Payload::SessionClosed(SessionClosed {
            reason: reason.into(),
            resume: None,
        }))]
    }

    fn handle_notification(&mut self, method: &str, obj: &Map<String, Value>) -> Vec<Event> {
        if method != M_UPDATE {
            return Vec::new();
        }
        let update = obj
            .get("params")
            .and_then(|p| p.get("update"))
            .cloned()
            .unwrap_or(Value::Null);
        self.translate_update(&update)
    }

    fn handle_server_request(&mut self, method: &str, obj: &Map<String, Value>) -> Vec<Event> {
        if method != M_PERMISSION {
            // Unknown server request — reply with -32601.
            let id = obj.get("id").cloned().unwrap_or(Value::Null);
            let rpc = json!({
                "jsonrpc": "2.0",
                "id": id,
                "error": {"code": -32601, "message": format!("method not found: {}", method)}
            });
            if let Ok(mut bytes) = serde_json::to_vec(&rpc) {
                bytes.push(b'\n');
                self.pending_out.push(OutFrame::rpc_response(bytes));
            }
            return vec![log_warn(format!(
                "gemini: unknown server request {}",
                method
            ))];
        }
        self.turn_has_non_message_activity = true;
        let id = obj.get("id").cloned().unwrap_or(Value::Null);
        let params = obj.get("params").cloned().unwrap_or(Value::Null);
        let tool_call = params.get("toolCall").cloned().unwrap_or(Value::Null);
        let options: Vec<PermissionOption> = params
            .get("options")
            .and_then(|v| v.as_array())
            .map(|arr| {
                arr.iter()
                    .map(|o| PermissionOption {
                        option_id: o
                            .get("optionId")
                            .and_then(|x| x.as_str())
                            .unwrap_or("")
                            .into(),
                        kind: o.get("kind").and_then(|x| x.as_str()).unwrap_or("").into(),
                        name: o.get("name").and_then(|x| x.as_str()).unwrap_or("").into(),
                    })
                    .collect()
            })
            .unwrap_or_default();
        let tool_call_id = tool_call
            .get("toolCallId")
            .and_then(|v| v.as_str())
            .unwrap_or("")
            .trim()
            .to_string();
        let req_id = if !tool_call_id.is_empty() {
            format!("gemini-{}", tool_call_id)
        } else if let Some(i) = id.as_i64() {
            format!("gemini-{}", i)
        } else {
            "gemini-permission".into()
        };
        let tool_title = tool_call
            .get("title")
            .and_then(|v| v.as_str())
            .unwrap_or("")
            .trim()
            .to_string();
        let tool_kind = tool_call
            .get("kind")
            .and_then(|v| v.as_str())
            .unwrap_or("")
            .trim()
            .to_string();
        let tool = if !tool_title.is_empty() {
            tool_title
        } else if !tool_kind.is_empty() {
            tool_kind.clone()
        } else {
            "permission".into()
        };
        self.server_approvals.insert(
            req_id.clone(),
            ServerApproval {
                rpc_id: id,
                options,
            },
        );
        vec![Event::new(Payload::PermissionRequest(PermissionRequest {
            req_id,
            tool,
            input: Some(project_tool_call_update(&tool_call)),
            risk: risk_for_tool_kind(&tool_kind),
            questions: Vec::new(),
        }))]
    }

    fn translate_update(&mut self, update: &Value) -> Vec<Event> {
        let tag = update
            .get("sessionUpdate")
            .and_then(|v| v.as_str())
            .unwrap_or("")
            .trim();
        match tag {
            "agent_message_chunk" => {
                let message_id = update
                    .get("messageId")
                    .and_then(|v| v.as_str())
                    .unwrap_or("")
                    .to_string();
                let text = content_block_text(update.get("content"));
                if text.is_empty() {
                    return Vec::new();
                }
                self.pending_assistant_text.push_str(&text);
                vec![Event::new(Payload::Timeline(Timeline {
                    item: event::new_timeline_assistant(&message_id, &text, true),
                }))]
            }
            "agent_thought_chunk" => {
                let message_id = update
                    .get("messageId")
                    .and_then(|v| v.as_str())
                    .unwrap_or("")
                    .to_string();
                let text = content_block_text(update.get("content"));
                if text.is_empty() {
                    return Vec::new();
                }
                vec![Event::new(Payload::Timeline(Timeline {
                    item: event::new_timeline_reasoning(&message_id, &text),
                }))]
            }
            "tool_call" => {
                self.turn_has_non_message_activity = true;
                self.translate_tool_call_full(update)
            }
            "tool_call_update" => {
                self.turn_has_non_message_activity = true;
                self.translate_tool_call_update(update)
            }
            "usage_update" => {
                let amount = update
                    .pointer("/cost/amount")
                    .and_then(|v| v.as_f64())
                    .unwrap_or(0.0);
                let currency = update
                    .pointer("/cost/currency")
                    .and_then(|v| v.as_str())
                    .unwrap_or("")
                    .to_ascii_uppercase();
                if amount == 0.0 || currency != "USD" {
                    return Vec::new();
                }
                vec![Event::new(Payload::UsageDelta(UsageDelta {
                    delta: Usage {
                        cost_usd: amount,
                        ..Default::default()
                    },
                }))]
            }
            "available_commands_update" => {
                self.command_catalog =
                    build_available_commands_catalog(update, self.command_catalog.revision + 1);
                Vec::new()
            }
            _ => Vec::new(),
        }
    }

    fn translate_tool_call_full(&mut self, v: &Value) -> Vec<Event> {
        let id = v
            .get("toolCallId")
            .and_then(|x| x.as_str())
            .unwrap_or("")
            .to_string();
        let status = v
            .get("status")
            .and_then(|x| x.as_str())
            .unwrap_or("")
            .to_string();
        let mut out = Vec::new();
        if !id.is_empty() && self.seen_tool_calls.insert(id.clone()) {
            out.push(tl(event::new_timeline_tool_call(&id, map_tool_call(v))));
        }
        if is_final_status(&status) && !id.is_empty() && self.seen_tool_results.insert(id.clone()) {
            out.push(tl(event::new_timeline_tool_result(
                "",
                &id,
                build_tool_result(&status, v),
            )));
        }
        out
    }

    fn translate_tool_call_update(&mut self, v: &Value) -> Vec<Event> {
        let id = v
            .get("toolCallId")
            .and_then(|x| x.as_str())
            .unwrap_or("")
            .to_string();
        let status = v
            .get("status")
            .and_then(|x| x.as_str())
            .unwrap_or("")
            .to_string();
        let title = v.get("title").and_then(|x| x.as_str()).unwrap_or("").trim();
        let kind = v.get("kind").and_then(|x| x.as_str()).unwrap_or("").trim();
        let mut out = Vec::new();
        if !id.is_empty()
            && !self.seen_tool_calls.contains(&id)
            && (!title.is_empty() || !kind.is_empty())
            && self.seen_tool_calls.insert(id.clone())
        {
            out.push(tl(event::new_timeline_tool_call(&id, map_tool_call(v))));
        }
        if is_final_status(&status) && !id.is_empty() && self.seen_tool_results.insert(id.clone()) {
            out.push(tl(event::new_timeline_tool_result(
                "",
                &id,
                build_tool_result(&status, v),
            )));
        }
        out
    }

    fn reset_turn_state(&mut self) {
        self.turn_has_non_message_activity = false;
        self.pending_assistant_text.clear();
    }
}

// ——— helpers ———

fn tl(item: TimelineItem) -> Event {
    Event::new(Payload::Timeline(Timeline { item }))
}

fn log_warn(text: String) -> Event {
    Event::new(Payload::Log(LogLine {
        level: "warn".into(),
        stream: "stdout".into(),
        text,
    }))
}

fn prompt_params(session_id: &str, input: &Input) -> Value {
    let mut blocks: Vec<Value> = Vec::new();
    let text = input.text.trim();
    if !text.is_empty() {
        blocks.push(json!({"type": "text", "text": text}));
    }
    for img in &input.images {
        if img.data.is_empty() || img.media_type.trim().is_empty() {
            continue;
        }
        use base64::Engine;
        let b64 = base64::engine::general_purpose::STANDARD.encode(&img.data);
        blocks.push(json!({
            "type": "image",
            "mimeType": img.media_type,
            "data": b64
        }));
    }
    json!({"sessionId": session_id, "prompt": blocks})
}

fn build_available_commands_catalog(update: &Value, revision: u64) -> AgentCommandCatalog {
    let mut out = Vec::new();
    if let Some(commands) = update.get("availableCommands").and_then(|v| v.as_array()) {
        for command in commands {
            let raw_name = command.get("name").and_then(|v| v.as_str()).unwrap_or("");
            let name = normalize_command_name(raw_name);
            if name.is_empty()
                || out
                    .iter()
                    .any(|cmd: &AgentCommand| cmd.name.as_str() == name)
            {
                continue;
            }
            let description = command
                .get("description")
                .and_then(|v| v.as_str())
                .map(|s| s.trim().to_string())
                .filter(|s| !s.is_empty())
                .map(Into::into);
            out.push(AgentCommand {
                name: name.into(),
                description,
                aliases: Vec::new(),
                source: AgentCommandSource::ProviderNative,
                input: AgentCommandInput::Text {
                    label: "arguments".into(),
                    required: false,
                },
                completion: AgentCommandCompletion::ProviderIdle,
            });
        }
    }
    AgentCommandCatalog {
        commands: out,
        complete: true,
        revision,
    }
}

fn parse_gemini_models(raw: Option<&Value>) -> Vec<GeminiModelInfo> {
    raw.and_then(|value| value.get("availableModels"))
        .and_then(Value::as_array)
        .map(|items| {
            items
                .iter()
                .filter_map(|item| {
                    let id = item
                        .get("modelId")
                        .or_else(|| item.get("id"))
                        .and_then(Value::as_str)
                        .map(str::trim)
                        .filter(|value| !value.is_empty())?;
                    let name = item
                        .get("name")
                        .and_then(Value::as_str)
                        .map(str::trim)
                        .filter(|value| !value.is_empty())
                        .unwrap_or(id);
                    let description = item
                        .get("description")
                        .and_then(Value::as_str)
                        .map(str::trim)
                        .unwrap_or("");
                    Some(GeminiModelInfo {
                        id: id.into(),
                        name: name.into(),
                        description: description.into(),
                    })
                })
                .collect()
        })
        .unwrap_or_default()
}

fn parse_gemini_modes(raw: &Value) -> Vec<GeminiModeInfo> {
    raw.get("availableModes")
        .and_then(Value::as_array)
        .map(|items| {
            items
                .iter()
                .filter_map(|item| {
                    let id = item
                        .get("id")
                        .and_then(Value::as_str)
                        .map(str::trim)
                        .filter(|value| !value.is_empty())?;
                    let name = item
                        .get("name")
                        .and_then(Value::as_str)
                        .map(str::trim)
                        .filter(|value| !value.is_empty())
                        .unwrap_or(id);
                    let description = item
                        .get("description")
                        .and_then(Value::as_str)
                        .map(str::trim)
                        .unwrap_or("");
                    Some(GeminiModeInfo {
                        id: id.into(),
                        name: name.into(),
                        description: description.into(),
                    })
                })
                .collect()
        })
        .unwrap_or_default()
}

fn gemini_model_catalog(current: &str, models: &[GeminiModelInfo]) -> AgentModelCatalog {
    AgentModelCatalog {
        current_model: (!current.trim().is_empty()).then(|| current.into()),
        current_reasoning: None,
        models: models
            .iter()
            .map(|model| AgentModelOption {
                id: model.id.as_str().into(),
                display_name: (!model.name.is_empty() && model.name != model.id)
                    .then(|| model.name.clone().into()),
                description: (!model.description.is_empty())
                    .then(|| model.description.clone().into()),
                supported_reasoning: Vec::new(),
            })
            .collect(),
    }
}

fn gemini_permission_catalog(current: &str, modes: &[GeminiModeInfo]) -> AgentPermissionCatalog {
    AgentPermissionCatalog {
        current_mode: (!current.trim().is_empty()).then(|| current.into()),
        modes: modes
            .iter()
            .map(|mode| AgentPermissionOption {
                id: mode.id.as_str().into(),
                display_name: (!mode.name.is_empty() && mode.name != mode.id)
                    .then(|| mode.name.clone().into()),
                description: (!mode.description.is_empty())
                    .then(|| mode.description.clone().into()),
            })
            .collect(),
    }
}

fn build_native_slash_command(
    catalog: &AgentCommandCatalog,
    command: &AgentCommandInvocation,
    provider: &str,
) -> Result<String> {
    let name = normalize_command_name(command.name.as_str());
    if name.is_empty() {
        return Err(LucarneError::dialect(format!(
            "{provider}: empty command name"
        )));
    }
    if name.contains(char::is_whitespace) || name.contains('/') {
        return Err(LucarneError::dialect(format!(
            "{provider}: invalid command name {:?}",
            command.name
        )));
    }
    if !catalog
        .commands
        .iter()
        .any(|cmd| command_supports_name(cmd, name))
    {
        return Err(LucarneError::dialect(format!(
            "{provider}: unsupported command {:?}",
            command.name
        )));
    }
    let args = command.args.as_deref().unwrap_or("").trim();
    if args.is_empty() {
        Ok(format!("/{name}"))
    } else {
        Ok(format!("/{name} {args}"))
    }
}

fn command_supports_name(command: &AgentCommand, name: &str) -> bool {
    command.name.as_str() == name || command.aliases.iter().any(|alias| alias.as_str() == name)
}

fn normalize_command_name(raw: &str) -> &str {
    raw.trim().trim_start_matches('/')
}

fn desired_mode(available: &[String], perm: PermissionMode) -> String {
    let candidates: &[&str] = match perm {
        PermissionMode::Default => &["default"],
        PermissionMode::Write => &["autoEdit", "auto_edit"],
        PermissionMode::ReadOnly => &["plan"],
        PermissionMode::Auto | PermissionMode::Full | PermissionMode::Bypass => &["yolo"],
    };
    for candidate in candidates {
        if available.iter().any(|mode| mode == candidate) {
            return (*candidate).to_string();
        }
    }
    String::new()
}

fn choose_permission_option_id(
    options: &[PermissionOption],
    dec: event::Decision,
) -> Option<String> {
    let preferred: &[&[&str]] = match dec {
        event::Decision::Allow => &[
            &["allow_once", "proceed_once"],
            &["allow_always", "proceed_always"],
            &["allow", "proceed"],
        ],
        _ => &[
            &["reject_once", "deny_once"],
            &["reject_always", "deny_always"],
            &["cancel", "reject", "deny"],
        ],
    };
    for names in preferred {
        for opt in options {
            if matches_option(opt, names) {
                return Some(opt.option_id.clone());
            }
        }
    }
    if dec == event::Decision::Allow {
        for opt in options {
            if !matches_option(opt, &["cancel", "reject", "deny"]) {
                return Some(opt.option_id.clone());
            }
        }
    }
    None
}

fn matches_option(opt: &PermissionOption, needles: &[&str]) -> bool {
    let haystacks = [
        opt.option_id.to_ascii_lowercase(),
        opt.kind.to_ascii_lowercase(),
        opt.name.to_ascii_lowercase(),
    ];
    for h in &haystacks {
        let h = h.trim();
        for n in needles {
            if h == *n || h.contains(*n) {
                return true;
            }
        }
    }
    false
}

#[cfg(test)]
mod tests {
    use super::{desired_mode, prompt_params, Gemini};
    use crate::dialect::{Dialect, ImageRef, Input, PermissionMode};
    use serde_json::json;

    #[test]
    fn desired_mode_maps_canonical_permission_modes() {
        let available = vec![
            "default".to_string(),
            "autoEdit".to_string(),
            "plan".to_string(),
            "yolo".to_string(),
        ];

        assert_eq!(desired_mode(&available, PermissionMode::Default), "default");
        assert_eq!(desired_mode(&available, PermissionMode::Write), "autoEdit");
        assert_eq!(desired_mode(&available, PermissionMode::ReadOnly), "plan");
        assert_eq!(desired_mode(&available, PermissionMode::Auto), "yolo");
        assert_eq!(desired_mode(&available, PermissionMode::Full), "yolo");
    }

    #[test]
    fn desired_mode_accepts_snake_case_auto_edit_ids() {
        let available = vec![
            "default".to_string(),
            "auto_edit".to_string(),
            "plan".to_string(),
            "yolo".to_string(),
        ];

        assert_eq!(desired_mode(&available, PermissionMode::Write), "auto_edit");
    }

    #[test]
    fn command_catalog_does_not_inject_adapter_commands() {
        let catalog = Gemini::new().command_catalog();
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
    fn prompt_params_include_image_blocks() {
        let params = prompt_params(
            "gemini-session",
            &Input {
                text: "read the token".into(),
                images: vec![ImageRef {
                    media_type: "image/png".into(),
                    data: vec![1, 2, 3],
                }],
            },
        );

        assert_eq!(
            params,
            json!({
                "sessionId": "gemini-session",
                "prompt": [
                    {"type": "text", "text": "read the token"},
                    {"type": "image", "mimeType": "image/png", "data": "AQID"}
                ]
            })
        );
    }
}

fn risk_for_tool_kind(kind: &str) -> Risk {
    match kind.to_ascii_lowercase().trim() {
        "edit" | "delete" | "move" | "execute" | "switch_mode" => Risk::High,
        "search" | "fetch" => Risk::Medium,
        _ => Risk::Low,
    }
}

fn is_final_status(s: &str) -> bool {
    matches!(s.to_ascii_lowercase().trim(), "completed" | "failed")
}

fn is_failure_status(s: &str) -> bool {
    s.to_ascii_lowercase().trim() == "failed"
}

fn content_block_text(raw: Option<&Value>) -> String {
    let Some(v) = raw else { return String::new() };
    if let Some(obj) = v.as_object() {
        if obj.get("type").and_then(|x| x.as_str()) == Some("text") {
            return obj
                .get("text")
                .and_then(|x| x.as_str())
                .unwrap_or("")
                .trim()
                .to_string();
        }
        if obj.get("type").and_then(|x| x.as_str()) == Some("resource") {
            return obj
                .get("resource")
                .and_then(|r| r.get("text"))
                .and_then(|x| x.as_str())
                .unwrap_or("")
                .trim()
                .to_string();
        }
    }
    String::new()
}

fn parse_usage(raw: Option<&Value>) -> Option<Usage> {
    let v = raw?;
    let i = |k: &str| v.get(k).and_then(|x| x.as_i64()).unwrap_or(0);
    let f = |k: &str| v.get(k).and_then(|x| x.as_f64()).unwrap_or(0.0);
    let u = Usage {
        input_tokens: i("inputTokens"),
        output_tokens: i("outputTokens"),
        cache_read_tokens: i("cachedReadTokens"),
        cache_write_tokens: i("cachedWriteTokens"),
        cost_usd: f("costUsd"),
    };
    if u.input_tokens == 0
        && u.output_tokens == 0
        && u.cache_read_tokens == 0
        && u.cache_write_tokens == 0
        && u.cost_usd == 0.0
    {
        None
    } else {
        Some(u)
    }
}

fn gemini_stop_reason(result: &Value) -> String {
    result
        .get("stopReason")
        .and_then(|v| v.as_str())
        .unwrap_or("")
        .trim()
        .to_ascii_lowercase()
}

/// Prefer the explicit `result.usage` block; fall back to
/// `result.meta.quota.*` which older Gemini ACP builds use to report
/// token spend. Mirrors Go's `parsePromptUsage`.
fn parse_prompt_usage(usage_raw: Option<&Value>, meta_raw: Option<&Value>) -> Option<Usage> {
    if let Some(u) = parse_usage(usage_raw) {
        return Some(u);
    }
    let meta = meta_raw?;
    let quota = meta.get("quota")?;
    let i = |k: &str| quota.get(k).and_then(|x| x.as_i64()).unwrap_or(0);
    let f = |k: &str| quota.get(k).and_then(|x| x.as_f64()).unwrap_or(0.0);
    let u = Usage {
        input_tokens: i("inputTokens"),
        output_tokens: i("outputTokens"),
        cache_read_tokens: i("cachedReadTokens"),
        cost_usd: f("costUsd"),
        ..Default::default()
    };
    if u.input_tokens == 0 && u.output_tokens == 0 && u.cache_read_tokens == 0 && u.cost_usd == 0.0
    {
        None
    } else {
        Some(u)
    }
}

fn map_tool_call(v: &Value) -> ToolCall {
    let name = v
        .get("kind")
        .and_then(|x| x.as_str())
        .filter(|s| !s.trim().is_empty())
        .or_else(|| {
            v.get("title")
                .and_then(|x| x.as_str())
                .filter(|s| !s.trim().is_empty())
        })
        .unwrap_or("gemini_tool")
        .trim();
    event::tool_call(name, v.clone())
}

/// Converts a raw ACP `toolCall` JSON object to a filtered snake_case map
/// for use as `PermissionRequest.input`. Matches Go's `projectToolCallUpdate`.
fn project_tool_call_update(v: &Value) -> Value {
    let mut out = serde_json::Map::new();
    let trim = |s: &str| s.trim().to_string();
    let str_field = |key: &str| {
        v.get(key)
            .and_then(|x| x.as_str())
            .map(trim)
            .filter(|s| !s.is_empty())
    };
    if let Some(s) = str_field("toolCallId") {
        out.insert("tool_call_id".into(), Value::String(s));
    }
    if let Some(s) = str_field("title") {
        out.insert("title".into(), Value::String(s));
    }
    if let Some(s) = str_field("kind") {
        out.insert("kind".into(), Value::String(s));
    }
    if let Some(locs) = v.get("locations") {
        if locs.as_array().map(|a| !a.is_empty()).unwrap_or(false) {
            out.insert("locations".into(), locs.clone());
        }
    }
    if let Some(content) = v.get("content") {
        if content.as_array().map(|a| !a.is_empty()).unwrap_or(false) {
            out.insert("content".into(), content.clone());
        }
    }
    if let Some(raw_input) = v.get("rawInput") {
        if !matches!(raw_input, Value::Null) {
            let bytes = serde_json::to_vec(raw_input).unwrap_or_default();
            if !bytes.is_empty() && bytes != b"null" {
                out.insert("raw_input".into(), raw_input.clone());
            }
        }
    }
    Value::Object(out)
}

fn build_tool_result(status: &str, v: &Value) -> ToolResult {
    let raw_output = v.get("rawOutput");
    let output_str = match raw_output {
        Some(Value::String(s)) => s.trim().to_string(),
        Some(other) => serde_json::to_string(other).unwrap_or_default(),
        None => {
            // Try content blocks.
            let Some(arr) = v.get("content").and_then(|x| x.as_array()) else {
                return ToolResult::default();
            };
            let mut parts: Vec<String> = Vec::new();
            for p in arr {
                let ty = p.get("type").and_then(|x| x.as_str()).unwrap_or("");
                match ty {
                    "content" => {
                        let text = content_block_text(p.get("content"));
                        if !text.is_empty() {
                            parts.push(text);
                        }
                    }
                    "diff" => {
                        if let Some(path) = p.get("path").and_then(|x| x.as_str()) {
                            if !path.is_empty() {
                                parts.push(format!("diff:{}", path));
                            }
                        }
                    }
                    "terminal" => {
                        if let Some(tid) = p.get("terminalId").and_then(|x| x.as_str()) {
                            if !tid.is_empty() {
                                parts.push(format!("terminal:{}", tid));
                            }
                        }
                    }
                    _ => {}
                }
            }
            parts.join("\n")
        }
    };
    let output = output_str.trim().to_string();
    let mut r = ToolResult {
        output: output.clone(),
        ..Default::default()
    };
    if is_failure_status(status) {
        // Go: `result.Error = firstNonEmptyString(result.Output, "tool failed")`
        // — does NOT clear output.
        r.error = if output.is_empty() {
            "tool failed".into()
        } else {
            output
        };
    }
    r
}
