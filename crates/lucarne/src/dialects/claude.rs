//! Claude dialect — translates Claude Code's `--output-format stream-json`
//! into canonical events; encodes user input / permission replies back
//! to stdin.
//!
//! Wire shape (inbound lines):
//!
//! ```text
//! {"type":"system","subtype":"init","session_id":"...","model":"..."}
//! {"type":"assistant","message":{"id":"...","content":[{"type":"text"|"thinking"|"tool_use",...}], "usage":{...}}}
//! {"type":"user","uuid":"...","message":{"content":[{"type":"text","text":"..."}]},"isReplay":true}
//! {"type":"user","message":{"content":[{"type":"tool_result","tool_use_id":"...","content":[...],"is_error":bool}]}}
//! {"type":"control_request","request_id":"...","request":{"tool":"Bash","input":{...}}}
//! {"type":"result","subtype":"success"|"error_during_execution","session_id":"...","duration_ms":N,"usage":{...}}
//! ```
//!
//! Wire shape (outbound on stdin, after we send the first prompt via stream-json):
//!
//! ```text
//! {"type":"user","message":{"role":"user","content":[{"type":"text","text":"..."}]}}
//! {"type":"control_request","request_id":"...","request":{"subtype":"get_settings"|"apply_flag_settings",...}}
//! {"type":"control_response","response":{"subtype":"success","request_id":"...","response":{"behavior":"allow"|"deny","updatedInput":{...}}}}
//! ```
//!
//! Interruption is SIGINT (not stdin).

use crate::{
    agent_runtime::{
        AgentCommand, AgentCommandCatalog, AgentCommandCompletion, AgentCommandInput,
        AgentCommandInvocation, AgentCommandSource, AgentContextUsage, AgentForkResult,
        AgentForkTarget, AgentForkTargetCatalog, AgentModelCatalog, AgentModelOption,
        AgentPermissionCatalog, AgentPermissionOption, AgentReasoningOption, AgentSkillCatalog,
        AgentSkillSummary, AgentStatus, AgentTokenUsage,
    },
    dialect::{
        command_result_events, fork_name, model_args, normalize_agent_command_name, permission_arg,
        CommandDispatch, CommandResult, Dialect, Input, ModelSelection, OutFrame, PermissionMode,
        PermissionSelection, SessionParams,
    },
    error::{LucarneError, Result},
    event::{
        self, Event, LogLine, Payload, PermissionAnswer, PermissionQuestion,
        PermissionQuestionOption, PermissionRequest, PermissionResponse, ResumeHandle, Risk,
        SessionClosed, SessionStarted, Timeline, TimelineItem, ToolResult, TurnCompleted,
        TurnFailed, Usage, UsageDelta,
    },
};
use base64::{engine::general_purpose::URL_SAFE_NO_PAD, Engine as _};
use serde::Deserialize;
use serde_json::{json, Value};
use smol_str::SmolStr;
use std::collections::{BTreeMap, HashMap, VecDeque};
use tracing::debug;

const CLAUDE_PERMISSION_MODES: &[&str] = &[
    "default",
    "acceptEdits",
    "plan",
    "auto",
    "bypassPermissions",
];

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum ReplyMode {
    Structured,
    UserTextQuestion,
}

#[cfg(test)]
mod tests {
    use super::Claude;
    use crate::agent_runtime::SessionRef;
    use crate::dialect::{CommandDispatch, CommandResult, Dialect, OutFrame, SessionParams};
    use serde_json::json;

    #[test]
    fn init_sends_sdk_initialize_control_request() {
        let mut claude = Claude::new();
        let frames = claude.init(&SessionParams::default());
        assert_eq!(frames.len(), 1);
        let OutFrame::Stdin(frame) = &frames[0] else {
            panic!("expected stdin frame");
        };
        let value: serde_json::Value = serde_json::from_slice(frame).expect("json");
        assert_eq!(
            value.pointer("/request/subtype").and_then(|v| v.as_str()),
            Some("initialize")
        );
        assert_eq!(
            value.get("request_id").and_then(|v| v.as_str()),
            Some("lucarne-control-1")
        );
    }

    #[test]
    fn legacy_amux_control_response_matches_current_lucarne_request() {
        let mut claude = Claude::new();
        claude.pending_controls.insert(
            "lucarne-control-1".into(),
            super::PendingControlCommand::ReloadPlugins,
        );

        let events = claude.translate(
            br#"{"type":"control_response","response":{"subtype":"success","request_id":"amux-control-1","response":{}}}"#,
        );

        assert!(events.is_empty());
        assert!(
            claude.pending_controls.is_empty(),
            "legacy replay response id should complete current lucarne control request"
        );
    }

    #[test]
    fn command_catalog_uses_initialize_commands_response() {
        let commands = json!({
            "commands": [
                {
                    "name": "debug",
                    "description": "Enable debug logging for this session and help diagnose issues",
                    "argumentHint": "[issue description]"
                },
                {
                    "name": "batch",
                    "description": "Run a batch job",
                    "argumentHint": "<instruction>"
                },
                {
                    "name": "usage",
                    "description": "Show the total cost and duration of the current session",
                    "argumentHint": "",
                    "aliases": ["cost", "stats"]
                }
            ]
        });
        let catalog = super::build_initialize_catalog(Some(&commands), 1).expect("catalog");
        let names = catalog
            .commands
            .iter()
            .map(|command| command.name.as_str())
            .collect::<Vec<_>>();
        assert_eq!(names, ["debug", "batch", "usage"]);
        assert_eq!(
            catalog.commands[0].description.as_deref(),
            Some("Enable debug logging for this session and help diagnose issues")
        );
        assert_eq!(
            catalog.commands[0].input,
            crate::agent_runtime::AgentCommandInput::Text {
                label: "[issue description]".into(),
                required: false,
            }
        );
        assert_eq!(
            catalog.commands[1].input,
            crate::agent_runtime::AgentCommandInput::Text {
                label: "<instruction>".into(),
                required: true,
            }
        );
        assert_eq!(
            catalog.commands[2]
                .aliases
                .iter()
                .map(|alias| alias.as_str())
                .collect::<Vec<_>>(),
            ["cost", "stats"]
        );
    }

    #[test]
    fn fresh_command_catalog_is_incomplete_before_init() {
        let catalog = Claude::new().command_catalog();
        assert!(!catalog.complete);
        assert!(catalog.commands.is_empty());
    }

    #[test]
    fn selected_fork_target_returns_resumable_claude_fork_ref() {
        let mut claude = Claude::new();
        claude.session_id = "session-source".into();

        let CommandDispatch::Ready(CommandResult::Forked(result)) =
            claude.fork(Some("turn-uuid")).expect("fork dispatch")
        else {
            panic!("expected ready fork result");
        };

        assert_eq!(
            result.source_session_ref,
            Some(SessionRef("session-source".into()))
        );
        assert_eq!(
            result.session_ref,
            Some(SessionRef(
                "claude-fork:v1:c2Vzc2lvbi1zb3VyY2U:dHVybi11dWlk".into()
            ))
        );
    }

    #[test]
    fn apply_patch_tool_passthrough_keeps_name_and_input() {
        let mut claude = Claude::new();
        let frame = json!({
            "type": "assistant",
            "message": {
                "id": "msg-1",
                "content": [{
                    "type": "tool_use",
                    "id": "tool-1",
                    "name": "apply_patch",
                    "input": {"patch": "*** Begin Patch\n*** End Patch"}
                }]
            }
        });

        let evs = claude.translate(&serde_json::to_vec(&frame).unwrap());
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
    fn thinking_block_projects_as_reasoning_not_assistant_output() {
        let mut claude = Claude::new();
        let frame = json!({
            "type": "assistant",
            "message": {
                "id": "msg-1",
                "content": [{
                    "type": "thinking",
                    "thinking": "I am checking the current workspace first."
                }]
            }
        });

        let evs = claude.translate(&serde_json::to_vec(&frame).unwrap());
        assert!(
            evs.iter().any(|ev| {
                matches!(
                    &ev.payload,
                    crate::event::Payload::Timeline(timeline)
                        if timeline.item.reasoning.as_ref().is_some_and(|reasoning| {
                            reasoning.text == "I am checking the current workspace first."
                        })
                )
            }),
            "Claude thinking blocks should project as reasoning: {evs:?}"
        );
        assert!(
            evs.iter().all(|ev| {
                !matches!(
                    &ev.payload,
                    crate::event::Payload::Timeline(timeline)
                        if timeline.item.assistant_message.is_some()
                )
            }),
            "Claude thinking blocks must not project as assistant output: {evs:?}"
        );
    }

    #[test]
    fn status_maps_current_claude_control_responses() {
        let settings = json!({
            "effective": {
                "env": {
                    "ANTHROPIC_AUTH_TOKEN": "sk-ant-redacted",
                    "ANTHROPIC_BASE_URL": "https://api.derouter.network/proxy"
                },
                "model": "opus[1m]"
            },
            "applied": {
                "model": "claude-opus-4-7[1m]",
                "effort": "xhigh"
            },
            "sources": [
                { "source": "userSettings" }
            ]
        });
        let context = json!({
            "totalTokens": 747399,
            "maxTokens": 1000000,
            "percentage": 75,
            "model": "claude-opus-4-7[1m]",
            "apiUsage": null
        });
        let account = json!({
            "tokenSource": "ANTHROPIC_AUTH_TOKEN",
            "apiProvider": "firstParty"
        });

        let status = super::claude_status(
            "sess",
            "/tmp/project",
            "",
            None,
            crate::dialect::PermissionMode::Default,
            Some(&settings),
            Some(&context),
            None,
            Some(&account),
            Some("2.1.112"),
        );

        assert_eq!(status.version.as_deref(), Some("2.1.112"));
        assert_eq!(status.model.as_deref(), Some("opus[1m]"));
        assert_eq!(status.model_detail.as_deref(), Some("claude-opus-4-7[1m]"));
        assert_eq!(status.reasoning.as_deref(), Some("xhigh"));
        assert_eq!(
            status.account.as_deref(),
            Some("ANTHROPIC_AUTH_TOKEN (firstParty)")
        );
        assert_eq!(
            status.base_url.as_deref(),
            Some("https://api.derouter.network/proxy")
        );
        assert_eq!(
            status.context,
            Some(crate::agent_runtime::AgentContextUsage {
                used_tokens: Some(747399),
                max_tokens: Some(1000000),
                percent_used: Some(75),
            })
        );
        assert!(status.tokens.is_none());
    }
}

#[derive(Clone, Debug)]
struct PendingPermission {
    mode: ReplyMode,
    input: Option<Value>,
    questions: Vec<PermissionQuestion>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
struct PendingForkPrompt {
    id: Option<String>,
    preview: String,
}

#[derive(Clone, Debug, Default)]
struct ClaudeModelInfo {
    value: String,
    display_name: String,
    description: String,
    supported_effort_levels: Vec<String>,
}

#[derive(Clone, Debug)]
enum PendingControlCommand {
    Initialize,
    ReloadPlugins,
    ModelSet { value: String },
    ModelVerify { value: String },
    ModelReasoningSet { model: String, effort: String },
    ModelReasoningVerify { model: String, effort: String },
    PermissionsList,
    PermissionsSet { mode: PermissionMode },
    PermissionsVerify { mode: PermissionMode },
    SkillsList,
    StatusSettings,
    StatusContext { settings: Value },
}

pub struct Claude {
    cfg: SessionParams,
    session_id: String,
    resume_id: String,
    turn_failed: bool,
    turn_idle: bool,
    pending: HashMap<String, PendingPermission>,
    out_frames: Vec<OutFrame>,
    deferred: Vec<String>,
    command_catalog: AgentCommandCatalog,
    next_control_id: u64,
    pending_controls: HashMap<String, PendingControlCommand>,
    current_model: String,
    current_reasoning_effort: Option<String>,
    current_permission_mode: PermissionMode,
    latest_usage: Option<Usage>,
    models: Vec<ClaudeModelInfo>,
    account: Option<Value>,
    cli_version: Option<String>,
    fork_targets: Vec<AgentForkTarget>,
    pending_fork_prompts: VecDeque<PendingForkPrompt>,
}

impl Claude {
    pub fn new() -> Self {
        Self::with_cli_version(None)
    }

    pub fn with_cli_version(cli_version: Option<String>) -> Self {
        Self {
            cfg: SessionParams::default(),
            session_id: String::new(),
            resume_id: String::new(),
            turn_failed: false,
            turn_idle: true,
            pending: HashMap::new(),
            out_frames: Vec::new(),
            deferred: Vec::new(),
            command_catalog: AgentCommandCatalog::default(),
            next_control_id: 1,
            pending_controls: HashMap::new(),
            current_model: String::new(),
            current_reasoning_effort: None,
            current_permission_mode: PermissionMode::Default,
            latest_usage: None,
            models: Vec::new(),
            account: None,
            cli_version,
            fork_targets: Vec::new(),
            pending_fork_prompts: VecDeque::new(),
        }
    }
}

impl Default for Claude {
    fn default() -> Self {
        Self::new()
    }
}

impl Claude {
    fn list_commands(&mut self) -> Result<CommandDispatch> {
        Ok(CommandDispatch::ready(CommandResult::Commands(
            self.command_catalog.clone(),
        )))
    }

    fn list_models(&mut self) -> Result<CommandDispatch> {
        Ok(CommandDispatch::ready(CommandResult::Models(
            claude_model_catalog(&self.current_model, &self.models),
        )))
    }

    fn set_model(&mut self, model: &str, reasoning: Option<&str>) -> Result<CommandDispatch> {
        let model = model.to_string();
        let effort = reasoning.map(validate_reasoning_effort).transpose()?;
        self.turn_idle = false;
        if let Some(effort) = effort {
            self.queue_control_request(
                PendingControlCommand::ModelReasoningSet {
                    model: model.clone(),
                    effort: effort.clone(),
                },
                json!({
                    "subtype": "apply_flag_settings",
                    "settings": {
                        "model": model,
                        "effort": effort,
                    },
                }),
            )
            .map(CommandDispatch::deferred)
        } else {
            self.queue_control_request(
                PendingControlCommand::ModelSet {
                    value: model.clone(),
                },
                json!({
                    "subtype": "apply_flag_settings",
                    "settings": {
                        "model": model,
                    },
                }),
            )
            .map(CommandDispatch::deferred)
        }
    }

    fn list_permissions(&mut self) -> Result<CommandDispatch> {
        self.turn_idle = false;
        self.queue_control_request(
            PendingControlCommand::PermissionsList,
            json!({ "subtype": "get_settings" }),
        )
        .map(CommandDispatch::deferred)
    }

    fn set_permissions(&mut self, value: &str) -> Result<CommandDispatch> {
        let Some(mode) = claude_permission_mode_from_provider(value) else {
            return Err(LucarneError::dialect(format!(
                "claude: unsupported permissionMode {:?}; expected one of {}",
                value,
                CLAUDE_PERMISSION_MODES.join(", ")
            )));
        };
        self.turn_idle = false;
        self.queue_control_request(
            PendingControlCommand::PermissionsSet { mode },
            json!({
                "subtype": "apply_flag_settings",
                "settings": {
                    "permissionMode": claude_permission_mode_to_provider(mode),
                },
            }),
        )
        .map(CommandDispatch::deferred)
    }

    fn list_skills(&mut self) -> Result<CommandDispatch> {
        self.turn_idle = false;
        self.queue_control_request(
            PendingControlCommand::SkillsList,
            json!({ "subtype": "get_context_usage" }),
        )
        .map(CommandDispatch::deferred)
    }

    fn status(&mut self) -> Result<CommandDispatch> {
        self.turn_idle = false;
        self.queue_control_request(
            PendingControlCommand::StatusSettings,
            json!({ "subtype": "get_settings" }),
        )
        .map(CommandDispatch::deferred)
    }

    fn list_fork(&mut self) -> Result<CommandDispatch> {
        Ok(CommandDispatch::ready(CommandResult::ForkTargets(
            AgentForkTargetCatalog {
                targets: self.fork_targets.clone(),
            },
        )))
    }

    fn new_conversation_command(&mut self) -> Result<CommandDispatch> {
        self.turn_idle = false;
        self.clear_fork_targets();
        encode_claude_official_command("clear", None).map(CommandDispatch::deferred)
    }

    fn quit(&mut self) -> Result<CommandDispatch> {
        self.turn_idle = false;
        encode_claude_official_command("exit", None).map(CommandDispatch::deferred)
    }

    fn fork(&mut self, target: Option<&str>) -> Result<CommandDispatch> {
        let target = target.unwrap_or("").trim();
        if target.is_empty() {
            return self.list_fork();
        }
        let source_session_id =
            first_non_empty(&[self.session_id.as_str(), self.resume_id.as_str()]);
        let Some(fork_ref) = encode_claude_fork_ref(&source_session_id, target) else {
            return Err(LucarneError::dialect(
                "claude: fork invoked before session is ready".to_string(),
            ));
        };
        Ok(CommandDispatch::ready(CommandResult::Forked(
            AgentForkResult {
                session_ref: Some(crate::agent_runtime::SessionRef(fork_ref.into())),
                source_session_ref: Some(crate::agent_runtime::SessionRef(
                    source_session_id.into(),
                )),
            },
        )))
    }

    fn invoke_provider_command(
        &mut self,
        command: &AgentCommandInvocation,
    ) -> Result<CommandDispatch> {
        let text = build_native_slash_command(&self.command_catalog, command, self.name())?;
        self.turn_idle = false;
        self.record_pending_fork_prompt(&text);
        encode_user_text(&text).map(CommandDispatch::deferred)
    }
}

impl Dialect for Claude {
    fn name(&self) -> &'static str {
        "claude"
    }

    fn init(&mut self, cfg: &SessionParams) -> Vec<OutFrame> {
        self.cfg = cfg.clone();
        self.resume_id = cfg
            .resume_data()
            .and_then(|d| d.get("session_id"))
            .and_then(|v| v.as_str())
            .unwrap_or("")
            .to_string();
        self.turn_idle = true;
        self.clear_fork_targets();
        debug!(
            target: "lucarne::dialects::claude",
            model = cfg.model.as_str(),
            cwd = cfg.cwd.as_str(),
            resume = !self.resume_id.is_empty(),
            "claude dialect initialized"
        );
        self.queue_control_request(
            PendingControlCommand::Initialize,
            json!({ "subtype": "initialize" }),
        )
        .expect("claude initialize control request is serializable")
    }

    fn drain_out_frames(&mut self) -> Vec<OutFrame> {
        std::mem::take(&mut self.out_frames)
    }

    fn encode_user_message(&mut self, input: &Input) -> Result<Vec<OutFrame>> {
        self.turn_idle = false;
        self.record_pending_fork_prompt(&input_fork_prompt_text(input));
        encode_user_input(input)
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
            "fork" => match fork_name(command) {
                Some(target) => self.fork(Some(&target)),
                None => self.list_fork(),
            },
            "list_commands" => self.list_commands(),
            name => Err(LucarneError::dialect(format!(
                "claude: unsupported system command {name:?}"
            ))),
        }
    }

    fn handle_native_command(
        &mut self,
        command: &AgentCommandInvocation,
    ) -> Result<CommandDispatch> {
        self.invoke_provider_command(command)
    }

    fn encode_permission_response(
        &mut self,
        req_id: &str,
        resp: &PermissionResponse,
    ) -> Result<Vec<OutFrame>> {
        let pending = self.pending.remove(req_id);
        let Some(p) = pending else {
            if !resp.answers.is_empty() {
                return Err(LucarneError::dialect(format!(
                    "claude: no pending permission request {:?} for structured answers",
                    req_id
                )));
            }
            return encode_control_response(req_id, resp.decision, None);
        };
        match p.mode {
            ReplyMode::Structured => {
                let updated = build_updated_input(&p, resp);
                encode_control_response(req_id, resp.decision, updated)
            }
            ReplyMode::UserTextQuestion => {
                let reply = build_question_reply(&p, resp);
                if self.turn_idle {
                    encode_user_text(&reply)
                } else {
                    self.deferred.push(reply);
                    Ok(Vec::new())
                }
            }
        }
    }

    fn encode_interrupt(&mut self) -> Result<Vec<OutFrame>> {
        Ok(vec![OutFrame::signal("SIGINT")])
    }

    fn translate(&mut self, frame: &[u8]) -> Vec<Event> {
        #[derive(Deserialize, Default)]
        struct Raw {
            #[serde(default, rename = "type")]
            ty: String,
            #[serde(default)]
            subtype: String,
            #[serde(default)]
            stop_reason: String,
            #[serde(default)]
            session_id: String,
            #[serde(default)]
            uuid: String,
            #[serde(default)]
            model: String,
            #[serde(default)]
            message: Option<Value>,
            #[serde(default)]
            request_id: String,
            #[serde(default)]
            request: Option<Value>,
            #[serde(default)]
            is_error: bool,
            #[serde(default)]
            result: String,
            #[serde(default)]
            errors: Vec<Value>,
            #[serde(default)]
            total_cost_usd: f64,
            #[serde(default)]
            usage: Option<Value>,
            #[serde(default)]
            #[serde(rename = "permissionMode")]
            permission_mode: String,
            #[serde(default)]
            response: Option<Value>,
        }
        let raw: Raw = match serde_json::from_slice(frame) {
            Ok(v) => v,
            Err(e) => {
                return vec![Event::new(Payload::Log(LogLine {
                    level: "warn".into(),
                    stream: "stdout".into(),
                    text: format!("claude: bad JSON: {}", e),
                }))];
            }
        };

        match raw.ty.as_str() {
            "system" => {
                if raw.subtype == "init" {
                    self.session_id = raw.session_id.clone();
                    self.current_model = raw.model.clone();
                    self.current_permission_mode =
                        claude_permission_mode_from_provider(&raw.permission_mode)
                            .unwrap_or_default();
                    debug!(
                        target: "lucarne::dialects::claude",
                        session_id = raw.session_id.as_str(),
                        model = raw.model.as_str(),
                        permission_mode = ?self.current_permission_mode,
                        "claude session initialized"
                    );
                    return vec![Event::new(Payload::SessionStarted(SessionStarted {
                        session_id: raw.session_id,
                        model: raw.model,
                    }))];
                }
                Vec::new()
            }
            "control_response" => self.translate_control_response(raw.response.as_ref()),
            "assistant" => {
                self.turn_idle = false;
                self.translate_assistant(non_empty_str(&raw.uuid), raw.message.as_ref())
            }
            "user" => {
                self.turn_idle = false;
                self.translate_user(non_empty_str(&raw.uuid), raw.message.as_ref())
            }
            "control_request" => {
                self.turn_idle = false;
                self.translate_control_request(&raw.request_id, raw.request.as_ref())
            }
            "result" => {
                if !raw.session_id.is_empty() {
                    self.session_id = raw.session_id.clone();
                }
                let usage = parse_usage(raw.usage.as_ref(), raw.total_cost_usd);
                if let Some(usage) = usage.as_ref().filter(|usage| has_usage(usage)) {
                    self.latest_usage = Some(usage.clone());
                }
                self.turn_failed = raw.is_error
                    || raw.subtype == "error_during_execution"
                    || is_max_turns_result(&raw.subtype, &raw.stop_reason, &raw.result);
                self.turn_idle = true;
                debug!(
                    target: "lucarne::dialects::claude",
                    session_id = self.session_id.as_str(),
                    failed = self.turn_failed,
                    subtype = raw.subtype.as_str(),
                    stop_reason = raw.stop_reason.as_str(),
                    has_usage = usage.as_ref().is_some_and(has_usage),
                    "claude turn result translated"
                );
                let mut out = Vec::new();
                if !self.turn_failed {
                    if !self.deferred.is_empty() {
                        let pending = std::mem::take(&mut self.deferred);
                        for text in pending {
                            match encode_user_text(&text) {
                                Ok(frames) => self.out_frames.extend(frames),
                                Err(e) => {
                                    return vec![Event::new(Payload::Log(LogLine {
                                        level: "warn".into(),
                                        stream: "stdout".into(),
                                        text: format!(
                                            "claude: failed to encode deferred reply: {}",
                                            e
                                        ),
                                    }))];
                                }
                            }
                        }
                    }
                }
                if self.turn_failed {
                    let code = first_non_empty(&[raw.stop_reason.trim(), &raw.subtype]);
                    let mut err_msg = first_non_empty(&[
                        raw.result.trim(),
                        &first_claude_error(&raw.errors),
                        "claude execution error",
                    ]);
                    if is_max_turns_result(&raw.subtype, &raw.stop_reason, &raw.result)
                        && err_msg == "claude execution error"
                    {
                        err_msg = "maximum turns reached".to_string();
                    }
                    out.push(Event::new(Payload::TurnFailed(TurnFailed {
                        turn_id: String::new(),
                        error: err_msg,
                        code,
                    })));
                } else {
                    out.push(Event::new(Payload::TurnCompleted(TurnCompleted {
                        turn_id: String::new(),
                        usage,
                    })));
                }
                out
            }
            _ => Vec::new(),
        }
    }

    fn on_exit(&mut self, exit_code: i32, err: Option<String>) -> Vec<Event> {
        let reason = if let Some(e) = err {
            e
        } else if exit_code != 0 {
            format!("exit:{}", exit_code)
        } else {
            "ok".into()
        };
        let mut payload = SessionClosed {
            reason,
            resume: None,
        };
        let sid = resolve_session_id(&self.resume_id, &self.session_id, self.turn_failed);
        if !sid.is_empty() {
            let mut data: BTreeMap<String, Value> = BTreeMap::new();
            data.insert("session_id".into(), Value::String(sid));
            if !self.cfg.cwd.is_empty() {
                data.insert("cwd".into(), Value::String(self.cfg.cwd.clone()));
            }
            payload.resume = Some(ResumeHandle { version: 1, data });
        }
        vec![Event::new(Payload::SessionClosed(payload))]
    }
}

// ——— translation helpers ———

impl Claude {
    fn clear_fork_targets(&mut self) {
        self.fork_targets.clear();
        self.pending_fork_prompts.clear();
    }

    fn record_pending_fork_prompt(&mut self, text: &str) {
        self.record_pending_fork_prompt_with_id(text, None);
    }

    fn record_pending_fork_prompt_with_id(&mut self, text: &str, id: Option<&str>) {
        let preview = compact_list_text(text);
        if preview.is_empty() {
            return;
        }
        let id = id
            .map(str::trim)
            .filter(|id| !id.is_empty())
            .map(str::to_string);
        if let Some(pending) = self
            .pending_fork_prompts
            .back_mut()
            .filter(|pending| pending.preview == preview)
        {
            if pending.id.is_none() {
                pending.id = id;
            }
            return;
        }
        self.pending_fork_prompts
            .push_back(PendingForkPrompt { id, preview });
    }

    fn record_fork_target(
        &mut self,
        assistant_uuid: Option<&str>,
        assistant_id: &str,
        assistant_text: &str,
    ) {
        let fallback_id = assistant_uuid
            .map(str::trim)
            .filter(|id| !id.is_empty())
            .unwrap_or_else(|| assistant_id.trim());
        if fallback_id.is_empty() {
            return;
        }
        let Some(user_prompt) = self.pending_fork_prompts.pop_front() else {
            return;
        };
        let target_id = user_prompt.id.as_deref().unwrap_or(fallback_id);
        if self
            .fork_targets
            .iter()
            .any(|target| target.id.as_str() == target_id)
        {
            return;
        }
        let assistant_preview = compact_list_text(assistant_text);
        self.fork_targets.push(AgentForkTarget {
            id: target_id.into(),
            label: Some(user_prompt.preview.into()),
            description: (!assistant_preview.is_empty())
                .then(|| format!("reply: {assistant_preview}").into()),
        });
        if self.fork_targets.len() > 20 {
            self.fork_targets.remove(0);
        }
    }

    fn queue_control_request(
        &mut self,
        pending: PendingControlCommand,
        request: Value,
    ) -> Result<Vec<OutFrame>> {
        let request_id = format!("lucarne-control-{}", self.next_control_id);
        self.next_control_id += 1;
        self.pending_controls.insert(request_id.clone(), pending);
        let payload = json!({
            "type": "control_request",
            "request_id": request_id,
            "request": request,
        });
        let mut line = serde_json::to_vec(&payload)?;
        line.push(b'\n');
        Ok(vec![OutFrame::Stdin(line)])
    }

    fn translate_control_response(&mut self, response: Option<&Value>) -> Vec<Event> {
        let Some(response) = response else {
            return Vec::new();
        };
        let request_id = response
            .get("request_id")
            .and_then(Value::as_str)
            .unwrap_or("");
        if response.get("subtype").and_then(Value::as_str) == Some("success") {
            if let Some(catalog) = build_initialize_catalog(
                response.get("response"),
                self.command_catalog.revision + 1,
            ) {
                self.command_catalog = catalog;
            }
            if let Some(models) = parse_initialize_models(response.get("response")) {
                self.models = models;
            }
            if let Some(account) = response
                .get("response")
                .and_then(|value| value.get("account"))
            {
                self.account = Some(account.clone());
            }
        }
        let Some(pending_key) = self.control_pending_key(request_id) else {
            return Vec::new();
        };
        let Some(pending) = self.pending_controls.remove(pending_key.as_str()) else {
            return Vec::new();
        };
        let request_id = pending_key.as_str();
        if response.get("subtype").and_then(Value::as_str) != Some("success") {
            if matches!(
                pending,
                PendingControlCommand::Initialize | PendingControlCommand::ReloadPlugins
            ) {
                return vec![Event::new(Payload::Log(LogLine {
                    level: "warn".into(),
                    stream: "stdout".into(),
                    text: format!("claude command catalog control request {request_id} failed"),
                }))];
            }
            return vec![Event::new(Payload::TurnFailed(TurnFailed {
                turn_id: request_id.into(),
                error: format!("claude control request {request_id} failed"),
                code: response
                    .get("subtype")
                    .and_then(Value::as_str)
                    .unwrap_or("")
                    .into(),
            }))];
        }
        match pending {
            PendingControlCommand::Initialize => {
                if !self.command_catalog.complete {
                    match self.queue_reload_plugins_commands() {
                        Ok(()) => {}
                        Err(err) => {
                            return vec![Event::new(Payload::Log(LogLine {
                                level: "warn".into(),
                                stream: "stdout".into(),
                                text: format!(
                                    "claude reload_plugins control request failed: {err}"
                                ),
                            }))];
                        }
                    }
                }
                Vec::new()
            }
            PendingControlCommand::ReloadPlugins => Vec::new(),
            PendingControlCommand::ModelSet { value } => {
                match self.queue_control_request(
                    PendingControlCommand::ModelVerify { value },
                    json!({ "subtype": "get_settings" }),
                ) {
                    Ok(frames) => self.out_frames.extend(frames),
                    Err(err) => {
                        return vec![Event::new(Payload::TurnFailed(TurnFailed {
                            turn_id: request_id.into(),
                            error: err.to_string().into(),
                            code: String::new(),
                        }))];
                    }
                }
                Vec::new()
            }
            PendingControlCommand::ModelReasoningSet { model, effort } => {
                match self.queue_control_request(
                    PendingControlCommand::ModelReasoningVerify { model, effort },
                    json!({ "subtype": "get_settings" }),
                ) {
                    Ok(frames) => self.out_frames.extend(frames),
                    Err(err) => {
                        return vec![Event::new(Payload::TurnFailed(TurnFailed {
                            turn_id: request_id.into(),
                            error: err.to_string().into(),
                            code: String::new(),
                        }))];
                    }
                }
                Vec::new()
            }
            PendingControlCommand::PermissionsSet { mode } => {
                match self.queue_control_request(
                    PendingControlCommand::PermissionsVerify { mode },
                    json!({ "subtype": "get_settings" }),
                ) {
                    Ok(frames) => self.out_frames.extend(frames),
                    Err(err) => {
                        return vec![Event::new(Payload::TurnFailed(TurnFailed {
                            turn_id: request_id.into(),
                            error: err.to_string().into(),
                            code: String::new(),
                        }))];
                    }
                }
                Vec::new()
            }
            PendingControlCommand::StatusSettings => {
                let settings = response.get("response").cloned().unwrap_or(Value::Null);
                match self.queue_control_request(
                    PendingControlCommand::StatusContext { settings },
                    json!({ "subtype": "get_context_usage" }),
                ) {
                    Ok(frames) => self.out_frames.extend(frames),
                    Err(err) => {
                        return vec![Event::new(Payload::TurnFailed(TurnFailed {
                            turn_id: request_id.into(),
                            error: err.to_string().into(),
                            code: String::new(),
                        }))];
                    }
                }
                Vec::new()
            }
            pending => self.finish_control_command(request_id, pending, response.get("response")),
        }
    }

    fn control_pending_key(&self, request_id: &str) -> Option<String> {
        if self.pending_controls.contains_key(request_id) {
            return Some(request_id.to_string());
        }
        request_id
            .strip_prefix("amux-control-")
            .map(|suffix| format!("lucarne-control-{suffix}"))
            .filter(|key| self.pending_controls.contains_key(key))
    }

    fn finish_control_command(
        &mut self,
        request_id: &str,
        pending: PendingControlCommand,
        result: Option<&Value>,
    ) -> Vec<Event> {
        match pending {
            PendingControlCommand::ModelVerify { value } => {
                if !claude_setting_matches(result, "/applied/model", &value)
                    && !claude_setting_matches(result, "/effective/model", &value)
                {
                    return vec![Event::new(Payload::TurnFailed(TurnFailed {
                        turn_id: request_id.into(),
                        error: format!("claude model verification failed for {value}").into(),
                        code: String::new(),
                    }))];
                }
                self.current_model = value.clone();
                self.current_reasoning_effort =
                    claude_setting_str(result, &["/effective/effort", "/applied/effort"])
                        .map(str::to_string);
                self.turn_idle = true;
                return command_result_events(
                    request_id,
                    CommandResult::ModelChanged(ModelSelection {
                        model: value.into(),
                        reasoning: self
                            .current_reasoning_effort
                            .as_ref()
                            .map(|effort| effort.as_str().into()),
                    }),
                );
            }
            PendingControlCommand::ModelReasoningVerify { model, effort } => {
                if (!claude_setting_matches(result, "/applied/model", &model)
                    && !claude_setting_matches(result, "/effective/model", &model))
                    || (!claude_setting_matches(result, "/applied/effort", &effort)
                        && !claude_setting_matches(result, "/effective/effort", &effort))
                {
                    return vec![Event::new(Payload::TurnFailed(TurnFailed {
                        turn_id: request_id.into(),
                        error: format!(
                            "claude model/reasoning verification failed for {model} {effort}"
                        )
                        .into(),
                        code: String::new(),
                    }))];
                }
                self.current_model = model.clone();
                self.current_reasoning_effort = Some(effort.clone());
                self.turn_idle = true;
                return command_result_events(
                    request_id,
                    CommandResult::ModelChanged(ModelSelection {
                        model: model.into(),
                        reasoning: Some(effort.into()),
                    }),
                );
            }
            PendingControlCommand::PermissionsList => {
                self.turn_idle = true;
                return command_result_events(
                    request_id,
                    CommandResult::Permissions(claude_permission_catalog(
                        result,
                        self.current_permission_mode,
                    )),
                );
            }
            PendingControlCommand::PermissionsVerify { mode } => {
                let provider = claude_permission_mode_to_provider(mode);
                if !claude_setting_matches(result, "/effective/permissionMode", provider) {
                    return vec![Event::new(Payload::TurnFailed(TurnFailed {
                        turn_id: request_id.into(),
                        error: format!(
                            "claude permission verification failed for {}",
                            claude_permission_mode_to_provider(mode)
                        )
                        .into(),
                        code: String::new(),
                    }))];
                }
                self.current_permission_mode = mode;
                self.turn_idle = true;
                return command_result_events(
                    request_id,
                    CommandResult::PermissionsChanged(PermissionSelection {
                        mode: claude_permission_mode_to_provider(mode).into(),
                    }),
                );
            }
            PendingControlCommand::SkillsList => {
                self.turn_idle = true;
                return command_result_events(
                    request_id,
                    CommandResult::Skills(claude_skill_catalog(result)),
                );
            }
            PendingControlCommand::StatusContext { settings } => {
                self.turn_idle = true;
                return command_result_events(
                    request_id,
                    CommandResult::Status(claude_status(
                        &self.session_id,
                        &self.cfg.cwd,
                        &self.current_model,
                        self.current_reasoning_effort.as_deref(),
                        self.current_permission_mode,
                        Some(&settings),
                        result,
                        self.latest_usage.as_ref(),
                        self.account.as_ref(),
                        self.cli_version.as_deref(),
                    )),
                );
            }
            PendingControlCommand::Initialize
            | PendingControlCommand::ReloadPlugins
            | PendingControlCommand::ModelSet { .. }
            | PendingControlCommand::ModelReasoningSet { .. }
            | PendingControlCommand::PermissionsSet { .. }
            | PendingControlCommand::StatusSettings => return Vec::new(),
        }
    }

    fn queue_reload_plugins_commands(&mut self) -> Result<()> {
        if self
            .pending_controls
            .values()
            .any(|pending| matches!(pending, PendingControlCommand::ReloadPlugins))
        {
            return Ok(());
        }
        let frames = self.queue_control_request(
            PendingControlCommand::ReloadPlugins,
            json!({ "subtype": "reload_plugins" }),
        )?;
        self.out_frames.extend(frames);
        Ok(())
    }

    fn translate_assistant(&mut self, uuid: Option<&str>, msg: Option<&Value>) -> Vec<Event> {
        let Some(m) = msg else { return Vec::new() };
        let id = m
            .get("id")
            .and_then(|v| v.as_str())
            .unwrap_or("")
            .to_string();
        let usage_raw = m.get("usage");
        let content = m
            .get("content")
            .and_then(|v| v.as_array())
            .cloned()
            .unwrap_or_default();
        let mut out = Vec::new();
        for b in &content {
            let ty = b.get("type").and_then(|v| v.as_str()).unwrap_or("");
            match ty {
                "text" => {
                    let text = b
                        .get("text")
                        .and_then(|v| v.as_str())
                        .unwrap_or("")
                        .to_string();
                    self.record_fork_target(uuid, &id, &text);
                    out.push(tl(event::new_timeline_assistant(&id, &text, false)));
                }
                "thinking" => {
                    let text = b
                        .get("text")
                        .and_then(|v| v.as_str())
                        .or_else(|| b.get("thinking").and_then(|v| v.as_str()))
                        .unwrap_or("")
                        .to_string();
                    out.push(tl(event::new_timeline_reasoning(&id, &text)));
                }
                "tool_use" => {
                    let tid = b
                        .get("id")
                        .and_then(|v| v.as_str())
                        .unwrap_or("")
                        .to_string();
                    let name = b
                        .get("name")
                        .and_then(|v| v.as_str())
                        .unwrap_or("")
                        .to_string();
                    let input = b.get("input").cloned();
                    let call =
                        event::tool_call(name.as_str(), input.clone().unwrap_or(Value::Null));
                    out.push(tl(event::new_timeline_tool_call(&tid, call)));
                    if let Some(req) =
                        self.synthesize_question_permission(&tid, &name, input.as_ref())
                    {
                        out.push(req);
                    }
                }
                _ => {}
            }
        }
        if let Some(u) = parse_usage(usage_raw, 0.0) {
            if has_usage(&u) {
                self.latest_usage = Some(u.clone());
                out.push(Event::new(Payload::UsageDelta(UsageDelta { delta: u })));
            }
        }
        out
    }

    fn translate_user(&mut self, uuid: Option<&str>, msg: Option<&Value>) -> Vec<Event> {
        let Some(m) = msg else { return Vec::new() };
        let content = m
            .get("content")
            .and_then(|v| v.as_array())
            .cloned()
            .unwrap_or_default();
        let mut out = Vec::new();
        for b in &content {
            match b.get("type").and_then(|v| v.as_str()).unwrap_or("") {
                "text" => {
                    if let Some(text) = b.get("text").and_then(|v| v.as_str()) {
                        self.record_pending_fork_prompt_with_id(text, uuid);
                    }
                    continue;
                }
                "tool_result" => {}
                _ => continue,
            }
            let tu = b
                .get("tool_use_id")
                .and_then(|v| v.as_str())
                .unwrap_or("")
                .to_string();
            let text = extract_text(b.get("content"));
            let is_error = b.get("is_error").and_then(|v| v.as_bool()).unwrap_or(false);
            let mut result = ToolResult {
                output: text.clone(),
                ..Default::default()
            };
            if is_error {
                result.error = text;
                result.output = String::new();
            }
            out.push(tl(event::new_timeline_tool_result("", &tu, result)));
        }
        out
    }

    fn translate_control_request(&mut self, req_id: &str, raw: Option<&Value>) -> Vec<Event> {
        let tool = raw
            .and_then(|v| v.get("tool").or_else(|| v.get("tool_name")))
            .and_then(|v| v.as_str())
            .unwrap_or("")
            .to_string();
        let input = raw.and_then(|v| v.get("input").cloned());
        let questions = project_questions(input.as_ref());
        self.pending.insert(
            req_id.to_string(),
            PendingPermission {
                mode: ReplyMode::Structured,
                input: input.clone(),
                questions: questions.clone(),
            },
        );
        vec![Event::new(Payload::PermissionRequest(PermissionRequest {
            req_id: req_id.to_string(),
            tool: tool.clone(),
            input,
            risk: risk_of(&tool),
            questions,
        }))]
    }

    fn synthesize_question_permission(
        &mut self,
        tool_use_id: &str,
        tool_name: &str,
        input: Option<&Value>,
    ) -> Option<Event> {
        if tool_name != "AskUserQuestion" {
            return None;
        }
        let questions = project_questions(input);
        if questions.is_empty() {
            return None;
        }
        let req_id = if tool_use_id.is_empty() {
            "claude-question".to_string()
        } else {
            tool_use_id.to_string()
        };
        self.pending.insert(
            req_id.clone(),
            PendingPermission {
                mode: ReplyMode::UserTextQuestion,
                input: input.cloned(),
                questions: questions.clone(),
            },
        );
        Some(Event::new(Payload::PermissionRequest(PermissionRequest {
            req_id,
            tool: tool_name.to_string(),
            input: input.cloned(),
            risk: risk_of(tool_name),
            questions,
        })))
    }
}

// ——— Permission reply builders ———

fn build_updated_input(p: &PendingPermission, resp: &PermissionResponse) -> Option<Value> {
    if resp.answers.is_empty() {
        return None;
    }
    let mut base: serde_json::Map<String, Value> = match p.input.as_ref() {
        Some(Value::Object(m)) => m.clone(),
        _ => serde_json::Map::new(),
    };
    let mut answers = serde_json::Map::new();
    for q in &p.questions {
        if let Some(ans) = lookup_answer(&resp.answers, q) {
            let text = format_permission_answer(&ans);
            if text.is_empty() {
                continue;
            }
            let key = first_non_empty(&[&q.header, &q.id, &q.question]);
            if key.is_empty() {
                continue;
            }
            answers.insert(key, Value::String(text));
        }
    }
    if answers.is_empty() {
        for (k, v) in &resp.answers {
            let text = format_permission_answer(v);
            if !text.is_empty() {
                answers.insert(k.clone(), Value::String(text));
            }
        }
    }
    if answers.is_empty() {
        return None;
    }
    base.insert("answers".into(), Value::Object(answers));
    Some(Value::Object(base))
}

fn build_question_reply(p: &PendingPermission, resp: &PermissionResponse) -> String {
    if resp.decision == event::Decision::Deny {
        return "I do not want to answer that question. Do not proceed with it.".into();
    }
    let mut lines = Vec::new();
    for q in &p.questions {
        if let Some(ans) = lookup_answer(&resp.answers, q) {
            let text = format_permission_answer(&ans);
            if text.is_empty() {
                continue;
            }
            let label = first_non_empty(&[&q.header, &q.question, &q.id]);
            if label.is_empty() {
                lines.push(text);
            } else {
                lines.push(format!("{}: {}", label, text));
            }
        }
    }
    if lines.is_empty() {
        for answer in resp.answers.values() {
            let text = format_permission_answer(answer);
            if !text.is_empty() {
                lines.push(text);
            }
        }
    }
    if lines.is_empty() {
        "Please continue.".into()
    } else {
        lines.join("\n")
    }
}

fn lookup_answer(
    answers: &BTreeMap<String, PermissionAnswer>,
    q: &PermissionQuestion,
) -> Option<PermissionAnswer> {
    for key in [&q.id, &q.header, &q.question] {
        if !key.is_empty() {
            if let Some(v) = answers.get(key) {
                return Some(v.clone());
            }
        }
    }
    None
}

fn format_permission_answer(a: &PermissionAnswer) -> String {
    if !a.text.is_empty() {
        return a.text.trim().into();
    }
    a.answers.join(", ")
}

fn project_questions(input: Option<&Value>) -> Vec<PermissionQuestion> {
    let Some(m) = input.and_then(|v| v.as_object()) else {
        return Vec::new();
    };
    let Some(raw_qs) = m.get("questions").and_then(|v| v.as_array()) else {
        return Vec::new();
    };
    let mut out = Vec::new();
    for rq in raw_qs {
        let Some(o) = rq.as_object() else { continue };
        let str_of = |keys: &[&str]| -> String {
            for k in keys {
                if let Some(s) = o.get(*k).and_then(|v| v.as_str()) {
                    if !s.is_empty() {
                        return s.to_string();
                    }
                }
            }
            String::new()
        };
        let bool_of = |keys: &[&str]| -> bool {
            for k in keys {
                if let Some(b) = o.get(*k).and_then(|v| v.as_bool()) {
                    if b {
                        return true;
                    }
                }
            }
            false
        };
        let mut q = PermissionQuestion {
            id: str_of(&["id"]),
            header: str_of(&["header", "title"]),
            question: str_of(&["question", "prompt"]),
            multi_select: bool_of(&["multi_select", "multiSelect"]),
            is_other: bool_of(&["is_other", "isOther"]),
            is_secret: bool_of(&["is_secret", "isSecret"]),
            options: Vec::new(),
        };
        if let Some(opts) = o.get("options").and_then(|v| v.as_array()) {
            for ro in opts {
                let Some(oo) = ro.as_object() else { continue };
                let label = ["label", "value"]
                    .iter()
                    .find_map(|k| oo.get(*k).and_then(|v| v.as_str()))
                    .unwrap_or("")
                    .to_string();
                if label.is_empty() {
                    continue;
                }
                q.options.push(PermissionQuestionOption {
                    label,
                    description: oo
                        .get("description")
                        .and_then(|v| v.as_str())
                        .unwrap_or("")
                        .to_string(),
                });
            }
        }
        if q.header.is_empty() && q.question.is_empty() && q.options.is_empty() {
            continue;
        }
        out.push(q);
    }
    out
}

// ——— wire encoders ———

fn encode_user_text(text: &str) -> Result<Vec<OutFrame>> {
    if text.is_empty() {
        return Err(LucarneError::dialect("claude: empty prompt"));
    }
    let payload = serde_json::json!({
        "type": "user",
        "message": {
            "role": "user",
            "content": [{"type": "text", "text": text}],
        },
    });
    let mut line = serde_json::to_vec(&payload)?;
    line.push(b'\n');
    Ok(vec![OutFrame::Stdin(line)])
}

fn encode_claude_official_command(command: &str, args: Option<&str>) -> Result<Vec<OutFrame>> {
    let mut text = format!("/{command}");
    if let Some(args) = args.map(str::trim).filter(|value| !value.is_empty()) {
        text.push(' ');
        text.push_str(args);
    }
    encode_user_text(&text)
}

fn build_initialize_catalog(result: Option<&Value>, revision: u64) -> Option<AgentCommandCatalog> {
    let commands = result?
        .get("commands")
        .and_then(Value::as_array)?
        .iter()
        .filter_map(initialize_command_to_agent_command)
        .collect::<Vec<_>>();
    (!commands.is_empty()).then_some(AgentCommandCatalog {
        commands,
        complete: true,
        revision,
    })
}

fn parse_initialize_models(result: Option<&Value>) -> Option<Vec<ClaudeModelInfo>> {
    let models = result?
        .get("models")
        .and_then(Value::as_array)?
        .iter()
        .filter_map(parse_model_info)
        .collect::<Vec<_>>();
    (!models.is_empty()).then_some(models)
}

fn parse_model_info(model: &Value) -> Option<ClaudeModelInfo> {
    let value = model.get("value").and_then(Value::as_str)?.trim();
    if value.is_empty() {
        return None;
    }
    let supported_effort_levels = model
        .get("supportedEffortLevels")
        .or_else(|| model.get("supported_effort_levels"))
        .and_then(Value::as_array)
        .map(|levels| {
            levels
                .iter()
                .filter_map(Value::as_str)
                .filter(|level| !level.trim().is_empty())
                .map(str::to_string)
                .collect::<Vec<_>>()
        })
        .unwrap_or_default();
    Some(ClaudeModelInfo {
        value: value.to_string(),
        display_name: model
            .get("displayName")
            .or_else(|| model.get("display_name"))
            .and_then(Value::as_str)
            .unwrap_or(value)
            .to_string(),
        description: model
            .get("description")
            .and_then(Value::as_str)
            .unwrap_or("")
            .to_string(),
        supported_effort_levels,
    })
}

fn claude_model_catalog(current_model: &str, models: &[ClaudeModelInfo]) -> AgentModelCatalog {
    AgentModelCatalog {
        current_model: (!current_model.trim().is_empty()).then(|| current_model.into()),
        current_reasoning: None,
        models: models
            .iter()
            .map(|model| AgentModelOption {
                id: model.value.as_str().into(),
                display_name: (!model.display_name.is_empty() && model.display_name != model.value)
                    .then(|| model.display_name.clone().into()),
                description: (!model.description.is_empty())
                    .then(|| model.description.clone().into()),
                supported_reasoning: model
                    .supported_effort_levels
                    .iter()
                    .map(|level| AgentReasoningOption {
                        value: level.as_str().into(),
                        description: None,
                        is_default: None,
                    })
                    .collect(),
            })
            .collect(),
    }
}

fn initialize_command_to_agent_command(command: &Value) -> Option<AgentCommand> {
    let name = command
        .get("name")
        .and_then(Value::as_str)
        .map(normalize_command_name)
        .filter(|name| !name.is_empty())?;
    let description = command
        .get("description")
        .and_then(Value::as_str)
        .filter(|description| !description.is_empty())
        .map(Into::into);
    let argument_hint = command
        .get("argumentHint")
        .or_else(|| command.get("argument_hint"))
        .and_then(Value::as_str)
        .unwrap_or("");
    let aliases = initialize_command_aliases(command, name);
    Some(AgentCommand {
        name: name.into(),
        description,
        aliases,
        source: AgentCommandSource::ProviderNative,
        input: if argument_hint.is_empty() {
            AgentCommandInput::None
        } else {
            AgentCommandInput::Text {
                label: argument_hint.into(),
                required: command_argument_hint_required(argument_hint),
            }
        },
        completion: AgentCommandCompletion::ProviderIdle,
    })
}

fn initialize_command_aliases(command: &Value, name: &str) -> Vec<SmolStr> {
    let Some(aliases) = command.get("aliases").and_then(Value::as_array) else {
        return Vec::new();
    };
    let mut out = Vec::new();
    for alias in aliases.iter().filter_map(Value::as_str) {
        let alias = normalize_command_name(alias);
        if alias.is_empty()
            || alias == name
            || alias.contains(char::is_whitespace)
            || alias.contains('/')
            || out
                .iter()
                .any(|existing: &SmolStr| existing.as_str() == alias)
        {
            continue;
        }
        out.push(alias.into());
    }
    out
}

fn command_argument_hint_required(argument_hint: &str) -> bool {
    let trimmed = argument_hint.trim();
    let lower = trimmed.to_ascii_lowercase();
    trimmed.contains('<') && trimmed.contains('>') && !lower.contains("optional")
}

fn validate_reasoning_effort(value: &str) -> Result<String> {
    if ["low", "medium", "high", "xhigh", "max"].contains(&value) {
        Ok(value.to_string())
    } else {
        Err(LucarneError::dialect(format!(
            "claude: unsupported reasoning effort {value:?}; expected one of low, medium, high, xhigh, max"
        )))
    }
}

fn claude_permission_mode_to_provider(mode: PermissionMode) -> &'static str {
    match mode {
        PermissionMode::Default => "default",
        PermissionMode::Write => "acceptEdits",
        PermissionMode::ReadOnly => "plan",
        PermissionMode::Auto => "auto",
        PermissionMode::Full | PermissionMode::Bypass => "bypassPermissions",
    }
}

fn claude_permission_mode_from_provider(raw: &str) -> Option<PermissionMode> {
    match raw.trim() {
        "" | "default" => Some(PermissionMode::Default),
        "acceptEdits" => Some(PermissionMode::Write),
        "plan" => Some(PermissionMode::ReadOnly),
        "auto" | "dontAsk" => Some(PermissionMode::Auto),
        "bypassPermissions" => Some(PermissionMode::Full),
        _ => None,
    }
}

fn claude_permission_catalog(
    result: Option<&Value>,
    current_mode: PermissionMode,
) -> AgentPermissionCatalog {
    let current = result
        .and_then(|value| value.pointer("/effective/permissionMode"))
        .and_then(Value::as_str)
        .filter(|value| !value.trim().is_empty())
        .unwrap_or_else(|| claude_permission_mode_to_provider(current_mode));
    AgentPermissionCatalog {
        current_mode: Some(current.into()),
        modes: CLAUDE_PERMISSION_MODES
            .iter()
            .map(|mode| AgentPermissionOption {
                id: (*mode).into(),
                display_name: None,
                description: None,
            })
            .collect(),
    }
}

fn claude_skill_catalog(result: Option<&Value>) -> AgentSkillCatalog {
    let frontmatter = result
        .and_then(|value| value.pointer("/skills/skillFrontmatter"))
        .and_then(Value::as_array);
    if let Some(frontmatter) = frontmatter {
        let mut skills = Vec::new();
        for skill in frontmatter {
            let Some(name) = skill.get("name").and_then(Value::as_str) else {
                continue;
            };
            let source = skill.get("source").and_then(Value::as_str).unwrap_or("");
            let tokens = skill.get("tokens").and_then(Value::as_u64);
            skills.push(AgentSkillSummary {
                name: name.into(),
                display_name: None,
                description: None,
                path: None,
                scope: None,
                source: (!source.is_empty()).then(|| source.into()),
                tokens,
                enabled: None,
            });
        }
        return AgentSkillCatalog { skills };
    }
    AgentSkillCatalog::default()
}

fn claude_status(
    session_id: &str,
    cwd: &str,
    current_model: &str,
    current_reasoning_effort: Option<&str>,
    current_permission_mode: PermissionMode,
    settings: Option<&Value>,
    context_usage: Option<&Value>,
    latest_usage: Option<&Usage>,
    account: Option<&Value>,
    cli_version: Option<&str>,
) -> AgentStatus {
    let model = claude_setting_str(settings, &["/effective/model", "/applied/model", "/model"])
        .or_else(|| claude_setting_str(context_usage, &["/model"]))
        .or_else(|| (!current_model.trim().is_empty()).then_some(current_model));
    let model_detail = claude_setting_str(settings, &["/applied/model"])
        .or_else(|| claude_setting_str(context_usage, &["/model"]))
        .filter(|detail| model != Some(*detail));
    let reasoning = claude_setting_str(
        settings,
        &[
            "/effective/effort",
            "/applied/effort",
            "/effective/reasoningEffort",
            "/applied/reasoningEffort",
        ],
    )
    .or(current_reasoning_effort);
    let permissions = claude_setting_str(
        settings,
        &[
            "/effective/permissionMode",
            "/applied/permissionMode",
            "/permissionMode",
        ],
    )
    .unwrap_or_else(|| claude_permission_mode_to_provider(current_permission_mode));
    let directory = claude_setting_str(settings, &["/cwd", "/currentWorkingDirectory"])
        .or_else(|| (!cwd.trim().is_empty()).then_some(cwd));
    AgentStatus {
        version: cli_version
            .or_else(|| {
                claude_setting_str(settings, &["/version", "/claudeVersion", "/cliVersion"])
            })
            .map(Into::into),
        session_id: (!session_id.trim().is_empty()).then(|| session_id.into()),
        directory: directory.map(Into::into),
        model: model.map(Into::into),
        model_detail: model_detail.map(Into::into),
        reasoning: reasoning.map(Into::into),
        permissions: Some(permissions.into()),
        account: claude_status_account(settings, account),
        base_url: claude_status_base_url(settings),
        proxy: claude_status_proxy(),
        setting_sources: claude_status_setting_sources(settings),
        tokens: claude_status_tokens(context_usage, latest_usage),
        context: claude_status_context(context_usage),
        compactions: claude_status_compactions(context_usage),
        ..Default::default()
    }
}

fn claude_status_account(settings: Option<&Value>, account: Option<&Value>) -> Option<SmolStr> {
    let token_source =
        claude_setting_str(account, &["/tokenSource", "/token_source"]).or_else(|| {
            claude_settings_env_str(settings, "ANTHROPIC_AUTH_TOKEN")
                .map(|_| "ANTHROPIC_AUTH_TOKEN")
        });
    let provider = claude_setting_str(account, &["/apiProvider", "/api_provider"]);
    match (token_source, provider) {
        (Some(token_source), Some(provider)) => Some(format!("{token_source} ({provider})").into()),
        (Some(token_source), None) => Some(token_source.into()),
        (None, Some(provider)) => Some(provider.into()),
        (None, None) => None,
    }
}

fn claude_status_base_url(settings: Option<&Value>) -> Option<SmolStr> {
    claude_settings_env_str(settings, "ANTHROPIC_BASE_URL").map(Into::into)
}

fn claude_status_proxy() -> Option<SmolStr> {
    [
        "http_proxy",
        "https_proxy",
        "all_proxy",
        "HTTP_PROXY",
        "HTTPS_PROXY",
        "ALL_PROXY",
    ]
    .iter()
    .find_map(|key| {
        std::env::var(key)
            .ok()
            .map(|value| value.trim().to_string())
            .filter(|value| !value.is_empty())
    })
    .map(Into::into)
}

fn claude_status_setting_sources(settings: Option<&Value>) -> Option<SmolStr> {
    let sources = settings?
        .get("sources")
        .and_then(Value::as_array)?
        .iter()
        .filter_map(|source| source.get("source").and_then(Value::as_str))
        .map(claude_setting_source_label)
        .collect::<Vec<_>>();
    (!sources.is_empty()).then(|| sources.join(", ").into())
}

fn claude_setting_source_label(source: &str) -> String {
    match source {
        "userSettings" => "User settings".into(),
        "projectSettings" => "Project settings".into(),
        "localSettings" => "Local settings".into(),
        value => value.into(),
    }
}

fn claude_settings_env_str<'a>(settings: Option<&'a Value>, key: &str) -> Option<&'a str> {
    let settings = settings?;
    settings
        .pointer(&format!("/effective/env/{key}"))
        .or_else(|| settings.pointer(&format!("/applied/env/{key}")))
        .or_else(|| settings.pointer(&format!("/env/{key}")))
        .and_then(Value::as_str)
        .filter(|value| !value.trim().is_empty())
}

fn claude_setting_str<'a>(value: Option<&'a Value>, pointers: &[&str]) -> Option<&'a str> {
    let value = value?;
    pointers.iter().find_map(|pointer| {
        value
            .pointer(pointer)
            .and_then(Value::as_str)
            .filter(|s| !s.trim().is_empty())
    })
}

fn claude_status_tokens(
    context_usage: Option<&Value>,
    latest_usage: Option<&Usage>,
) -> Option<AgentTokenUsage> {
    let raw = context_usage
        .and_then(|value| value.pointer("/apiUsage"))
        .filter(|value| !value.is_null())
        .or_else(|| context_usage.and_then(|value| value.pointer("/usage")));
    if let Some(raw) = raw {
        let input = u64_from_value(raw, &["input_tokens", "inputTokens"]);
        let output = u64_from_value(raw, &["output_tokens", "outputTokens"]);
        let total = u64_from_value(raw, &["total_tokens", "totalTokens"]);
        if input.is_some() || output.is_some() || total.is_some() {
            return Some(AgentTokenUsage {
                input_tokens: input,
                output_tokens: output,
                total_tokens: total,
            });
        }
    }
    latest_usage.map(|usage| AgentTokenUsage {
        input_tokens: non_negative_u64(usage.input_tokens),
        output_tokens: non_negative_u64(usage.output_tokens),
        total_tokens: non_negative_u64(
            usage.input_tokens
                + usage.output_tokens
                + usage.cache_read_tokens
                + usage.cache_write_tokens,
        ),
    })
}

fn claude_status_context(context_usage: Option<&Value>) -> Option<AgentContextUsage> {
    let root = context_usage?;
    let context = root
        .pointer("/context")
        .or_else(|| root.pointer("/contextUsage"))
        .unwrap_or(root);
    let used = u64_from_value(
        context,
        &[
            "used_tokens",
            "usedTokens",
            "current_tokens",
            "currentTokens",
            "context_tokens",
            "contextTokens",
            "total_tokens",
            "totalTokens",
        ],
    );
    let max = u64_from_value(
        context,
        &[
            "max_tokens",
            "maxTokens",
            "context_window",
            "contextWindow",
            "model_context_window",
            "modelContextWindow",
        ],
    );
    if used.is_none() && max.is_none() {
        return None;
    }
    let percent = u64_from_value(
        context,
        &["percent_used", "percentUsed", "percentage", "percent"],
    )
    .and_then(|value| u8::try_from(value).ok())
    .or_else(|| match (used, max) {
        (Some(used), Some(max)) if max > 0 => {
            Some(((used as f64 / max as f64) * 100.0).round() as u8)
        }
        _ => None,
    });
    Some(AgentContextUsage {
        used_tokens: used,
        max_tokens: max,
        percent_used: percent,
    })
}

fn claude_status_compactions(context_usage: Option<&Value>) -> Option<u64> {
    let root = context_usage?;
    u64_from_value(root, &["compactions"]).or_else(|| {
        root.pointer("/context")
            .and_then(|context| u64_from_value(context, &["compactions"]))
    })
}

fn u64_from_value(value: &Value, keys: &[&str]) -> Option<u64> {
    keys.iter()
        .find_map(|key| value.get(*key).and_then(Value::as_u64))
}

fn non_negative_u64(value: i64) -> Option<u64> {
    u64::try_from(value).ok()
}

fn compact_list_text(value: &str) -> String {
    const MAX_CHARS: usize = 160;
    let compact = value.split_whitespace().collect::<Vec<_>>().join(" ");
    if compact.chars().count() <= MAX_CHARS {
        return compact;
    }
    let mut shortened = compact.chars().take(MAX_CHARS).collect::<String>();
    shortened.truncate(shortened.trim_end().len());
    format!("{shortened}...")
}

fn claude_setting_matches(result: Option<&Value>, pointer: &str, expected: &str) -> bool {
    result
        .and_then(|value| value.pointer(pointer))
        .and_then(Value::as_str)
        == Some(expected)
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
    let Some(catalog_command) = catalog
        .commands
        .iter()
        .find(|cmd| command_supports_name(cmd, name))
    else {
        return Err(LucarneError::dialect(format!(
            "{provider}: unsupported command {:?}",
            command.name
        )));
    };
    let args = command.args.as_deref().unwrap_or("").trim();
    if args.is_empty() && command_requires_arguments(catalog_command) {
        return Err(LucarneError::dialect(format!(
            "{provider}: command /{name} requires arguments"
        )));
    }
    if args.is_empty() {
        Ok(format!("/{name}"))
    } else {
        Ok(format!("/{name} {args}"))
    }
}

fn command_supports_name(command: &AgentCommand, name: &str) -> bool {
    command.name.as_str() == name || command.aliases.iter().any(|alias| alias.as_str() == name)
}

fn command_requires_arguments(command: &AgentCommand) -> bool {
    matches!(
        command.input,
        AgentCommandInput::Text { required: true, .. }
    )
}

fn normalize_command_name(raw: &str) -> &str {
    raw.trim().trim_start_matches('/')
}

fn input_fork_prompt_text(input: &Input) -> String {
    let mut parts = Vec::new();
    for (idx, image) in input.images.iter().enumerate() {
        if image.media_type.trim().is_empty() || image.data.is_empty() {
            continue;
        }
        parts.push(format!("[Image #{}]", idx + 1));
    }
    if !input.text.trim().is_empty() {
        parts.push(input.text.trim().to_string());
    }
    parts.join(" ")
}

fn encode_user_input(input: &Input) -> Result<Vec<OutFrame>> {
    use base64::Engine;

    let mut content = Vec::new();
    for (idx, image) in input.images.iter().enumerate() {
        if image.media_type.trim().is_empty() || image.data.is_empty() {
            continue;
        }
        content.push(serde_json::json!({
            "type": "text",
            "text": format!("[Image #{}]", idx + 1),
        }));
        content.push(serde_json::json!({
            "type": "image",
            "source": {
                "type": "base64",
                "media_type": image.media_type,
                "data": base64::engine::general_purpose::STANDARD.encode(&image.data),
            }
        }));
    }
    if !input.text.is_empty() {
        content.push(serde_json::json!({"type": "text", "text": input.text}));
    }
    if content.is_empty() {
        return Err(LucarneError::dialect("claude: empty prompt"));
    }
    let payload = serde_json::json!({
        "type": "user",
        "message": {
            "role": "user",
            "content": content,
        },
    });
    let mut line = serde_json::to_vec(&payload)?;
    line.push(b'\n');
    Ok(vec![OutFrame::Stdin(line)])
}

fn encode_control_response(
    req_id: &str,
    decision: event::Decision,
    updated: Option<Value>,
) -> Result<Vec<OutFrame>> {
    let behavior = if decision == event::Decision::Allow {
        "allow"
    } else {
        "deny"
    };
    let mut response = serde_json::Map::new();
    response.insert("behavior".into(), Value::String(behavior.into()));
    if let Some(Value::Object(m)) = updated {
        if !m.is_empty() {
            response.insert("updatedInput".into(), Value::Object(m));
        }
    }
    let payload = serde_json::json!({
        "type": "control_response",
        "response": {
            "subtype": "success",
            "request_id": req_id,
            "response": Value::Object(response),
        },
    });
    let mut line = serde_json::to_vec(&payload)?;
    line.push(b'\n');
    Ok(vec![OutFrame::Stdin(line)])
}

// ——— small utils ———

fn tl(item: TimelineItem) -> Event {
    Event::new(Payload::Timeline(Timeline { item }))
}

fn non_empty_str(value: &str) -> Option<&str> {
    let value = value.trim();
    (!value.is_empty()).then_some(value)
}

const CLAUDE_FORK_REF_PREFIX: &str = "claude-fork:v1:";

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct ClaudeForkRef {
    pub(crate) source_session_id: String,
    pub(crate) resume_session_at: String,
}

pub(crate) fn encode_claude_fork_ref(
    source_session_id: &str,
    resume_session_at: &str,
) -> Option<String> {
    let source_session_id = source_session_id.trim();
    let resume_session_at = resume_session_at.trim();
    if source_session_id.is_empty() || resume_session_at.is_empty() {
        return None;
    }
    Some(format!(
        "{CLAUDE_FORK_REF_PREFIX}{}:{}",
        URL_SAFE_NO_PAD.encode(source_session_id.as_bytes()),
        URL_SAFE_NO_PAD.encode(resume_session_at.as_bytes())
    ))
}

pub(crate) fn decode_claude_fork_ref(value: &str) -> Option<ClaudeForkRef> {
    let value = value.strip_prefix(CLAUDE_FORK_REF_PREFIX)?;
    let (source, target) = value.split_once(':')?;
    let source_session_id = String::from_utf8(URL_SAFE_NO_PAD.decode(source).ok()?).ok()?;
    let resume_session_at = String::from_utf8(URL_SAFE_NO_PAD.decode(target).ok()?).ok()?;
    (!source_session_id.trim().is_empty() && !resume_session_at.trim().is_empty()).then_some(
        ClaudeForkRef {
            source_session_id,
            resume_session_at,
        },
    )
}

fn first_non_empty(xs: &[&str]) -> String {
    for s in xs {
        let t = s.trim();
        if !t.is_empty() {
            return t.to_string();
        }
    }
    String::new()
}

fn is_max_turns_result(subtype: &str, stop_reason: &str, result: &str) -> bool {
    if subtype.trim().eq_ignore_ascii_case("error_max_turns") {
        return true;
    }
    if stop_reason.trim().eq_ignore_ascii_case("max_turns") {
        return true;
    }
    result.to_ascii_lowercase().contains("max turn")
}

fn first_claude_error(items: &[Value]) -> String {
    for raw in items {
        if let Some(s) = raw.as_str() {
            let s = s.trim();
            if !s.is_empty() {
                return s.to_string();
            }
        }
        if let Some(o) = raw.as_object() {
            for k in ["message", "error", "detail"] {
                if let Some(s) = o.get(k).and_then(|v| v.as_str()) {
                    let s = s.trim();
                    if !s.is_empty() {
                        return s.to_string();
                    }
                }
            }
        }
    }
    String::new()
}

fn has_usage(u: &Usage) -> bool {
    u.input_tokens != 0
        || u.output_tokens != 0
        || u.cache_read_tokens != 0
        || u.cache_write_tokens != 0
        || u.cost_usd != 0.0
}

fn parse_usage(raw: Option<&Value>, fallback_cost: f64) -> Option<Usage> {
    let Some(raw) = raw else {
        return if fallback_cost == 0.0 {
            None
        } else {
            Some(Usage {
                cost_usd: fallback_cost,
                ..Default::default()
            })
        };
    };
    let Some(obj) = raw.as_object() else {
        // Go: `if err := json.Unmarshal(raw, &usage); err != nil { ... }`
        // When raw is present but not a valid usage object, fall back.
        return if fallback_cost == 0.0 {
            None
        } else {
            Some(Usage {
                cost_usd: fallback_cost,
                ..Default::default()
            })
        };
    };
    let i = |k: &str| obj.get(k).and_then(|v| v.as_i64()).unwrap_or(0);
    let f = |k: &str| obj.get(k).and_then(|v| v.as_f64()).unwrap_or(0.0);
    let first_nonzero_i = |a: i64, b: i64| if a != 0 { a } else { b };
    let first_nonzero_f = |vals: &[f64]| -> f64 {
        for v in vals {
            if *v != 0.0 {
                return *v;
            }
        }
        0.0
    };
    let u = Usage {
        input_tokens: i("input_tokens"),
        output_tokens: i("output_tokens"),
        cache_read_tokens: first_nonzero_i(i("cache_read_tokens"), i("cache_read_input_tokens")),
        cache_write_tokens: first_nonzero_i(
            i("cache_write_tokens"),
            i("cache_creation_input_tokens"),
        ),
        cost_usd: first_nonzero_f(&[f("total_cost_usd"), f("cost_usd"), fallback_cost]),
    };
    if has_usage(&u) {
        Some(u)
    } else {
        None
    }
}

fn resolve_session_id(requested: &str, emitted: &str, failed: bool) -> String {
    if failed && !requested.is_empty() && !emitted.is_empty() && emitted != requested {
        return String::new();
    }
    emitted.to_string()
}

fn risk_of(tool: &str) -> Risk {
    match tool {
        "Bash" | "Write" | "Edit" => Risk::High,
        "WebFetch" => Risk::Medium,
        _ => Risk::Low,
    }
}

fn extract_text(raw: Option<&Value>) -> String {
    let Some(v) = raw else { return String::new() };
    if let Some(s) = v.as_str() {
        return s.to_string();
    }
    if let Some(arr) = v.as_array() {
        let mut out = String::new();
        for it in arr {
            // String elements (Go: `case string:`)
            if let Some(s) = it.as_str() {
                out.push_str(s);
                continue;
            }
            // Object elements — try "text", "content", "output" (Go: flattenTextArray)
            if let Some(obj) = it.as_object() {
                let text = first_string(obj, &["text", "content", "output"]);
                if !text.is_empty() {
                    out.push_str(&text);
                    continue;
                }
                // Non-text object with no recognized field — abort
                return String::new();
            }
            // Unknown element type — abort
            return String::new();
        }
        return out;
    }
    serde_json::to_string(v).unwrap_or_default()
}

fn first_string(o: &serde_json::Map<String, Value>, keys: &[&str]) -> String {
    for k in keys {
        if let Some(s) = o.get(*k).and_then(|v| v.as_str()) {
            if !s.is_empty() {
                return s.to_string();
            }
        }
    }
    String::new()
}
