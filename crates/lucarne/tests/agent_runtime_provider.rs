#![allow(clippy::needless_update, clippy::redundant_closure)]

use lucarne::adapter::{
    ArgProfile, Capabilities, Protocol, ProtocolAdapter, ProtocolOptions, ProtocolSessionParts,
    Spec,
};
use lucarne::agent_runtime::{
    AgentCapabilities, AgentErrorKind, AgentImageInput, AgentInput, AgentProvider, OpenSession,
    ProtocolProvider, ResumeSession, SessionRef,
};
use lucarne::dialect::{Dialect, Input, OutFrame, SessionParams};
use lucarne::event::{self, Event as CanonicalEvent, Payload, SessionStarted, Timeline};
use lucarne::framer::Framer;
use lucarne::launcher::LocalLauncher;
use lucarne::ProviderId;
use serde_json::{json, Value};
use smol_str::SmolStr;
use std::sync::{
    atomic::{AtomicUsize, Ordering},
    Arc, Mutex,
};
use std::time::Duration;

#[tokio::test]
async fn agent_runtime_provider_probe_projects_internal_probe() {
    let adapter = Arc::new(MockAdapter::new(
        Spec {
            id: ProviderId::from_static("gemini"),
            label: "Gemini".into(),
            protocol: Protocol::StdioJsonrpc,
            capabilities: Capabilities {
                thinking: true,
                tool_stream: true,
                usage: true,
                structured_intervention: true,
                command_catalog: true,
                permission_intercept: false,
                ..Default::default()
            },
            ..Default::default()
        },
        lucarne::adapter::ProbeResult {
            available: true,
            version: "1.2.3".into(),
            ..Default::default()
        },
    ));
    let provider = mock_provider(&adapter);

    let probe = provider.probe().await.expect("probe");

    assert_eq!(probe.provider_id, ProviderId::from_static("gemini"));
    assert_eq!(probe.provider_version, Some(SmolStr::new("1.2.3")));
    assert_eq!(
        probe.capabilities,
        AgentCapabilities {
            reasoning_stream: true,
            tool_stream: true,
            usage_reporting: true,
            structured_intervention: true,
            command_catalog: true,
        }
    );
}

#[tokio::test]
async fn agent_runtime_provider_accepts_adapter_owned_provider_id() {
    let adapter = Arc::new(MockAdapter::new(
        Spec {
            id: ProviderId::from_static("qwen"),
            label: "Qwen".into(),
            protocol: Protocol::StdioJsonrpc,
            ..Default::default()
        },
        lucarne::adapter::ProbeResult {
            available: true,
            version: "0.1.0".into(),
            ..Default::default()
        },
    ));
    let provider = ProtocolProvider::new(adapter.protocol_adapter())
        .expect("custom provider id should belong to adapter spec");

    assert_eq!(provider.id(), ProviderId::from_static("qwen"));
    assert_eq!(
        provider.probe().await.expect("probe").provider_id,
        ProviderId::from_static("qwen"),
        "runtime provider construction must not depend on the built-in provider catalog"
    );
}

#[tokio::test]
async fn agent_runtime_provider_open_translates_open_session_args_and_submits_initial_input() {
    let adapter = Arc::new(MockAdapter::new(
        Spec {
            id: ProviderId::from_static("claude"),
            label: "Claude".into(),
            protocol: Protocol::StdioNewlineJson,
            capabilities: Capabilities {
                thinking: true,
                tool_stream: true,
                usage: true,
                structured_intervention: true,
                command_catalog: true,
                permission_intercept: false,
                ..Default::default()
            },
            arg_profile: ArgProfile {
                system_prompt: true,
                resume_session_key: "session_id".into(),
                resume_session_id_hint: true,
                ..Default::default()
            },
            ..Default::default()
        },
        Default::default(),
    ));
    let provider = mock_provider(&adapter);

    let session = provider
        .open(OpenSession {
            model: Some("claude-sonnet-4".into()),
            cwd: Some("/tmp/provider-open".into()),
            initial_input: Some(AgentInput {
                text: "hello".into(),
                images: Vec::new(),
            }),
            idle_timeout_ms: None,
            args: json!({
                "system_prompt": "be concise",
                "extra_env": {
                    "FOO": "BAR"
                },
                "extra_args": ["--debug"]
            }),
        })
        .await
        .expect("open");

    let start = adapter.take_only_start();
    assert_eq!(start.model, "claude-sonnet-4");
    assert_eq!(start.cwd, "/tmp/provider-open");
    assert_eq!(start.system_prompt, "be concise");
    assert_eq!(start.first_prompt, "");
    assert_eq!(start.extra_env.get("FOO").map(String::as_str), Some("BAR"));
    assert_eq!(start.extra_args, vec!["--debug".to_string()]);
    assert_eq!(
        start.permission_mode.as_str(),
        lucarne::dialect::PermissionMode::Default.as_str()
    );

    let mut events = session.take_events().await.expect("take events");
    assert!(matches!(
        events.recv().await,
        Some(lucarne::agent_runtime::Event::Message(message))
            if message.text.as_str() == "submit:hello"
    ));
}

#[tokio::test]
async fn journey_56_session_params_preserve_cwd_model_permissions_across_open() {
    let adapter = Arc::new(MockAdapter::new(
        Spec {
            id: ProviderId::from_static("claude"),
            label: "Claude".into(),
            protocol: Protocol::StdioNewlineJson,
            arg_profile: ArgProfile {
                resume_session_key: "session_id".into(),
                resume_session_id_hint: true,
                ..Default::default()
            },
            ..Default::default()
        },
        Default::default(),
    ));
    let provider = mock_provider(&adapter);

    let _opened = provider
        .open(OpenSession {
            model: Some("gpt-5.5".into()),
            cwd: Some("/tmp/journey-56-open".into()),
            args: json!({
                "permission_mode": "bypass",
                "extra_env": {"LUCARNE_JOURNEY": "56"},
            }),
            ..Default::default()
        })
        .await
        .expect("open with session params");
    let open_start = adapter.take_only_start();
    assert_eq!(open_start.model, "gpt-5.5");
    assert_eq!(open_start.cwd, "/tmp/journey-56-open");
    assert_eq!(
        open_start.permission_mode,
        lucarne::dialect::PermissionMode::Bypass
    );
    assert_eq!(
        open_start
            .extra_env
            .get("LUCARNE_JOURNEY")
            .map(String::as_str),
        Some("56")
    );

    let _resumed = provider
        .resume(ResumeSession {
            session_ref: SessionRef(SmolStr::new("journey-56-session")),
            args: json!({
                "cwd": "/tmp/journey-56-resume",
                "permission_mode": "bypass",
                "extra_env": {"LUCARNE_JOURNEY": "56-resume"},
            }),
            ..Default::default()
        })
        .await
        .expect("resume with session params");
    let resume_start = adapter.take_only_start();
    assert_eq!(resume_start.cwd, "/tmp/journey-56-resume");
    assert_eq!(
        resume_start.resume.as_ref().expect("resume handle").data["session_id"],
        json!("journey-56-session")
    );
    assert_eq!(
        resume_start.resume.as_ref().expect("resume handle").data["cwd"],
        json!("/tmp/journey-56-resume")
    );
    assert_eq!(
        resume_start.permission_mode,
        lucarne::dialect::PermissionMode::Bypass
    );
    assert_eq!(
        resume_start
            .extra_env
            .get("LUCARNE_JOURNEY")
            .map(String::as_str),
        Some("56-resume")
    );
}

#[tokio::test]
async fn agent_runtime_provider_open_submits_multimodal_initial_input() {
    let adapter = Arc::new(MockAdapter::new(
        Spec {
            id: ProviderId::from_static("claude"),
            label: "Claude".into(),
            protocol: Protocol::StdioNewlineJson,
            ..Default::default()
        },
        Default::default(),
    ));
    let provider = mock_provider(&adapter);

    let session = provider
        .open(OpenSession {
            initial_input: Some(AgentInput {
                text: "inspect".into(),
                images: vec![AgentImageInput {
                    media_type: "image/png".into(),
                    data_base64: "AQID".into(),
                }],
            }),
            ..Default::default()
        })
        .await
        .expect("open");

    let start = adapter.take_only_start();
    assert_eq!(start.first_prompt, "");

    let mut events = session.take_events().await.expect("take events");
    assert!(matches!(
        events.recv().await,
        Some(lucarne::agent_runtime::Event::Message(message))
            if message.text.as_str() == "submit:inspect|images=image/png:3"
    ));
}

#[tokio::test]
async fn agent_runtime_provider_resume_translates_session_ref_to_internal_resume_handle() {
    let adapter = Arc::new(MockAdapter::new(
        Spec {
            id: ProviderId::from_static("codex"),
            label: "Codex".into(),
            protocol: Protocol::StdioJsonrpc,
            capabilities: Capabilities {
                thinking: true,
                tool_stream: true,
                usage: true,
                structured_intervention: true,
                command_catalog: true,
                permission_intercept: false,
                ..Default::default()
            },
            arg_profile: ArgProfile {
                resume_session_key: "thread_id".into(),
                ..Default::default()
            },
            ..Default::default()
        },
        Default::default(),
    ));
    let provider = mock_provider(&adapter);

    let _session = provider
        .resume(ResumeSession {
            session_ref: SessionRef(SmolStr::new("thread-123")),
            idle_timeout_ms: None,
            args: json!({
                "cwd": "/tmp/provider-resume",
                "extra_env": {"OPENAI_API_KEY": "test"},
                "extra_args": ["--profile", "fast"]
            }),
        })
        .await
        .expect("resume");

    let start = adapter.take_only_start();
    assert_eq!(start.first_prompt, "");
    assert_eq!(
        start.extra_env.get("OPENAI_API_KEY").map(String::as_str),
        Some("test")
    );
    let data = &start.resume.as_ref().expect("resume handle").data;
    assert_eq!(data["thread_id"], json!("thread-123"));
    assert_eq!(data["cwd"], json!("/tmp/provider-resume"));
}

#[tokio::test(start_paused = true)]
async fn agent_runtime_provider_resume_session_id_hint_survives_provider_ready_timeout() {
    let adapter = Arc::new(NoReadyAdapter::new(Spec {
        id: ProviderId::from_static("codex"),
        label: "Codex".into(),
        protocol: Protocol::StdioJsonrpc,
        arg_profile: ArgProfile {
            resume_session_key: "thread_id".into(),
            resume_session_id_hint: true,
            ..Default::default()
        },
        ..Default::default()
    }));
    let provider = ProtocolProvider::new(adapter.protocol_adapter()).expect("provider");
    let task = tokio::spawn(async move {
        provider
            .resume(ResumeSession {
                session_ref: SessionRef(SmolStr::new("019e3925-5892-7163-bc90-553af11c982c")),
                idle_timeout_ms: None,
                args: json!({}),
            })
            .await
    });

    tokio::task::yield_now().await;
    tokio::time::advance(Duration::from_secs(5)).await;
    let session = task.await.expect("resume task").expect("resume");

    assert_eq!(
        session.id().0.as_str(),
        "019e3925-5892-7163-bc90-553af11c982c"
    );
    assert_eq!(
        session
            .provider_session_id()
            .await
            .map(|id| id.0.to_string()),
        Some("019e3925-5892-7163-bc90-553af11c982c".to_string())
    );
    session.close().await.expect("close session");
}

#[tokio::test]
async fn agent_runtime_provider_resume_uses_adapter_arg_profile_not_provider_name() {
    let adapter = Arc::new(MockAdapter::new(
        Spec {
            id: ProviderId::from_static("codex"),
            label: "Codex".into(),
            protocol: Protocol::StdioJsonrpc,
            arg_profile: ArgProfile {
                resume_session_key: "session_id".into(),
                ..Default::default()
            },
            ..Default::default()
        },
        Default::default(),
    ));
    let provider = mock_provider(&adapter);

    let _session = provider
        .resume(ResumeSession {
            session_ref: SessionRef(SmolStr::new("session-from-profile")),
            idle_timeout_ms: None,
            args: json!({}),
        })
        .await
        .expect("resume");

    let start = adapter.take_only_start();
    let data = &start.resume.as_ref().expect("resume handle").data;
    assert_eq!(data["session_id"], json!("session-from-profile"));
    assert!(data.get("thread_id").is_none());
}

#[tokio::test]
async fn agent_runtime_provider_resume_waits_for_provider_started_session_id_for_claude() {
    let adapter = Arc::new(MockAdapter::new(
        Spec {
            id: ProviderId::from_static("claude"),
            label: "Claude".into(),
            protocol: Protocol::StdioNewlineJson,
            capabilities: Capabilities {
                thinking: true,
                tool_stream: true,
                usage: true,
                structured_intervention: true,
                command_catalog: true,
                permission_intercept: false,
                ..Default::default()
            },
            ..Default::default()
        },
        Default::default(),
    ));
    let provider = mock_provider(&adapter);

    let session = provider
        .resume(ResumeSession {
            session_ref: SessionRef(SmolStr::new("sess-previous")),
            idle_timeout_ms: None,
            args: json!({
                "cwd": "/tmp/provider-resume",
                "extra_env": {"LUCARNE_FIXTURE": "resume.fixture"},
            }),
        })
        .await
        .expect("resume");

    assert_eq!(session.id().0.as_str(), "provider-session");

    let start = adapter.take_only_start();
    assert_eq!(
        start.resume.as_ref().expect("resume handle").data["session_id"],
        json!("sess-previous")
    );
}

#[tokio::test]
async fn agent_runtime_provider_claude_approval_resolve_does_not_restart_session() {
    let adapter = Arc::new(FailingClaudeRecoveryAdapter::default());
    let provider = claude_recovery_provider(&adapter);

    let session = provider
        .open(OpenSession {
            cwd: Some(
                std::env::current_dir()
                    .expect("current dir")
                    .display()
                    .to_string()
                    .into(),
            ),
            initial_input: Some(AgentInput {
                text: "trigger-approval".into(),
                images: Vec::new(),
            }),
            ..Default::default()
        })
        .await
        .expect("open");

    let mut events = session.take_events().await.expect("take events");
    let req_id = loop {
        match events.recv().await {
            Some(lucarne::agent_runtime::Event::InterventionRequest(
                lucarne::agent_runtime::InterventionRequest::Approval(request),
            )) => break request.req_id.to_string(),
            Some(_) => {}
            None => panic!("event stream closed before approval request"),
        }
    };

    session
        .resolve(
            &req_id,
            lucarne::agent_runtime::InterventionResponse::Approval(
                lucarne::agent_runtime::ApprovalDecision::Allow,
            ),
        )
        .await
        .expect("approval resolve should reach the wrapped session directly");
    assert_eq!(
        adapter.starts.load(Ordering::Relaxed),
        1,
        "approval resolution must not restart the provider session"
    );

    assert!(matches!(
        tokio::time::timeout(Duration::from_secs(1), events.recv())
            .await
            .expect("resolved event"),
        Some(lucarne::agent_runtime::Event::Reasoning(reasoning))
            if reasoning.text.as_str() == "resolved:allow"
    ));
}

#[tokio::test]
async fn journey_57_capabilities_single_descriptor_drives_runtime_and_ui() {
    let cases = [
        (
            Capabilities {
                thinking: true,
                tool_stream: true,
                usage: true,
                structured_intervention: true,
                command_catalog: true,
                permission_intercept: true,
                ..Default::default()
            },
            AgentCapabilities {
                reasoning_stream: true,
                tool_stream: true,
                usage_reporting: true,
                structured_intervention: true,
                command_catalog: true,
            },
        ),
        (
            Capabilities {
                thinking: false,
                tool_stream: false,
                usage: false,
                structured_intervention: false,
                command_catalog: false,
                permission_intercept: true,
                ..Default::default()
            },
            AgentCapabilities {
                reasoning_stream: false,
                tool_stream: false,
                usage_reporting: false,
                structured_intervention: false,
                command_catalog: false,
            },
        ),
    ];

    for (capabilities, expected) in cases {
        let adapter = Arc::new(MockAdapter::new(
            Spec {
                id: ProviderId::from_static("claude"),
                label: "Claude".into(),
                protocol: Protocol::StdioNewlineJson,
                capabilities,
                ..Default::default()
            },
            lucarne::adapter::ProbeResult {
                available: true,
                version: "journey-57".into(),
                ..Default::default()
            },
        ));
        let provider = mock_provider(&adapter);

        let probe = provider.probe().await.expect("probe");
        assert_eq!(probe.capabilities, expected);
    }
}

#[tokio::test]
async fn journey_59_descriptor_open_resume_uses_direct_provider_path() {
    let adapter = Arc::new(MockAdapter::new(
        Spec {
            id: ProviderId::from_static("codex"),
            label: "Codex".into(),
            protocol: Protocol::StdioJsonrpc,
            arg_profile: ArgProfile {
                resume_session_key: "thread_id".into(),
                ..Default::default()
            },
            ..Default::default()
        },
        Default::default(),
    ));
    let provider = mock_provider(&adapter);

    let opened = provider
        .open(OpenSession {
            cwd: Some("/tmp/journey-59-open".into()),
            initial_input: Some(AgentInput {
                text: "journey 59 open".into(),
                images: Vec::new(),
            }),
            ..Default::default()
        })
        .await
        .expect("open through provider path");
    assert_eq!(opened.provider_id(), ProviderId::from_static("codex"));
    assert_eq!(opened.id().0.as_str(), "provider-session");
    let open_start = adapter.take_only_start();
    assert_eq!(open_start.cwd, "/tmp/journey-59-open");

    let resumed = provider
        .resume(ResumeSession {
            session_ref: SessionRef(SmolStr::new("journey-59-thread")),
            args: json!({"cwd": "/tmp/journey-59-resume"}),
            ..Default::default()
        })
        .await
        .expect("resume through provider path");
    assert_eq!(resumed.provider_id(), ProviderId::from_static("codex"));
    assert_eq!(resumed.id().0.as_str(), "provider-session");
    let resume_start = adapter.take_only_start();
    assert_eq!(resume_start.cwd, "/tmp/journey-59-resume");
    assert_eq!(
        resume_start.resume.as_ref().expect("resume handle").data["thread_id"],
        json!("journey-59-thread")
    );
}

#[tokio::test]
async fn agent_runtime_provider_rejects_empty_provider_ids_and_invalid_arg_payloads() {
    let empty_adapter = Arc::new(MockAdapter::new(
        Spec {
            id: ProviderId::from_static(" "),
            label: "Empty".into(),
            protocol: Protocol::StdioJsonrpc,
            ..Default::default()
        },
        Default::default(),
    ));
    let empty_id = match ProtocolProvider::new(empty_adapter.protocol_adapter()) {
        Ok(_) => panic!("empty provider id should fail"),
        Err(err) => err,
    };
    assert_eq!(empty_id.kind, AgentErrorKind::Unsupported);

    let adapter = Arc::new(MockAdapter::new(
        Spec {
            id: ProviderId::from_static("claude"),
            label: "Claude".into(),
            protocol: Protocol::StdioNewlineJson,
            ..Default::default()
        },
        Default::default(),
    ));
    let provider = mock_provider(&adapter);
    let invalid = match provider
        .open(OpenSession {
            args: json!(["not", "an", "object"]),
            ..Default::default()
        })
        .await
    {
        Ok(_) => panic!("invalid args should fail"),
        Err(err) => err,
    };
    assert_eq!(invalid.kind, AgentErrorKind::Unsupported);

    let unknown_field = match provider
        .open(OpenSession {
            args: json!({
                "extra_env": {"FOO": "BAR"},
                "unknown_field": true
            }),
            ..Default::default()
        })
        .await
    {
        Ok(_) => panic!("unknown V1 args should fail"),
        Err(err) => err,
    };
    assert_eq!(unknown_field.kind, AgentErrorKind::Unsupported);

    let start = provider
        .open(OpenSession {
            args: json!({
                "extra_env": {"FOO": "BAR"},
                "permission_mode": "write"
            }),
            ..Default::default()
        })
        .await
        .expect("canonical permission_mode should succeed");
    drop(start);

    let start = adapter.take_only_start();
    assert_eq!(
        start.permission_mode,
        lucarne::dialect::PermissionMode::Write
    );
}

struct MockAdapter {
    spec: Spec,
    probe: lucarne::adapter::ProbeResult,
    starts: Mutex<Vec<SessionParams>>,
}

impl MockAdapter {
    fn new(spec: Spec, probe: lucarne::adapter::ProbeResult) -> Self {
        Self {
            spec,
            probe,
            starts: Mutex::new(Vec::new()),
        }
    }

    fn take_only_start(&self) -> SessionParams {
        let mut starts = self.starts.lock().expect("starts lock");
        assert_eq!(starts.len(), 1, "expected exactly one start request");
        starts.remove(0)
    }

    fn protocol_adapter(self: &Arc<Self>) -> Arc<ProtocolAdapter> {
        let probe_owner = Arc::clone(self);
        let prepare_owner = Arc::clone(self);
        Arc::new(ProtocolAdapter::new(ProtocolOptions {
            spec: self.spec.clone(),
            binary: "/bin/sh".into(),
            launcher: Some(Arc::new(LocalLauncher::new())),
            framer: Some(Framer::newline_json()),
            dialect_factory: Arc::new(|| Box::new(EchoDialect::default())),
            build_args: None,
            build_session: Some(Arc::new(move |_req, _files, launcher| {
                Ok(ProtocolSessionParts {
                    launcher,
                    args: vec!["-c".into(), "cat".into()],
                    dialect: Box::new(EchoDialect::default()),
                })
            })),
            prepare_start: Some(Arc::new(move |req, binary| {
                prepare_owner
                    .starts
                    .lock()
                    .expect("starts lock")
                    .push(req.clone());
                Ok((launchable_params(req), binary.to_string()))
            })),
            probe: Some(Arc::new(move || probe_owner.probe.clone())),
        }))
    }
}

fn mock_provider(adapter: &Arc<MockAdapter>) -> ProtocolProvider {
    ProtocolProvider::new(adapter.protocol_adapter()).expect("provider")
}

struct NoReadyAdapter {
    spec: Spec,
}

impl NoReadyAdapter {
    fn new(spec: Spec) -> Self {
        Self { spec }
    }

    fn protocol_adapter(self: &Arc<Self>) -> Arc<ProtocolAdapter> {
        Arc::new(ProtocolAdapter::new(ProtocolOptions {
            spec: self.spec.clone(),
            binary: "/bin/sh".into(),
            launcher: Some(Arc::new(LocalLauncher::new())),
            framer: Some(Framer::newline_json()),
            dialect_factory: Arc::new(|| Box::new(NoReadyDialect)),
            build_args: None,
            build_session: Some(Arc::new(move |_req, _files, launcher| {
                Ok(ProtocolSessionParts {
                    launcher,
                    args: vec!["-c".into(), "cat".into()],
                    dialect: Box::new(NoReadyDialect),
                })
            })),
            prepare_start: Some(Arc::new(|req, binary| {
                Ok((launchable_params(req), binary.to_string()))
            })),
            probe: Some(Arc::new(|| lucarne::adapter::ProbeResult::default())),
        }))
    }
}

struct NoReadyDialect;

impl Dialect for NoReadyDialect {
    fn name(&self) -> &'static str {
        "provider-no-ready"
    }

    fn init(&mut self, _cfg: &SessionParams) -> Vec<OutFrame> {
        Vec::new()
    }

    fn translate(&mut self, frame_bytes: &[u8]) -> Vec<CanonicalEvent> {
        panic!(
            "no-ready fixture should not receive provider frames: {}",
            String::from_utf8_lossy(frame_bytes)
        )
    }

    fn encode_user_message(&mut self, _input: &Input) -> lucarne::Result<Vec<OutFrame>> {
        panic!("user messages are not used in this test fixture")
    }

    fn encode_permission_response(
        &mut self,
        _req_id: &str,
        _resp: &lucarne::event::PermissionResponse,
    ) -> lucarne::Result<Vec<OutFrame>> {
        panic!("permission responses are not used in this test fixture")
    }

    fn encode_interrupt(&mut self) -> lucarne::Result<Vec<OutFrame>> {
        panic!("interrupt is not used in this test fixture")
    }
}

#[derive(Default)]
struct EchoDialect {
    seq: usize,
}

impl Dialect for EchoDialect {
    fn name(&self) -> &'static str {
        "provider-echo"
    }

    fn init(&mut self, _cfg: &SessionParams) -> Vec<OutFrame> {
        vec![OutFrame::stdin(frame_line(
            json!({"kind":"session_started","session_id":"provider-session"}),
        ))]
    }

    fn translate(&mut self, frame_bytes: &[u8]) -> Vec<CanonicalEvent> {
        let value: Value = serde_json::from_slice(frame_bytes).expect("decode frame");
        match value["kind"].as_str().expect("frame kind") {
            "session_started" => vec![CanonicalEvent::new(Payload::SessionStarted(
                SessionStarted {
                    session_id: value["session_id"].as_str().expect("session id").into(),
                    model: "echo-model".into(),
                },
            ))],
            "submit" => vec![CanonicalEvent::new(Payload::Timeline(Timeline {
                item: event::new_timeline_user(
                    &self.next_id(),
                    &format!(
                        "submit:{}{}",
                        value["text"].as_str().expect("text"),
                        image_debug_suffix(value.get("images"))
                    ),
                ),
            }))],
            other => panic!("unexpected frame kind: {other}"),
        }
    }

    fn encode_user_message(&mut self, input: &Input) -> lucarne::Result<Vec<OutFrame>> {
        Ok(vec![OutFrame::stdin(frame_line(json!({
            "kind":"submit",
            "text":input.text,
            "images": input.images.iter().map(|image| {
                json!({
                    "media_type": image.media_type,
                    "data_len": image.data.len(),
                })
            }).collect::<Vec<_>>(),
        })))])
    }

    fn encode_permission_response(
        &mut self,
        _req_id: &str,
        _resp: &lucarne::event::PermissionResponse,
    ) -> lucarne::Result<Vec<OutFrame>> {
        panic!("permission responses are not used in this test fixture")
    }

    fn encode_interrupt(&mut self) -> lucarne::Result<Vec<OutFrame>> {
        panic!("interrupt is not used in this test fixture")
    }
}

impl EchoDialect {
    fn next_id(&mut self) -> String {
        self.seq += 1;
        format!("event-{}", self.seq)
    }
}

#[derive(Default)]
struct FailingClaudeRecoveryAdapter {
    starts: AtomicUsize,
}

impl FailingClaudeRecoveryAdapter {
    fn protocol_adapter(self: &Arc<Self>) -> Arc<ProtocolAdapter> {
        let start_owner = Arc::clone(self);
        Arc::new(ProtocolAdapter::new(ProtocolOptions {
            spec: Spec {
                id: ProviderId::from_static("claude"),
                label: "Claude".into(),
                protocol: Protocol::StdioNewlineJson,
                capabilities: Capabilities {
                    thinking: true,
                    tool_stream: true,
                    usage: true,
                    structured_intervention: true,
                    command_catalog: true,
                    permission_intercept: false,
                    ..Default::default()
                },
                ..Default::default()
            },
            binary: "/bin/sh".into(),
            launcher: Some(Arc::new(LocalLauncher::new())),
            framer: Some(Framer::newline_json()),
            dialect_factory: Arc::new(|| Box::new(ClaudeRecoveryDialect::default())),
            build_args: None,
            build_session: Some(Arc::new(move |_req, _files, launcher| {
                if start_owner.starts.fetch_add(1, Ordering::Relaxed) == 0 {
                    Ok(ProtocolSessionParts {
                        launcher,
                        args: vec!["-c".into(), "cat".into()],
                        dialect: Box::new(ClaudeRecoveryDialect::default()),
                    })
                } else {
                    Err(lucarne::LucarneError::runtime(
                        "forced claude recovery restart failure",
                    ))
                }
            })),
            prepare_start: Some(Arc::new(|req, binary| {
                Ok((launchable_params(req), binary.to_string()))
            })),
            probe: Some(Arc::new(|| lucarne::adapter::ProbeResult::default())),
        }))
    }
}

fn claude_recovery_provider(adapter: &Arc<FailingClaudeRecoveryAdapter>) -> ProtocolProvider {
    ProtocolProvider::new(adapter.protocol_adapter()).expect("provider")
}

fn launchable_params(req: &SessionParams) -> SessionParams {
    let mut launch_req = req.clone();
    if !std::path::Path::new(&launch_req.cwd).is_dir() {
        launch_req.cwd = std::env::current_dir()
            .expect("current dir")
            .display()
            .to_string();
    }
    launch_req
}

#[derive(Default)]
struct ClaudeRecoveryDialect {
    approval_sent: bool,
}

impl Dialect for ClaudeRecoveryDialect {
    fn name(&self) -> &'static str {
        "claude-recovery"
    }

    fn init(&mut self, _cfg: &SessionParams) -> Vec<OutFrame> {
        vec![OutFrame::stdin(frame_line(
            json!({"kind":"session_started","session_id":"provider-session"}),
        ))]
    }

    fn translate(&mut self, frame_bytes: &[u8]) -> Vec<CanonicalEvent> {
        let value: Value = serde_json::from_slice(frame_bytes).expect("decode frame");
        match value["kind"].as_str().expect("frame kind") {
            "session_started" => vec![CanonicalEvent::new(Payload::SessionStarted(
                SessionStarted {
                    session_id: value["session_id"].as_str().expect("session id").into(),
                    model: "claude-recovery-model".into(),
                },
            ))],
            "submit"
                if value["text"].as_str() == Some("trigger-approval") && !self.approval_sent =>
            {
                self.approval_sent = true;
                vec![CanonicalEvent::new(Payload::PermissionRequest(
                    lucarne::event::PermissionRequest {
                        req_id: "claude-permission-toolu-1".into(),
                        tool: "Write".into(),
                        input: Some(json!({"file_path":"danger.txt"})),
                        risk: lucarne::event::Risk::High,
                        questions: vec![],
                    },
                ))]
            }
            "submit" => vec![CanonicalEvent::new(Payload::Timeline(Timeline {
                item: event::new_timeline_user(
                    "ignored-submit",
                    value["text"].as_str().expect("text"),
                ),
            }))],
            "resolved" => vec![CanonicalEvent::new(Payload::Timeline(Timeline {
                item: event::new_timeline_reasoning(
                    "resolved-event",
                    &format!("resolved:{}", value["decision"].as_str().expect("decision")),
                ),
            }))],
            other => panic!("unexpected frame kind: {other}"),
        }
    }

    fn encode_user_message(&mut self, input: &Input) -> lucarne::Result<Vec<OutFrame>> {
        Ok(vec![OutFrame::stdin(frame_line(
            json!({"kind":"submit","text":input.text}),
        ))])
    }

    fn encode_permission_response(
        &mut self,
        req_id: &str,
        resp: &lucarne::event::PermissionResponse,
    ) -> lucarne::Result<Vec<OutFrame>> {
        let decision = match resp.decision {
            lucarne::event::Decision::Allow => "allow",
            lucarne::event::Decision::Deny => "deny",
        };
        Ok(vec![OutFrame::stdin(frame_line(
            json!({"kind":"resolved","req_id": req_id, "decision": decision}),
        ))])
    }

    fn encode_interrupt(&mut self) -> lucarne::Result<Vec<OutFrame>> {
        panic!("interrupt is not used in this test fixture")
    }
}

fn frame_line(value: Value) -> Vec<u8> {
    let mut bytes = serde_json::to_vec(&value).expect("serialize frame");
    bytes.push(b'\n');
    bytes
}

fn image_debug_suffix(raw: Option<&Value>) -> String {
    let Some(images) = raw.and_then(Value::as_array) else {
        return String::new();
    };
    if images.is_empty() {
        return String::new();
    }
    let summary = images
        .iter()
        .map(|image| {
            format!(
                "{}:{}",
                image
                    .get("media_type")
                    .and_then(Value::as_str)
                    .unwrap_or(""),
                image.get("data_len").and_then(Value::as_u64).unwrap_or(0)
            )
        })
        .collect::<Vec<_>>()
        .join(",");
    format!("|images={summary}")
}
