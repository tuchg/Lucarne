use lucarne::agent_runtime::project::Projector;
use lucarne::agent_runtime::{
    AgentCapabilities, Event as PublicEvent, MessageRole, QuestionOption, SessionId, ToolCallEvent,
    ToolResultEvent, UsageEvent,
};
use lucarne::event::{
    self, Event as CanonicalEvent, Payload, PermissionQuestion, PermissionQuestionOption,
    PermissionRequest, SessionClosed, SessionStarted, Timeline, TurnCompleted, TurnFailed,
    TurnStarted, Usage, UsageDelta,
};
use serde_json::json;
use smol_str::SmolStr;

fn project(payload: Payload) -> lucarne::agent_runtime::project::Projection {
    project_for("claude", payload)
}

fn project_for(
    provider_id: &'static str,
    payload: Payload,
) -> lucarne::agent_runtime::project::Projection {
    let capabilities = AgentCapabilities {
        structured_intervention: matches!(provider_id, "claude" | "codex" | "gemini" | "test"),
        ..Default::default()
    };
    Projector::project(capabilities, CanonicalEvent::new(payload))
}

#[test]
fn agent_runtime_projection_projects_user_and_assistant_text_messages() {
    let user = project(Payload::Timeline(Timeline {
        item: event::new_timeline_user("item-1", "hello"),
    }));
    let assistant = project(Payload::Timeline(Timeline {
        item: event::new_timeline_assistant("item-2", "world", true),
    }));

    assert_eq!(user.events.len(), 1);
    assert_eq!(assistant.events.len(), 1);

    assert_eq!(
        user.events[0],
        PublicEvent::Message(lucarne::agent_runtime::MessageEvent {
            role: MessageRole::User,
            text: "hello".into(),
            streaming: false,
        })
    );
    assert_eq!(
        assistant.events[0],
        PublicEvent::Message(lucarne::agent_runtime::MessageEvent {
            role: MessageRole::Assistant,
            text: "world".into(),
            streaming: true,
        })
    );
}

#[test]
fn agent_runtime_projection_preserves_assistant_streaming_flag() {
    let projection = project(Payload::Timeline(Timeline {
        item: event::new_timeline_assistant("item-2", "chunk", true),
    }));

    match projection.events.as_slice() {
        [PublicEvent::Message(message)] => assert!(message.streaming),
        other => panic!("expected one assistant message, got {other:?}"),
    }
}

#[test]
fn agent_runtime_projection_projects_reasoning_text() {
    let projection = project(Payload::Timeline(Timeline {
        item: event::new_timeline_reasoning("item-1", "thinking"),
    }));

    assert_eq!(
        projection.events,
        vec![PublicEvent::Reasoning(
            lucarne::agent_runtime::ReasoningEvent {
                text: "thinking".into(),
            }
        )]
    );
}

#[test]
fn agent_runtime_projection_projects_tool_call_and_result_with_shared_call_id() {
    let call = project(Payload::Timeline(Timeline {
        item: event::new_timeline_tool_call("call-1", event::shell("ls -la")),
    }));
    let result = project(Payload::Timeline(Timeline {
        item: event::new_timeline_tool_result(
            "result-1",
            "call-1",
            event::ToolResult {
                output: "done".into(),
                ..Default::default()
            },
        ),
    }));

    assert_eq!(call.events.len(), 1);
    assert_eq!(result.events.len(), 1);

    match &call.events[0] {
        PublicEvent::ToolCall(ToolCallEvent {
            call_id,
            name,
            input,
        }) => {
            assert_eq!(
                call_id,
                &lucarne::agent_runtime::CallId(SmolStr::new("call-1"))
            );
            assert_eq!(name, "shell");
            assert_eq!(input["command"], "ls -la");
        }
        other => panic!("unexpected projected event: {other:?}"),
    }

    match &result.events[0] {
        PublicEvent::ToolResult(ToolResultEvent {
            call_id,
            output,
            is_error,
        }) => {
            assert_eq!(
                call_id,
                &lucarne::agent_runtime::CallId(SmolStr::new("call-1"))
            );
            assert_eq!(output["output"], "done");
            assert_eq!(is_error, &Some(false));
        }
        other => panic!("unexpected projected event: {other:?}"),
    }
}

#[test]
fn agent_runtime_projection_projects_usage_delta_and_turn_completed_usage_snapshot() {
    let delta = project(Payload::UsageDelta(UsageDelta {
        delta: Usage {
            input_tokens: 10,
            output_tokens: 20,
            cost_usd: 0.5,
            ..Default::default()
        },
    }));
    let completed = project(Payload::TurnCompleted(TurnCompleted {
        turn_id: "turn-1".into(),
        usage: Some(Usage {
            input_tokens: 7,
            output_tokens: 8,
            cost_usd: 0.25,
            ..Default::default()
        }),
    }));

    assert_eq!(delta.events.len(), 1);
    match &delta.events[0] {
        PublicEvent::Usage(UsageEvent {
            input_tokens,
            output_tokens,
            total_tokens,
            raw,
        }) => {
            assert_eq!(*input_tokens, Some(10));
            assert_eq!(*output_tokens, Some(20));
            assert!(raw["input_tokens"].is_number());
            assert!(raw["output_tokens"].is_number());
            assert_eq!(*total_tokens, Some(30));
        }
        other => panic!("unexpected projected event: {other:?}"),
    }

    assert_eq!(completed.events.len(), 1);
    match &completed.events[0] {
        PublicEvent::TurnCompleted(tc) => {
            let usage = tc.usage.as_ref().expect("usage present");
            assert_eq!(usage.input_tokens, Some(7));
            assert_eq!(usage.output_tokens, Some(8));
            assert!(usage.raw["input_tokens"].is_number());
            assert!(usage.raw["output_tokens"].is_number());
            assert_eq!(usage.total_tokens, Some(15));
            assert_eq!(&*tc.turn_id, "turn-1");
        }
        other => panic!("unexpected projected event: {other:?}"),
    }
}

#[test]
fn agent_runtime_projection_projects_structured_question_and_approval_requests() {
    let question = project(Payload::PermissionRequest(PermissionRequest {
        req_id: "req-q".into(),
        tool: "request_user_input".into(),
        input: Some(
            json!({"questions":[{"header":"hdr","question":"question?","options":[{"label":"yes","description":"allow"}],"multi_select":true,"is_other":true,"is_secret":true}]}),
        ),
        risk: event::Risk::Low,
        questions: vec![PermissionQuestion {
            id: "q-1".into(),
            header: "hdr".into(),
            question: "question?".into(),
            options: vec![PermissionQuestionOption {
                label: "yes".into(),
                description: "allow".into(),
            }],
            multi_select: true,
            is_other: true,
            is_secret: true,
        }],
    }));
    let approval = project(Payload::PermissionRequest(PermissionRequest {
        req_id: "req-a".into(),
        tool: "shell".into(),
        input: Some(json!({"command":"ls"})),
        risk: event::Risk::Medium,
        questions: Vec::new(),
    }));

    assert_eq!(question.events.len(), 1);
    assert_eq!(approval.events.len(), 1);

    match &question.events[0] {
        PublicEvent::InterventionRequest(
            lucarne::agent_runtime::InterventionRequest::Question(req),
        ) => {
            assert_eq!(req.req_id, SmolStr::new("req-q"));
            assert_eq!(req.questions.len(), 1);
            assert_eq!(req.questions[0].header.as_deref(), Some("hdr"));
            assert_eq!(req.questions[0].text.as_str(), "question?");
            assert_eq!(
                req.questions[0].options,
                vec![QuestionOption {
                    label: "yes".into(),
                    description: Some("allow".into()),
                }]
            );
            assert!(req.questions[0].multi_select);
        }
        other => panic!("unexpected projected event: {other:?}"),
    }

    match &approval.events[0] {
        PublicEvent::InterventionRequest(
            lucarne::agent_runtime::InterventionRequest::Approval(req),
        ) => {
            assert_eq!(req.req_id, SmolStr::new("req-a"));
            assert_eq!(req.tool_name, "shell");
            assert_eq!(req.input, Some(json!({"command":"ls"})));
            assert!(req.message.is_none());
        }
        other => panic!("unexpected projected event: {other:?}"),
    }
}

#[test]
fn agent_runtime_projection_suppresses_request_user_input_timeline_tool_call() {
    let projected_tool_call = project(Payload::Timeline(Timeline {
        item: event::new_timeline_tool_call(
            "call-q",
            event::unknown_tool("request_user_input", Some(json!({"questions":[] }))),
        ),
    }));
    let projected_request = project(Payload::PermissionRequest(PermissionRequest {
        req_id: "req-q".into(),
        tool: "request_user_input".into(),
        input: Some(json!({"questions":[] })),
        risk: event::Risk::Low,
        questions: Vec::new(),
    }));

    assert!(projected_tool_call.events.is_empty());
    assert_eq!(projected_request.events.len(), 1);
    assert!(matches!(
        projected_request.events[0],
        PublicEvent::InterventionRequest(lucarne::agent_runtime::InterventionRequest::Approval(_))
    ));
}

#[test]
fn agent_runtime_projection_uses_capabilities_for_structured_interventions() {
    let request = Payload::PermissionRequest(PermissionRequest {
        req_id: "req-g".into(),
        tool: "shell".into(),
        input: Some(json!({"command":"ls"})),
        risk: event::Risk::Medium,
        questions: Vec::new(),
    });

    let disabled = Projector::project(
        AgentCapabilities::default(),
        CanonicalEvent::new(request.clone()),
    );
    let enabled = Projector::project(
        AgentCapabilities {
            structured_intervention: true,
            ..Default::default()
        },
        CanonicalEvent::new(request),
    );

    assert!(disabled.events.is_empty());
    assert_eq!(enabled.events.len(), 1);
}

#[test]
fn agent_runtime_projection_ignores_internal_lifecycle_and_log_rows() {
    // TurnStarted and Log rows are internal-only;
    // TurnFailed / TurnCompleted are now surfaced publicly (see
    // `turn_lifecycle_events_surface_completion_markers`).
    let rows = vec![
        project(Payload::TurnStarted(TurnStarted {
            turn_id: "turn-1".into(),
        })),
        project(Payload::Log(event::LogLine {
            level: "info".into(),
            stream: "stdout".into(),
            text: "noise".into(),
        })),
    ];

    for projection in rows {
        assert!(projection.events.is_empty());
        assert!(projection.session_id.is_none());
        assert!(projection.close_reason.is_none());
    }
}

#[test]
fn agent_runtime_projection_turn_lifecycle_events_surface_completion_markers() {
    let failed = project(Payload::TurnFailed(TurnFailed {
        turn_id: "turn-1".into(),
        error: "boom".into(),
        code: "E1".into(),
    }));
    assert_eq!(failed.events.len(), 1);
    match &failed.events[0] {
        PublicEvent::TurnFailed(tf) => {
            assert_eq!(&*tf.turn_id, "turn-1");
            assert_eq!(&*tf.error, "boom");
            assert_eq!(&*tf.code, "E1");
        }
        other => panic!("unexpected: {other:?}"),
    }

    let completed = project(Payload::TurnCompleted(TurnCompleted {
        turn_id: "turn-2".into(),
        usage: None,
    }));
    assert_eq!(completed.events.len(), 1);
    match &completed.events[0] {
        PublicEvent::TurnCompleted(tc) => {
            assert_eq!(&*tc.turn_id, "turn-2");
            assert!(tc.usage.is_none());
        }
        other => panic!("unexpected: {other:?}"),
    }
}

#[test]
fn agent_runtime_projection_observes_session_metadata_without_emitting_public_events() {
    let started = project(Payload::SessionStarted(SessionStarted {
        session_id: "session-1".into(),
        model: "model".into(),
    }));
    let closed = project(Payload::SessionClosed(SessionClosed {
        reason: "finished".into(),
        resume: None,
    }));

    assert!(started.events.is_empty());
    assert_eq!(
        started.session_id,
        Some(SessionId(SmolStr::new("session-1")))
    );
    assert!(started.close_reason.is_none());

    assert!(closed.events.is_empty());
    assert!(closed.session_id.is_none());
    assert_eq!(closed.close_reason, Some("finished".into()));
}
