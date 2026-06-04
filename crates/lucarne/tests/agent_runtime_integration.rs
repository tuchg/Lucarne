#![allow(clippy::single_match)]

pub mod common;

use common::agent_runtime::{
    assert_stream_closed, assistant_texts, collect_until_closed, open_request, reasoning_texts,
    recv_event, resume_request, runtime, submit_input, take_events,
};
use lucarne::agent_runtime::{Event, InterventionRequest, InterventionResponse};
use serde_json::Value;
use tokio::time::{timeout, Duration};

#[tokio::test]
async fn agent_runtime_integration_provider_capabilities_match_design() {
    let runtime = runtime();

    for (provider_id, reasoning, tool, usage, structured) in [
        ("claude", true, true, true, true),
        ("codex", true, true, true, true),
        ("gemini", true, true, true, true),
    ] {
        let provider = runtime.provider(provider_id).expect("registered provider");
        let probe = provider.probe().await.expect("probe provider");
        assert_eq!(probe.provider_id.as_str(), provider_id);
        assert_eq!(probe.capabilities.reasoning_stream, reasoning);
        assert_eq!(probe.capabilities.tool_stream, tool);
        assert_eq!(probe.capabilities.usage_reporting, usage);
        assert_eq!(probe.capabilities.structured_intervention, structured);
    }
}

#[tokio::test]
async fn agent_runtime_integration_lists_registered_provider_descriptors() {
    let runtime = runtime();
    let descriptors = runtime
        .providers()
        .into_iter()
        .map(|descriptor| (descriptor.id.as_str(), descriptor.label.to_string()))
        .collect::<Vec<_>>();

    assert_eq!(
        descriptors,
        vec![
            ("claude", "Claude Code".to_string()),
            ("codex", "OpenAI Codex CLI".to_string()),
            ("gemini", "Google Gemini CLI".to_string()),
        ]
    );
    assert!(runtime.provider("missing").is_none());
}

#[tokio::test]
async fn agent_runtime_integration_gemini_approval_round_trip_uses_external_resolution() {
    let runtime = runtime();
    let session = runtime
        .open(
            "gemini",
            open_request("gemini", "permission.fixture", Some("Use tools")),
        )
        .await
        .expect("open gemini approval session");
    assert_eq!(session.id().0.as_str(), "gemini-sess-permission");

    let mut events = take_events(session.as_ref()).await;
    let req_id = loop {
        match recv_event(&mut events).await {
            Event::InterventionRequest(InterventionRequest::Approval(request)) => {
                assert_eq!(request.tool_name, "Write live-output.txt");
                break request.req_id.to_string();
            }
            _ => {}
        }
    };

    session
        .resolve(
            &req_id,
            InterventionResponse::Approval(lucarne::agent_runtime::ApprovalDecision::Allow),
        )
        .await
        .expect("resolve gemini approval");

    let events = collect_until_closed(&mut events).await;
    assert!(
        events.iter().any(|event| matches!(event, Event::ToolResult(tool_result) if tool_result.is_error == Some(false))),
        "missing successful tool result after approval: {events:?}"
    );
    assert!(
        assistant_texts(&events)
            .iter()
            .any(|text| text == "TOOL_OK"),
        "missing TOOL_OK assistant reply after approval: {events:?}"
    );
}

#[tokio::test]
async fn agent_runtime_integration_codex_approval_round_trip_uses_external_resolution() {
    let runtime = runtime();
    let session = runtime
        .open(
            "codex",
            open_request("codex", "permission_real.fixture", Some("delete target")),
        )
        .await
        .expect("open codex approval session");
    assert_eq!(session.id().0.as_str(), "real-thread-1");

    let mut events = take_events(session.as_ref()).await;
    let req_id = loop {
        match recv_event(&mut events).await {
            Event::InterventionRequest(InterventionRequest::Approval(request)) => {
                assert!(
                    request.tool_name.contains("requestApproval"),
                    "unexpected approval tool: {:?}",
                    request
                );
                break request.req_id.to_string();
            }
            _ => {}
        }
    };

    session
        .resolve(
            &req_id,
            InterventionResponse::Approval(lucarne::agent_runtime::ApprovalDecision::Allow),
        )
        .await
        .expect("resolve codex approval");

    let events = collect_until_closed(&mut events).await;
    assert!(
        events
            .iter()
            .any(|event| matches!(event, Event::ToolCall(tool_call) if tool_call.name == "shell")),
        "missing shell tool call after approval: {events:?}"
    );
}

#[tokio::test]
async fn agent_runtime_integration_open_without_initial_input_requires_submit() {
    let runtime = runtime();

    for (provider_id, fixture, prompt, session_id, assistant_text) in [
        (
            "claude",
            "basic.fixture",
            "Hello Claude",
            "sess-basic",
            "Hi! How can I help?",
        ),
        (
            "codex",
            "basic.fixture",
            "hello",
            "test-thread-basic",
            "Hello from Codex!",
        ),
        (
            "gemini",
            "basic.fixture",
            "Hello Gemini",
            "gemini-sess-basic",
            "Hello! How can I help you today?",
        ),
    ] {
        let session = runtime
            .open(provider_id, open_request(provider_id, fixture, None))
            .await
            .expect("open session");
        assert_eq!(session.id().0.as_str(), session_id);

        let mut events = take_events(session.as_ref()).await;
        assert!(
            timeout(Duration::from_millis(100), events.recv())
                .await
                .is_err(),
            "expected no public events before submit for provider {provider_id}"
        );

        submit_input(session.as_ref(), prompt).await;

        let events = collect_until_closed(&mut events).await;
        assert!(
            assistant_texts(&events)
                .iter()
                .any(|text| text == assistant_text),
            "missing assistant reply for provider {provider_id}: {events:?}"
        );
    }
}

#[tokio::test]
async fn agent_runtime_integration_close_ends_idle_session() {
    let runtime = runtime();

    for (provider_id, fixture) in [
        ("claude", "basic.fixture"),
        ("codex", "basic.fixture"),
        ("gemini", "basic.fixture"),
    ] {
        let session = runtime
            .open(provider_id, open_request(provider_id, fixture, None))
            .await
            .expect("open session");
        let mut events = take_events(session.as_ref()).await;
        session.close().await.expect("close session");
        assert_stream_closed(&mut events).await;
    }
}

#[tokio::test]
async fn agent_runtime_integration_resume_accepts_provider_owned_args() {
    let runtime = runtime();

    for (provider_id, fixture, session_ref, prompt, expected_id, expected_text) in [
        (
            "claude",
            "resume.fixture",
            "sess-previous",
            "continue",
            "sess-resumed",
            "Resumed OK.",
        ),
        (
            "codex",
            "resume_success.fixture",
            "thread-previous",
            "resume check",
            "test-thread-resumed",
            "Resumed prior thread.",
        ),
        (
            "gemini",
            "resume.fixture",
            "resume-123",
            "resume check",
            "resume-123",
            "Resumed session.",
        ),
    ] {
        let session = runtime
            .resume(
                provider_id,
                resume_request(provider_id, fixture, session_ref),
            )
            .await
            .expect("resume session");
        assert_eq!(session.id().0.as_str(), expected_id);

        let mut events = take_events(session.as_ref()).await;
        submit_input(session.as_ref(), prompt).await;

        let events = collect_until_closed(&mut events).await;
        assert!(
            assistant_texts(&events)
                .iter()
                .any(|text| text == expected_text),
            "missing resume reply for provider {provider_id}: {events:?}"
        );
    }
}

#[tokio::test]
async fn agent_runtime_integration_claude_question_passthrough_projects_question_intervention() {
    let runtime = runtime();
    let session = runtime
        .open(
            "claude",
            open_request(
                "claude",
                "question_passthrough.fixture",
                Some("pick a theme"),
            ),
        )
        .await
        .expect("open claude question session");
    assert_eq!(session.id().0.as_str(), "sess-question-pass");

    let mut events = take_events(session.as_ref()).await;
    let events = collect_until_closed(&mut events).await;
    assert!(
        events.iter().any(|event| matches!(
            event,
            Event::InterventionRequest(InterventionRequest::Question(request))
                if request.questions.len() == 1
                    && request.questions[0].header.as_deref() == Some("Theme")
                    && request.questions[0].text.as_str() == "Which color should I use?"
        )),
        "missing structured question projection: {events:?}"
    );
    assert!(
        !events.iter().any(|event| matches!(
            event,
            Event::ToolCall(tool_call) if tool_call.name == "AskUserQuestion"
        )),
        "unexpected public AskUserQuestion tool call: {events:?}"
    );
}

#[tokio::test]
async fn agent_runtime_integration_codex_tool_and_interrupt_flows() {
    let runtime = runtime();

    let session = runtime
        .open(
            "codex",
            open_request("codex", "legacy.fixture", Some("legacy tool call")),
        )
        .await
        .expect("open codex legacy session");
    let mut events = take_events(session.as_ref()).await;
    let events = collect_until_closed(&mut events).await;
    let tool_call = events
        .iter()
        .find_map(|event| match event {
            Event::ToolCall(tool_call) => Some(tool_call),
            _ => None,
        })
        .expect("tool call");
    assert_eq!(tool_call.name, "shell");
    assert_eq!(
        tool_call.input["command"],
        Value::String("echo hello".into())
    );
    let tool_result = events
        .iter()
        .find_map(|event| match event {
            Event::ToolResult(tool_result) => Some(tool_result),
            _ => None,
        })
        .expect("tool result");
    assert_eq!(tool_result.output["output"], Value::String("hello".into()));

    let session = runtime
        .open(
            "codex",
            open_request(
                "codex",
                "interrupt_real.fixture",
                Some("Use tools, do not simulate. Run a shell command to execute `sleep 30` in the current working directory. Do not recover or switch tools."),
            ),
        )
        .await
        .expect("open codex interrupt session");
    let mut events = take_events(session.as_ref()).await;
    loop {
        match recv_event(&mut events).await {
            Event::ToolCall(tool_call)
                if tool_call.name == "shell"
                    && tool_call.input["command"] == Value::String("sleep 30".into()) =>
            {
                session.interrupt().await.expect("interrupt session");
                break;
            }
            _ => {}
        }
    }
    let events = collect_until_closed(&mut events).await;
    assert!(
        !events
            .iter()
            .any(|event| matches!(event, Event::ToolResult(_))),
        "unexpected tool result after interrupt: {events:?}"
    );
    assert!(
        assistant_texts(&events).is_empty(),
        "unexpected assistant reply after interrupt: {events:?}"
    );
}

#[tokio::test]
async fn agent_runtime_integration_codex_submit_while_busy_queues_follow_up_turn() {
    let runtime = runtime();
    let session = runtime
        .open(
            "codex",
            open_request(
                "codex",
                "queued_submit_real.fixture",
                Some("Use tools, do not simulate. Run a shell command to execute `sleep 8` in the current working directory. Do not recover or switch tools. After the command finishes, reply with exactly FIRST_DONE and stop."),
            ),
        )
        .await
        .expect("open codex queued submit session");

    let mut events = take_events(session.as_ref()).await;
    loop {
        match recv_event(&mut events).await {
            Event::ToolCall(tool_call)
                if tool_call.name == "shell"
                    && tool_call.input["command"] == Value::String("sleep 8".into()) =>
            {
                submit_input(
                    session.as_ref(),
                    "Ignore the previous task. Reply with exactly SECOND_DONE and nothing else.",
                )
                .await;
                break;
            }
            _ => {}
        }
    }

    let events = collect_until_closed(&mut events).await;
    assert_eq!(
        assistant_texts(&events),
        vec!["FIRST_DONE".to_string(), "SECOND_DONE".to_string()],
        "expected queued codex follow-up turn after busy submit: {events:?}"
    );
    let completed_turns = events
        .iter()
        .filter_map(|event| match event {
            Event::TurnCompleted(turn) => Some(turn.turn_id.to_string()),
            _ => None,
        })
        .collect::<Vec<_>>();
    assert_eq!(
        completed_turns,
        vec![
            "turn-real-queue-1".to_string(),
            "turn-real-queue-2".to_string()
        ],
        "final_answer items must not synthesize duplicate turn completion: {events:?}"
    );
}

#[tokio::test]
async fn agent_runtime_integration_codex_interrupt_preserves_late_tool_completion() {
    let runtime = runtime();
    let session = runtime
        .open(
            "codex",
            open_request(
                "codex",
                "interrupt_real_completion.fixture",
                Some("Use tools, do not simulate. Run a shell command to execute `sleep 30; printf AFTER_INTERRUPT` in the current working directory. Do not recover or switch tools. If the command finishes, reply with exactly INTERRUPT_MISSED."),
            ),
        )
        .await
        .expect("open codex interrupt completion session");

    let mut events = take_events(session.as_ref()).await;
    loop {
        match recv_event(&mut events).await {
            Event::ToolCall(tool_call)
                if tool_call.name == "shell"
                    && tool_call.input["command"]
                        == Value::String("sleep 30; printf AFTER_INTERRUPT".into()) =>
            {
                session.interrupt().await.expect("interrupt session");
                break;
            }
            _ => {}
        }
    }

    let events = collect_until_closed(&mut events).await;
    assert!(
        events.iter().any(|event| matches!(
            event,
            Event::ToolResult(tool_result)
                if tool_result.output["output"] == Value::String("AFTER_INTERRUPT".into())
        )),
        "expected post-interrupt command completion to remain visible: {events:?}"
    );
    assert!(
        assistant_texts(&events).is_empty(),
        "unexpected assistant reply after interrupt: {events:?}"
    );
}

#[tokio::test]
async fn agent_runtime_integration_gemini_reasoning_and_tool_streams() {
    let runtime = runtime();

    let session = runtime
        .open(
            "gemini",
            open_request(
                "gemini",
                "question.fixture",
                Some("Use tools, do not simulate. Ask me one clarifying question using the provider's native question flow. After it is answered, reply with exactly QUESTION_OK."),
            ),
        )
        .await
        .expect("open gemini question session");
    let mut events = take_events(session.as_ref()).await;
    let events = collect_until_closed(&mut events).await;
    assert!(
        !reasoning_texts(&events).is_empty(),
        "expected reasoning events from gemini question flow"
    );
    assert!(
        !events.iter().any(|event| matches!(
            event,
            Event::InterventionRequest(lucarne::agent_runtime::InterventionRequest::Question(_))
        )),
        "unexpected synthetic structured question: {events:?}"
    );
    assert!(
        assistant_texts(&events)
            .iter()
            .any(|text| text == "Which `.pen` file would you like to open or work with?"),
        "missing gemini assistant question: {events:?}"
    );

    let session = runtime
        .open(
            "gemini",
            open_request(
                "gemini",
                "tool_success_output.fixture",
                Some("run a command"),
            ),
        )
        .await
        .expect("open gemini tool session");
    let mut events = take_events(session.as_ref()).await;
    let events = collect_until_closed(&mut events).await;
    assert!(
        events
            .iter()
            .any(|event| matches!(event, Event::ToolCall(_))),
        "expected gemini tool call: {events:?}"
    );
    let tool_result = events
        .iter()
        .find_map(|event| match event {
            Event::ToolResult(tool_result) => Some(tool_result),
            _ => None,
        })
        .expect("tool result");
    assert_eq!(tool_result.output["output"], Value::String("hello".into()));
}
