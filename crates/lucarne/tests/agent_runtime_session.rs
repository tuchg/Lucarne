use lucarne::agent_runtime::{
    AgentCommand, AgentCommandCatalog, AgentCommandCompletion, AgentCommandInput,
    AgentCommandInvocation, AgentCommandSource, AgentImageInput, AgentInput, AgentModelOption,
    AgentSession, AgentSessionFacade, AgentSessionOptions, ApprovalDecision, Event as PublicEvent,
    InstanceId, InterventionRequest, InterventionResponse, Question, QuestionAnswer,
    QuestionOption, QuestionResponse, RuntimeBusFilter, SessionId,
};
use lucarne::dialect::{CommandDispatch, CommandResult, Dialect, Input, OutFrame, SessionParams};
use lucarne::event::{
    self, Event as CanonicalEvent, Payload, PermissionQuestion, PermissionQuestionOption,
    PermissionRequest, PermissionResponse, SessionClosed, SessionStarted, Timeline,
};
use lucarne::framer::Framer;
use lucarne::launcher::{LaunchSpec, LocalLauncher};
use lucarne::runtime::{self, Config as RuntimeConfig};
use lucarne::ProviderId;
use serde_json::{json, Value};
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::mpsc;
use tokio::time::{advance, timeout};

const TEST_PROVIDER_ID: ProviderId = ProviderId::from_static("test");

#[test]
fn agent_session_facade_metadata_uses_synchronous_locks() {
    let source = std::fs::read_to_string(
        std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("src/agent_runtime/session.rs"),
    )
    .expect("read session source");

    assert!(
        source.contains("session: Arc<AsyncMutex<runtime::Session>>"),
        "wrapped provider session should keep an async mutex because provider calls are async"
    );
    assert!(
        source.contains("provider_ready_rx: AsyncMutex<watch::Receiver<bool>>"),
        "provider readiness receiver should keep an async mutex because changed() awaits"
    );
    assert!(
        source.contains("provider_session_id: Arc<StdRwLock<Option<SessionId>>>"),
        "provider session id is read by callers and rarely overwritten by provider-native resume ids"
    );
    assert!(
        source.contains("close_reason: Arc<StdRwLock<Option<SmolStr>>>"),
        "observed close reason is read by callers and only filled from close paths"
    );
    assert!(
        source.contains("question_keys: Arc<StdRwLock<BTreeMap<String, Vec<Vec<String>>>>>"),
        "permission question lookup keys are read independently during resolve and should use RwLock"
    );

    for forbidden in [
        "provider_session_id: Arc<Mutex<Option<SessionId>>>",
        "provider_session_id: Arc<StdMutex<Option<SessionId>>>",
        "events_rx: Mutex<Option<AgentEventStream>>",
        "close_reason: Arc<Mutex<Option<SmolStr>>>",
        "close_reason: Arc<StdMutex<Option<SmolStr>>>",
        "question_keys: Arc<Mutex<BTreeMap<String, Vec<Vec<String>>>>>",
        "question_keys: Arc<StdMutex<BTreeMap<String, Vec<Vec<String>>>>>",
        "idle_task: Mutex<Option<JoinHandle<()>>>",
        "drain_task: Mutex<Option<JoinHandle<()>>>",
    ] {
        assert!(
            !source.contains(forbidden),
            "agent session metadata should not require async mutexes: {forbidden}"
        );
    }
}

#[test]
fn runtime_session_metadata_uses_synchronous_locks() {
    let source = std::fs::read_to_string(
        std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("src/runtime.rs"),
    )
    .expect("read runtime session source");

    assert!(
        source.contains("stdin: Arc<AsyncMutex<Option<tokio::process::ChildStdin>>>"),
        "child stdin should keep an async mutex because writes await"
    );
    assert!(
        source.contains("dialect: Arc<AsyncMutex<Box<dyn Dialect>>>"),
        "dialect should keep an async mutex because provider operations are serialized"
    );

    for forbidden in [
        "events_rx: Mutex<Option<mpsc::Receiver<Event>>>",
        "events_tx: Arc<Mutex<Option<mpsc::Sender<Event>>>>",
        "tasks: Mutex<Vec<JoinHandle<()>>>",
    ] {
        assert!(
            !source.contains(forbidden),
            "runtime session metadata should not require async mutexes: {forbidden}"
        );
    }
}

#[test]
fn launcher_process_pipe_handles_use_synchronous_locks() {
    let source = std::fs::read_to_string(
        std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("src/launcher.rs"),
    )
    .expect("read launcher source");

    for expected in [
        "stdin: StdMutex<Option<ChildStdin>>",
        "stdout: StdMutex<Option<Box<dyn AsyncRead + Unpin + Send>>>",
        "stderr: StdMutex<Option<Box<dyn AsyncRead + Unpin + Send>>>",
    ] {
        assert!(
            source.contains(expected),
            "launcher process pipe handles are only taken once and should use sync locks: {expected}"
        );
    }
    assert!(
        !source.contains("Mutex as AsyncMutex"),
        "launcher should not need async mutexes for one-shot pipe handle ownership"
    );
}

#[tokio::test]
async fn agent_runtime_session_readiness_blocks_until_session_started() {
    let (session, tx) = runtime::synthetic_session();
    let task = tokio::spawn(AgentSessionFacade::attach(
        TEST_PROVIDER_ID,
        InstanceId("instance-1".into()),
        session,
        None,
    ));

    tx.send(CanonicalEvent::new(Payload::Timeline(Timeline {
        item: event::new_timeline_user("item-1", "before-start"),
    })))
    .await
    .expect("send timeline");

    tokio::time::sleep(Duration::from_millis(25)).await;
    assert!(
        !task.is_finished(),
        "session became ready before SessionStarted"
    );

    tx.send(CanonicalEvent::new(Payload::SessionStarted(
        SessionStarted {
            session_id: "provider-session".into(),
            model: "model".into(),
        },
    )))
    .await
    .expect("send session started");

    let session = task.await.expect("join").expect("attach session");
    let handle: &dyn AgentSession = &session;
    assert_eq!(handle.id().0.as_str(), "provider-session");
    assert_eq!(handle.instance_id().0.as_str(), "instance-1");
}

#[tokio::test]
async fn agent_runtime_session_initial_input_can_bootstrap_session_started() {
    let session = start_delayed_start_session().await;
    let session = timeout(
        Duration::from_secs(2),
        AgentSessionFacade::attach(
            TEST_PROVIDER_ID,
            InstanceId("instance-bootstrap".into()),
            session,
            Some(AgentInput {
                text: "boot".into(),
                images: Vec::new(),
            }),
        ),
    )
    .await
    .expect("attach should not hang waiting for SessionStarted")
    .expect("attach should succeed");

    assert_eq!(session.id().0.as_str(), "provider-session-delayed");

    let mut events = session.take_events().await.expect("take events");
    assert!(matches!(
        events.recv().await,
        Some(PublicEvent::Message(message))
            if message.role == lucarne::agent_runtime::MessageRole::User
                && message.text.as_str() == "submit:boot"
    ));
}

#[tokio::test]
async fn agent_runtime_session_list_models_uses_typed_facade_api_without_public_events() {
    let session = start_echo_session().await;
    let session = AgentSessionFacade::attach(
        TEST_PROVIDER_ID,
        InstanceId("instance-typed-models".into()),
        session,
        None,
    )
    .await
    .expect("attach session");

    let catalog = session.list_models().await.expect("list typed models");

    assert_eq!(catalog.current_model.as_deref(), Some("echo-model"));
    assert_eq!(catalog.models.len(), 1);
    assert_eq!(catalog.models[0].id.as_str(), "echo-model");

    let mut events = session.take_events().await.expect("take public events");
    assert!(
        timeout(Duration::from_millis(100), events.recv())
            .await
            .is_err(),
        "typed list_models should return data directly, not leak a command result into the public stream"
    );
}

#[tokio::test]
async fn agent_runtime_session_list_models_works_after_public_event_stream_is_taken() {
    let session = start_echo_session().await;
    let session = AgentSessionFacade::attach(
        TEST_PROVIDER_ID,
        InstanceId("instance-typed-models-after-take".into()),
        session,
        None,
    )
    .await
    .expect("attach session");
    let mut events = session
        .take_events()
        .await
        .expect("take public events first");

    let catalog = session
        .list_models()
        .await
        .expect("typed list_models should not depend on the public stream");

    assert_eq!(catalog.current_model.as_deref(), Some("echo-model"));
    assert_eq!(catalog.models.len(), 1);
    assert_eq!(catalog.models[0].id.as_str(), "echo-model");
    assert!(
        timeout(Duration::from_millis(100), events.recv())
            .await
            .is_err(),
        "typed list_models should not leak command events to the already-taken public stream"
    );
}

#[tokio::test]
async fn agent_runtime_session_known_session_id_hint_allows_delayed_session_started() {
    let session = start_delayed_start_session().await;
    let session = timeout(
        Duration::from_secs(6),
        AgentSessionFacade::attach_with_known_session_id(
            TEST_PROVIDER_ID,
            InstanceId("instance-known-resume".into()),
            session,
            None,
            Some(lucarne::agent_runtime::SessionId(
                "provider-session-delayed".into(),
            )),
        ),
    )
    .await
    .expect("attach should not hang with a known session id")
    .expect("attach should succeed");

    assert_eq!(session.id().0.as_str(), "provider-session-delayed");

    let mut events = session.take_events().await.expect("take events");
    assert!(
        timeout(Duration::from_millis(100), events.recv())
            .await
            .is_err(),
        "expected no public events before the real SessionStarted arrives"
    );

    session
        .submit(AgentInput {
            text: "resume".into(),
            images: Vec::new(),
        })
        .await
        .expect("submit");

    assert!(matches!(
        events.recv().await,
        Some(PublicEvent::Message(message))
            if message.role == lucarne::agent_runtime::MessageRole::User
                && message.text.as_str() == "submit:resume"
    ));
}

#[tokio::test(start_paused = true)]
async fn agent_runtime_session_known_session_id_timeout_forces_public_drain_ready() {
    let (session, tx) = runtime::synthetic_session();
    let task = tokio::spawn(AgentSessionFacade::attach_with_known_session_id(
        TEST_PROVIDER_ID,
        InstanceId("instance-known-force-ready".into()),
        session,
        None,
        Some(SessionId("known-provider-session".into())),
    ));

    advance(Duration::from_secs(5)).await;
    let session = task.await.expect("join").expect("attach session");
    assert_eq!(session.id().0.as_str(), "known-provider-session");
    assert_eq!(
        session
            .provider_session_id()
            .await
            .map(|id| id.0.to_string()),
        Some("known-provider-session".to_string())
    );

    let mut events = session.take_events().await.expect("take events");
    tx.send(CanonicalEvent::new(Payload::Timeline(Timeline {
        item: event::new_timeline_assistant(
            "item-after-known-force-ready",
            "after-known-force-ready",
            false,
        ),
    })))
    .await
    .expect("send timeline");

    assert!(matches!(
        timeout(Duration::from_millis(100), events.recv()).await,
        Ok(Some(PublicEvent::Message(message)))
            if message.text.as_str() == "after-known-force-ready"
    ));
}

#[tokio::test(start_paused = true)]
async fn agent_runtime_session_fresh_idle_attach_uses_runtime_session_until_provider_id_arrives() {
    let (session, tx) = runtime::synthetic_session();
    let runtime_session_id = session.id().to_string();
    let task = tokio::spawn(AgentSessionFacade::attach(
        TEST_PROVIDER_ID,
        InstanceId("instance-fresh-idle".into()),
        session,
        None,
    ));

    advance(Duration::from_secs(2)).await;
    let session = task.await.expect("join").expect("attach session");
    assert_eq!(session.id().0.as_str(), runtime_session_id);
    assert!(
        session.provider_session_id().await.is_none(),
        "runtime fallback id must not be exposed as a provider resume id"
    );

    let mut events = session.take_events().await.expect("take events");
    tx.send(CanonicalEvent::new(Payload::SessionStarted(
        SessionStarted {
            session_id: "provider-session-late".into(),
            model: "model".into(),
        },
    )))
    .await
    .expect("send late session started");
    tx.send(CanonicalEvent::new(Payload::Timeline(Timeline {
        item: event::new_timeline_assistant("item-after-force-ready", "after-force-ready", false),
    })))
    .await
    .expect("send timeline");

    assert!(matches!(
        events.recv().await,
        Some(PublicEvent::Message(message)) if message.text.as_str() == "after-force-ready"
    ));
    assert_eq!(
        session
            .provider_session_id()
            .await
            .map(|id| id.0.to_string()),
        Some("provider-session-late".to_string())
    );
}

#[tokio::test]
async fn agent_runtime_session_fails_when_session_closed_arrives_first() {
    let (session, tx) = runtime::synthetic_session();
    let task = tokio::spawn(AgentSessionFacade::attach(
        TEST_PROVIDER_ID,
        InstanceId("instance-2".into()),
        session,
        None,
    ));

    tx.send(CanonicalEvent::new(Payload::SessionClosed(SessionClosed {
        reason: "closed-early".into(),
        resume: None,
    })))
    .await
    .expect("send closed");

    let err = match task.await.expect("join") {
        Ok(_) => panic!("attach should fail"),
        Err(err) => err,
    };
    assert!(err.message.contains("provider-native session id"));
}

#[tokio::test]
async fn agent_runtime_session_take_events_is_single_consumer() {
    let (session, tx) = runtime::synthetic_session();
    tx.send(CanonicalEvent::new(Payload::SessionStarted(
        SessionStarted {
            session_id: "provider-session".into(),
            model: "model".into(),
        },
    )))
    .await
    .expect("send session started");

    let session = AgentSessionFacade::attach(
        TEST_PROVIDER_ID,
        InstanceId("instance-3".into()),
        session,
        None,
    )
    .await
    .expect("attach");

    tx.send(CanonicalEvent::new(Payload::Timeline(Timeline {
        item: event::new_timeline_assistant("item-2", "hello", true),
    })))
    .await
    .expect("send assistant message");

    let mut events = session.take_events().await.expect("first take_events");
    let err = session
        .take_events()
        .await
        .expect_err("second take_events should fail");

    assert!(err.message.contains("already taken"));
    assert!(matches!(
        events.recv().await,
        Some(PublicEvent::Message(message)) if message.text.as_str() == "hello"
    ));
}

#[tokio::test]
async fn agent_runtime_session_control_methods_forward_to_wrapped_session() {
    let session = start_echo_session().await;
    let session = AgentSessionFacade::attach(
        TEST_PROVIDER_ID,
        InstanceId("instance-4".into()),
        session,
        Some(AgentInput {
            text: "boot".into(),
            images: Vec::new(),
        }),
    )
    .await
    .expect("attach");
    let mut events = session.take_events().await.expect("take events");

    assert!(matches!(
        events.recv().await,
        Some(PublicEvent::Message(message))
            if message.role == lucarne::agent_runtime::MessageRole::User
                && message.text.as_str() == "submit:boot"
    ));

    session
        .submit(AgentInput {
            text: "next".into(),
            images: Vec::new(),
        })
        .await
        .expect("submit");
    assert!(matches!(
        events.recv().await,
        Some(PublicEvent::Message(message))
            if message.role == lucarne::agent_runtime::MessageRole::User
                && message.text.as_str() == "submit:next"
    ));

    session
        .resolve(
            "approval-1",
            InterventionResponse::Approval(ApprovalDecision::Deny),
        )
        .await
        .expect("resolve approval");
    assert!(matches!(
        events.recv().await,
        Some(PublicEvent::Reasoning(reasoning))
            if reasoning.text.as_str()
                == "resolve:{\"answers\":{},\"decision\":\"deny\",\"req_id\":\"approval-1\"}"
    ));

    session.interrupt().await.expect("interrupt");
    assert!(matches!(
        events.recv().await,
        Some(PublicEvent::Reasoning(reasoning))
            if reasoning.text.as_str() == "interrupt"
    ));

    session.close().await.expect("close");
    assert!(timeout(Duration::from_secs(2), events.recv())
        .await
        .expect("event stream close timeout")
        .is_none());
}

#[tokio::test]
async fn agent_runtime_session_forwards_multimodal_inputs_to_wrapped_session() {
    let session = start_echo_session().await;
    let session = AgentSessionFacade::attach(
        TEST_PROVIDER_ID,
        InstanceId("instance-images".into()),
        session,
        Some(AgentInput {
            text: "boot".into(),
            images: vec![
                AgentImageInput {
                    media_type: "image/png".into(),
                    data_base64: "AQID".into(),
                },
                AgentImageInput {
                    media_type: "image/jpeg".into(),
                    data_base64: "BAUGBw==".into(),
                },
            ],
        }),
    )
    .await
    .expect("attach");
    let mut events = session.take_events().await.expect("take events");

    assert!(matches!(
        events.recv().await,
        Some(PublicEvent::Message(message))
            if message.role == lucarne::agent_runtime::MessageRole::User
                && message.text.as_str() == "submit:boot|images=image/png:3,image/jpeg:4"
    ));

    session
        .submit(AgentInput {
            text: "".into(),
            images: vec![AgentImageInput {
                media_type: "image/webp".into(),
                data_base64: "CAkKCww=".into(),
            }],
        })
        .await
        .expect("submit image-only input");

    assert!(matches!(
        events.recv().await,
        Some(PublicEvent::Message(message))
            if message.role == lucarne::agent_runtime::MessageRole::User
                && message.text.as_str() == "submit:|images=image/webp:5"
    ));
}

#[tokio::test]
async fn agent_runtime_session_resolve_answers_uses_preserved_question_keys() {
    let session = start_echo_session().await;
    let session = AgentSessionFacade::attach(
        TEST_PROVIDER_ID,
        InstanceId("instance-6".into()),
        session,
        None,
    )
    .await
    .expect("attach");
    let mut events = session.take_events().await.expect("take events");

    session
        .submit(AgentInput {
            text: "ask-question".into(),
            images: Vec::new(),
        })
        .await
        .expect("submit question trigger");

    assert!(matches!(
        events.recv().await,
        Some(PublicEvent::InterventionRequest(InterventionRequest::Question(request)))
            if request.req_id.as_str() == "question-1"
                && request.questions
                    == vec![
                        Question {
                            header: Some("Header 1".into()),
                            text: "Question 1".into(),
                            options: vec![QuestionOption {
                                label: "A".into(),
                                description: Some("first".into()),
                            }],
                            multi_select: false,
                        },
                        Question {
                            header: None,
                            text: "Question 2".into(),
                            options: vec![],
                            multi_select: true,
                        },
                    ]
    ));

    session
        .resolve(
            "question-1",
            InterventionResponse::Answers(QuestionResponse {
                answers: vec![
                    QuestionAnswer {
                        values: vec!["picked-a".into()],
                    },
                    QuestionAnswer {
                        values: vec!["picked-b".into(), "picked-c".into()],
                    },
                ],
            }),
        )
        .await
        .expect("resolve answers");

    let payload = match events.recv().await {
        Some(PublicEvent::Reasoning(reasoning)) => reasoning
            .text
            .strip_prefix("resolve:")
            .expect("resolve payload prefix")
            .to_string(),
        other => panic!("unexpected event: {other:?}"),
    };
    let payload: Value = serde_json::from_str(&payload).expect("decode resolve payload");
    assert_eq!(payload["req_id"], "question-1");
    assert_eq!(payload["decision"], "allow");
    assert_eq!(
        payload["answers"]["Header 1"]["answers"],
        json!(["picked-a"])
    );
    assert!(payload["answers"].get("Question 1").is_none());
    assert_eq!(
        payload["answers"]["Question 2"]["answers"],
        json!(["picked-b", "picked-c"])
    );
    assert_eq!(
        payload["answers"]
            .as_object()
            .expect("answers object")
            .len(),
        2
    );
    assert!(payload["answers"].get("0").is_none());
    assert!(payload["answers"].get("1").is_none());
}

#[tokio::test]
async fn agent_runtime_session_resolve_answers_with_duplicate_headers_uses_distinct_keys() {
    let session = start_echo_session().await;
    let session = AgentSessionFacade::attach(
        TEST_PROVIDER_ID,
        InstanceId("instance-8".into()),
        session,
        None,
    )
    .await
    .expect("attach");
    let mut events = session.take_events().await.expect("take events");

    session
        .submit(AgentInput {
            text: "ask-duplicate-header-question".into(),
            images: Vec::new(),
        })
        .await
        .expect("submit duplicate-header question trigger");

    assert!(matches!(
        events.recv().await,
        Some(PublicEvent::InterventionRequest(InterventionRequest::Question(request)))
            if request.req_id.as_str() == "question-dup"
                && request.questions
                    == vec![
                        Question {
                            header: Some("Shared Header".into()),
                            text: "Question Alpha".into(),
                            options: vec![],
                            multi_select: false,
                        },
                        Question {
                            header: Some("Shared Header".into()),
                            text: "Question Beta".into(),
                            options: vec![],
                            multi_select: false,
                        },
                    ]
    ));

    session
        .resolve(
            "question-dup",
            InterventionResponse::Answers(QuestionResponse {
                answers: vec![
                    QuestionAnswer {
                        values: vec!["alpha-answer".into()],
                    },
                    QuestionAnswer {
                        values: vec!["beta-answer".into()],
                    },
                ],
            }),
        )
        .await
        .expect("resolve duplicate-header answers");

    let payload = match events.recv().await {
        Some(PublicEvent::Reasoning(reasoning)) => reasoning
            .text
            .strip_prefix("resolve:")
            .expect("resolve payload prefix")
            .to_string(),
        other => panic!("unexpected event: {other:?}"),
    };
    let payload: Value = serde_json::from_str(&payload).expect("decode resolve payload");
    assert_eq!(payload["req_id"], "question-dup");
    assert_eq!(
        payload["answers"]["Question Alpha"]["answers"],
        json!(["alpha-answer"])
    );
    assert_eq!(
        payload["answers"]["Question Beta"]["answers"],
        json!(["beta-answer"])
    );
    assert!(payload["answers"].get("Shared Header").is_none());
}

#[tokio::test]
async fn agent_runtime_session_resolve_answers_rejects_numeric_fallback_collision() {
    let session = start_echo_session().await;
    let session = AgentSessionFacade::attach(
        TEST_PROVIDER_ID,
        InstanceId("instance-10".into()),
        session,
        None,
    )
    .await
    .expect("attach");
    let mut events = session.take_events().await.expect("take events");

    session
        .submit(AgentInput {
            text: "ask-fallback-collision".into(),
            images: Vec::new(),
        })
        .await
        .expect("submit fallback collision trigger");

    assert!(matches!(
        events.recv().await,
        Some(PublicEvent::InterventionRequest(InterventionRequest::Question(request)))
            if request.req_id.as_str() == "question-fallback-collision"
    ));

    let err = session
        .resolve(
            "question-fallback-collision",
            InterventionResponse::Answers(QuestionResponse {
                answers: vec![
                    QuestionAnswer {
                        values: vec!["explicit-fallback-key".into()],
                    },
                    QuestionAnswer {
                        values: vec!["fallback-slot-one".into()],
                    },
                ],
            }),
        )
        .await
        .expect_err("fallback collision should fail");
    assert!(err.message.contains("colliding question lookup keys"));
}

#[tokio::test]
async fn agent_runtime_session_failed_resolve_keeps_question_metadata_for_retry() {
    let session = start_echo_session().await;
    let session = AgentSessionFacade::attach(
        TEST_PROVIDER_ID,
        InstanceId("instance-11".into()),
        session,
        None,
    )
    .await
    .expect("attach");
    let mut events = session.take_events().await.expect("take events");

    session
        .submit(AgentInput {
            text: "ask-retry-preserve-keys".into(),
            images: Vec::new(),
        })
        .await
        .expect("submit retry-preserve-keys trigger");

    assert!(matches!(
        events.recv().await,
        Some(PublicEvent::InterventionRequest(InterventionRequest::Question(request)))
            if request.req_id.as_str() == "question-retry"
    ));

    let err = session
        .resolve(
            "question-retry",
            InterventionResponse::Answers(QuestionResponse {
                answers: vec![
                    QuestionAnswer {
                        values: vec!["fail-once".into()],
                    },
                    QuestionAnswer {
                        values: vec!["fail-once".into()],
                    },
                ],
            }),
        )
        .await
        .expect_err("first resolve should fail");
    assert!(err.message.contains("forced retryable resolve failure"));

    session
        .resolve(
            "question-retry",
            InterventionResponse::Answers(QuestionResponse {
                answers: vec![
                    QuestionAnswer {
                        values: vec!["alpha-retry".into()],
                    },
                    QuestionAnswer {
                        values: vec!["beta-retry".into()],
                    },
                ],
            }),
        )
        .await
        .expect("retry resolve should succeed");

    let payload = match events.recv().await {
        Some(PublicEvent::Reasoning(reasoning)) => reasoning
            .text
            .strip_prefix("resolve:")
            .expect("resolve payload prefix")
            .to_string(),
        other => panic!("unexpected event: {other:?}"),
    };
    let payload: Value = serde_json::from_str(&payload).expect("decode resolve payload");
    assert_eq!(payload["req_id"], "question-retry");
    assert_eq!(
        payload["answers"]["Question Retry Alpha"]["answers"],
        json!(["alpha-retry"])
    );
    assert_eq!(
        payload["answers"]["Question Retry Beta"]["answers"],
        json!(["beta-retry"])
    );
    assert!(payload["answers"].get("0").is_none());
    assert!(payload["answers"].get("1").is_none());
}

#[tokio::test]
async fn agent_runtime_session_identical_answers_still_fail_when_keys_collapse_slots() {
    let session = start_echo_session().await;
    let session = AgentSessionFacade::attach(
        TEST_PROVIDER_ID,
        InstanceId("instance-12".into()),
        session,
        None,
    )
    .await
    .expect("attach");
    let mut events = session.take_events().await.expect("take events");

    session
        .submit(AgentInput {
            text: "ask-identical-collision".into(),
            images: Vec::new(),
        })
        .await
        .expect("submit identical collision trigger");

    assert!(matches!(
        events.recv().await,
        Some(PublicEvent::InterventionRequest(InterventionRequest::Question(request)))
            if request.req_id.as_str() == "question-identical-collision"
    ));

    let err = session
        .resolve(
            "question-identical-collision",
            InterventionResponse::Answers(QuestionResponse {
                answers: vec![
                    QuestionAnswer {
                        values: vec!["same-answer".into()],
                    },
                    QuestionAnswer {
                        values: vec!["same-answer".into()],
                    },
                ],
            }),
        )
        .await
        .expect_err("identical-answer collision should fail");
    assert!(err.message.contains("colliding question lookup keys"));
}

#[tokio::test]
async fn agent_runtime_session_keyless_fallback_preserves_positional_value_order() {
    let session = start_echo_session().await;
    let session = AgentSessionFacade::attach(
        TEST_PROVIDER_ID,
        InstanceId("instance-13".into()),
        session,
        None,
    )
    .await
    .expect("attach");
    let mut events = session.take_events().await.expect("take events");

    session
        .submit(AgentInput {
            text: "ask-many-keyless".into(),
            images: Vec::new(),
        })
        .await
        .expect("submit keyless trigger");

    assert!(matches!(
        events.recv().await,
        Some(PublicEvent::InterventionRequest(InterventionRequest::Question(request)))
            if request.req_id.as_str() == "question-many-keyless"
    ));

    session
        .resolve(
            "question-many-keyless",
            InterventionResponse::Answers(QuestionResponse {
                answers: (0..12)
                    .map(|idx| QuestionAnswer {
                        values: vec![format!("keyless-{idx:02}").into()],
                    })
                    .collect(),
            }),
        )
        .await
        .expect("resolve keyless answers");

    let payload = match events.recv().await {
        Some(PublicEvent::Reasoning(reasoning)) => reasoning
            .text
            .strip_prefix("resolve:")
            .expect("resolve payload prefix")
            .to_string(),
        other => panic!("unexpected event: {other:?}"),
    };
    let payload: Value = serde_json::from_str(&payload).expect("decode resolve payload");
    let ordered_values = payload["ordered_answers"]
        .as_array()
        .expect("ordered_answers array")
        .iter()
        .map(|entry| {
            entry["answers"][0]
                .as_str()
                .expect("answer string")
                .to_string()
        })
        .collect::<Vec<_>>();
    let expected_values = (0..12)
        .map(|idx| format!("keyless-{idx:02}"))
        .collect::<Vec<_>>();
    assert_eq!(ordered_values, expected_values);
}

#[tokio::test]
async fn agent_runtime_session_projected_events_keep_source_order() {
    let (session, tx) = runtime::synthetic_session();
    tx.send(CanonicalEvent::new(Payload::SessionStarted(
        SessionStarted {
            session_id: "provider-session".into(),
            model: "model".into(),
        },
    )))
    .await
    .expect("send session started");

    let session = AgentSessionFacade::attach(
        TEST_PROVIDER_ID,
        InstanceId("instance-5".into()),
        session,
        None,
    )
    .await
    .expect("attach");
    let mut events = session.take_events().await.expect("take events");

    tx.send(CanonicalEvent::new(Payload::Timeline(Timeline {
        item: event::new_timeline_user("item-1", "one"),
    })))
    .await
    .expect("send user");
    tx.send(CanonicalEvent::new(Payload::Timeline(Timeline {
        item: event::new_timeline_reasoning("item-2", "two"),
    })))
    .await
    .expect("send reasoning");
    tx.send(CanonicalEvent::new(Payload::Timeline(Timeline {
        item: event::new_timeline_tool_call("call-1", event::shell("pwd")),
    })))
    .await
    .expect("send tool call");
    tx.send(CanonicalEvent::new(Payload::Timeline(Timeline {
        item: event::new_timeline_tool_result(
            "item-4",
            "call-1",
            event::ToolResult {
                output: "done".into(),
                ..Default::default()
            },
        ),
    })))
    .await
    .expect("send tool result");
    tx.send(CanonicalEvent::new(Payload::Timeline(Timeline {
        item: event::new_timeline_assistant("item-5", "three", true),
    })))
    .await
    .expect("send assistant");

    assert!(matches!(
        events.recv().await,
        Some(PublicEvent::Message(message)) if message.text.as_str() == "one"
    ));
    assert!(matches!(
        events.recv().await,
        Some(PublicEvent::Reasoning(reasoning)) if reasoning.text.as_str() == "two"
    ));
    assert!(matches!(
        events.recv().await,
        Some(PublicEvent::ToolCall(tool_call)) if tool_call.name == "shell"
    ));
    assert!(matches!(
        events.recv().await,
        Some(PublicEvent::ToolResult(tool_result))
            if tool_result.call_id.0.as_str() == "call-1"
    ));
    assert!(matches!(
        events.recv().await,
        Some(PublicEvent::Message(message)) if message.text.as_str() == "three"
    ));
}

#[tokio::test]
async fn agent_runtime_session_buffers_pre_readiness_events_until_session_started() {
    let (tx, rx) = mpsc::channel(2048);
    let session = runtime::Session::new_synthetic(rx, tx.clone());
    let attach = tokio::spawn(AgentSessionFacade::attach(
        TEST_PROVIDER_ID,
        InstanceId("instance-7".into()),
        session,
        None,
    ));

    for i in 0..1100 {
        tx.send(CanonicalEvent::new(Payload::Timeline(Timeline {
            item: event::new_timeline_user(&format!("item-{i}"), &format!("before-{i}")),
        })))
        .await
        .expect("send pre-readiness event");
    }
    tx.send(CanonicalEvent::new(Payload::SessionStarted(
        SessionStarted {
            session_id: "provider-session".into(),
            model: "model".into(),
        },
    )))
    .await
    .expect("send session started");

    let session = timeout(Duration::from_secs(2), attach)
        .await
        .expect("attach should complete after SessionStarted")
        .expect("join")
        .expect("attach session");
    let mut events = session.take_events().await.expect("take events");

    for i in 0..1100 {
        assert!(matches!(
            events.recv().await,
            Some(PublicEvent::Message(message)) if message.text.as_str() == format!("before-{i}")
        ));
    }
}

#[tokio::test]
async fn agent_runtime_session_close_returns_promptly_when_public_channel_is_backed_up() {
    let session = start_echo_session().await;
    let session = AgentSessionFacade::attach(
        TEST_PROVIDER_ID,
        InstanceId("instance-9".into()),
        session,
        None,
    )
    .await
    .expect("attach");

    session
        .submit(AgentInput {
            text: "flood:1200".into(),
            images: Vec::new(),
        })
        .await
        .expect("submit flood");
    tokio::time::sleep(Duration::from_millis(100)).await;

    timeout(Duration::from_secs(2), session.close())
        .await
        .expect("close should return promptly")
        .expect("close result");
}

#[tokio::test]
async fn agent_runtime_session_auto_closes_after_idle_timeout() {
    let session = start_echo_session().await;
    let session = AgentSessionFacade::attach_with_options(
        TEST_PROVIDER_ID,
        InstanceId("instance-idle".into()),
        session,
        None,
        None,
        AgentSessionOptions::with_idle_timeout(Some(Duration::from_millis(30))),
    )
    .await
    .expect("attach");
    let mut events = session.take_events().await.expect("take events");

    let closed = timeout(Duration::from_secs(2), events.recv())
        .await
        .expect("idle close should close public stream");
    assert!(closed.is_none(), "expected stream close, got {closed:?}");
    assert_eq!(
        session.observed_close_reason().await.as_deref(),
        Some("idle timeout")
    );
}

#[tokio::test(start_paused = true)]
async fn agent_runtime_session_idle_timeout_resets_after_stdio_event() {
    let (session, tx) = runtime::synthetic_session();
    tx.send(CanonicalEvent::new(Payload::SessionStarted(
        SessionStarted {
            session_id: "provider-session".into(),
            model: "model".into(),
        },
    )))
    .await
    .expect("send session started");
    let session = AgentSessionFacade::attach_with_options(
        TEST_PROVIDER_ID,
        InstanceId("instance-idle-stdio-activity".into()),
        session,
        None,
        None,
        AgentSessionOptions::with_idle_timeout(Some(Duration::from_millis(80))),
    )
    .await
    .expect("attach");
    let mut events = session.take_events().await.expect("take events");

    tokio::task::yield_now().await;
    advance(Duration::from_millis(50)).await;
    tx.send(CanonicalEvent::new(Payload::Timeline(Timeline {
        item: event::new_timeline_user("keepalive", "stdio keepalive"),
    })))
    .await
    .expect("send keepalive event");

    assert!(matches!(
        timeout(Duration::from_secs(1), events.recv())
            .await
            .expect("keepalive event should be delivered"),
        Some(PublicEvent::Message(message)) if message.text.as_str() == "stdio keepalive"
    ));
    advance(Duration::from_millis(50)).await;
    tokio::task::yield_now().await;
    assert_eq!(
        session.observed_close_reason().await.as_deref(),
        None,
        "stdio event should reset the idle timeout window"
    );

    advance(Duration::from_millis(31)).await;
    tokio::task::yield_now().await;
    assert_eq!(
        session.observed_close_reason().await.as_deref(),
        Some("idle timeout")
    );
}

#[tokio::test]
async fn agent_runtime_session_no_output_ack_command_returns_to_idle() {
    let session = start_echo_session().await;
    let session = AgentSessionFacade::attach_with_options(
        TEST_PROVIDER_ID,
        InstanceId("instance-idle-after-quiet-command".into()),
        session,
        None,
        None,
        AgentSessionOptions::with_idle_timeout(Some(Duration::from_millis(30))),
    )
    .await
    .expect("attach");
    let mut events = session.take_events().await.expect("take events");

    session
        .invoke_command(AgentCommandInvocation {
            name: "quiet".into(),
            args: None,
            values: serde_json::Value::Null,
            source: AgentCommandSource::ProviderNative,
        })
        .await
        .expect("invoke quiet command");

    let closed = timeout(Duration::from_secs(2), events.recv())
        .await
        .expect("idle close should close public stream after no-output command");
    assert!(closed.is_none(), "expected stream close, got {closed:?}");
    assert_eq!(
        session.observed_close_reason().await.as_deref(),
        Some("idle timeout")
    );
}

#[tokio::test]
async fn agent_runtime_session_idle_timeout_can_be_disabled() {
    let session = start_echo_session().await;
    let session = AgentSessionFacade::attach_with_options(
        TEST_PROVIDER_ID,
        InstanceId("instance-idle-disabled".into()),
        session,
        None,
        None,
        AgentSessionOptions::with_idle_timeout(None),
    )
    .await
    .expect("attach");
    let mut events = session.take_events().await.expect("take events");

    assert!(
        timeout(Duration::from_millis(80), events.recv())
            .await
            .is_err(),
        "disabled idle timeout should not close the stream"
    );
    session.close().await.expect("close");
}

#[tokio::test]
async fn agent_runtime_session_filter_updates_drain_buffered_old_events_before_switching() {
    let (tx, rx) = mpsc::channel(2048);
    let session = runtime::Session::new_synthetic(rx, tx.clone());
    tx.send(CanonicalEvent::new(Payload::SessionStarted(
        SessionStarted {
            session_id: "provider-session".into(),
            model: "model".into(),
        },
    )))
    .await
    .expect("send session started");

    let session = AgentSessionFacade::attach(
        TEST_PROVIDER_ID,
        InstanceId("instance-14".into()),
        session,
        None,
    )
    .await
    .expect("attach");
    let mut events = session.take_events().await.expect("take events");

    session
        .update_runtime_filter(RuntimeBusFilter {
            assistant_messages: true,
            ..Default::default()
        })
        .await
        .expect("enable assistant messages");

    for idx in 0..1025 {
        tx.send(CanonicalEvent::new(Payload::Timeline(Timeline {
            item: event::new_timeline_assistant(
                &format!("assistant-{idx}"),
                &format!("before-update-{idx}"),
                true,
            ),
        })))
        .await
        .expect("send assistant backlog");
    }
    tx.send(CanonicalEvent::new(Payload::Timeline(Timeline {
        item: event::new_timeline_user("user-before-update", "before-update-user"),
    })))
    .await
    .expect("send pre-update user event");

    timeout(
        Duration::from_secs(2),
        session.update_runtime_filter(RuntimeBusFilter {
            user_messages: true,
            ..Default::default()
        }),
    )
    .await
    .expect("filter update should not deadlock behind a full public queue")
    .expect("apply user filter");

    for idx in 0..1025 {
        assert!(matches!(
            events.recv().await,
            Some(PublicEvent::Message(message))
                if message.text.as_str() == format!("before-update-{idx}")
        ));
    }
    assert!(
        timeout(Duration::from_millis(100), events.recv())
            .await
            .is_err(),
        "pre-update user event should stay filtered under the old filter"
    );

    tx.send(CanonicalEvent::new(Payload::Timeline(Timeline {
        item: event::new_timeline_user("user-after-update", "after-update-user"),
    })))
    .await
    .expect("send post-update user event");

    assert!(matches!(
        events.recv().await,
        Some(PublicEvent::Message(message)) if message.text.as_str() == "after-update-user"
    ));
}

#[derive(Default)]
struct EchoDialect {
    seq: usize,
}

impl Dialect for EchoDialect {
    fn name(&self) -> &'static str {
        "echo-test"
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
                    session_id: value["session_id"].as_str().expect("session_id").into(),
                    model: "echo-model".into(),
                },
            ))],
            "quiet" => Vec::new(),
            "submit" if value["text"].as_str() == Some("ask-duplicate-header-question") => {
                vec![CanonicalEvent::new(Payload::PermissionRequest(
                    PermissionRequest {
                        req_id: "question-dup".into(),
                        tool: "request_user_input".into(),
                        input: Some(json!({
                            "questions":[
                                {
                                    "id":"",
                                    "header":"Shared Header",
                                    "question":"Question Alpha",
                                    "options":[],
                                    "multi_select":false
                                },
                                {
                                    "id":"",
                                    "header":"Shared Header",
                                    "question":"Question Beta",
                                    "options":[],
                                    "multi_select":false
                                }
                            ]
                        })),
                        risk: lucarne::event::Risk::Low,
                        questions: vec![
                            PermissionQuestion {
                                id: String::new(),
                                header: "Shared Header".into(),
                                question: "Question Alpha".into(),
                                options: vec![],
                                multi_select: false,
                                is_other: false,
                                is_secret: false,
                            },
                            PermissionQuestion {
                                id: String::new(),
                                header: "Shared Header".into(),
                                question: "Question Beta".into(),
                                options: vec![],
                                multi_select: false,
                                is_other: false,
                                is_secret: false,
                            },
                        ],
                    },
                ))]
            }
            "submit" if value["text"].as_str() == Some("ask-fallback-collision") => {
                vec![CanonicalEvent::new(Payload::PermissionRequest(
                    PermissionRequest {
                        req_id: "question-fallback-collision".into(),
                        tool: "request_user_input".into(),
                        input: Some(json!({
                            "questions":[
                                {
                                    "id":"__lucarne_pos_00000000000000000001",
                                    "header":"",
                                    "question":"",
                                    "options":[],
                                    "multi_select":false
                                },
                                {
                                    "id":"",
                                    "header":"",
                                    "question":"",
                                    "options":[],
                                    "multi_select":false
                                }
                            ]
                        })),
                        risk: lucarne::event::Risk::Low,
                        questions: vec![
                            PermissionQuestion {
                                id: "__lucarne_pos_00000000000000000001".into(),
                                header: String::new(),
                                question: String::new(),
                                options: vec![],
                                multi_select: false,
                                is_other: false,
                                is_secret: false,
                            },
                            PermissionQuestion {
                                id: String::new(),
                                header: String::new(),
                                question: String::new(),
                                options: vec![],
                                multi_select: false,
                                is_other: false,
                                is_secret: false,
                            },
                        ],
                    },
                ))]
            }
            "submit" if value["text"].as_str() == Some("ask-retry-preserve-keys") => {
                vec![CanonicalEvent::new(Payload::PermissionRequest(
                    PermissionRequest {
                        req_id: "question-retry".into(),
                        tool: "request_user_input".into(),
                        input: Some(json!({
                            "questions":[
                                {
                                    "id":"",
                                    "header":"Shared Retry Header",
                                    "question":"Question Retry Alpha",
                                    "options":[],
                                    "multi_select":false
                                },
                                {
                                    "id":"",
                                    "header":"Shared Retry Header",
                                    "question":"Question Retry Beta",
                                    "options":[],
                                    "multi_select":false
                                }
                            ]
                        })),
                        risk: lucarne::event::Risk::Low,
                        questions: vec![
                            PermissionQuestion {
                                id: String::new(),
                                header: "Shared Retry Header".into(),
                                question: "Question Retry Alpha".into(),
                                options: vec![],
                                multi_select: false,
                                is_other: false,
                                is_secret: false,
                            },
                            PermissionQuestion {
                                id: String::new(),
                                header: "Shared Retry Header".into(),
                                question: "Question Retry Beta".into(),
                                options: vec![],
                                multi_select: false,
                                is_other: false,
                                is_secret: false,
                            },
                        ],
                    },
                ))]
            }
            "submit" if value["text"].as_str() == Some("ask-identical-collision") => {
                vec![CanonicalEvent::new(Payload::PermissionRequest(
                    PermissionRequest {
                        req_id: "question-identical-collision".into(),
                        tool: "request_user_input".into(),
                        input: Some(json!({
                            "questions":[
                                {
                                    "id":"",
                                    "header":"Shared Only",
                                    "question":"",
                                    "options":[],
                                    "multi_select":false
                                },
                                {
                                    "id":"",
                                    "header":"Shared Only",
                                    "question":"",
                                    "options":[],
                                    "multi_select":false
                                }
                            ]
                        })),
                        risk: lucarne::event::Risk::Low,
                        questions: vec![
                            PermissionQuestion {
                                id: String::new(),
                                header: "Shared Only".into(),
                                question: String::new(),
                                options: vec![],
                                multi_select: false,
                                is_other: false,
                                is_secret: false,
                            },
                            PermissionQuestion {
                                id: String::new(),
                                header: "Shared Only".into(),
                                question: String::new(),
                                options: vec![],
                                multi_select: false,
                                is_other: false,
                                is_secret: false,
                            },
                        ],
                    },
                ))]
            }
            "submit" if value["text"].as_str() == Some("ask-many-keyless") => {
                vec![CanonicalEvent::new(Payload::PermissionRequest(
                    PermissionRequest {
                        req_id: "question-many-keyless".into(),
                        tool: "request_user_input".into(),
                        input: Some(json!({
                            "questions": (0..12).map(|_| json!({
                                "id":"",
                                "header":"",
                                "question":"",
                                "options":[],
                                "multi_select":false
                            })).collect::<Vec<_>>()
                        })),
                        risk: lucarne::event::Risk::Low,
                        questions: (0..12)
                            .map(|_| PermissionQuestion {
                                id: String::new(),
                                header: String::new(),
                                question: String::new(),
                                options: vec![],
                                multi_select: false,
                                is_other: false,
                                is_secret: false,
                            })
                            .collect(),
                    },
                ))]
            }
            "submit" if value["text"].as_str() == Some("ask-question") => {
                vec![CanonicalEvent::new(Payload::PermissionRequest(
                    PermissionRequest {
                        req_id: "question-1".into(),
                        tool: "request_user_input".into(),
                        input: Some(json!({
                            "questions":[
                                {
                                    "id":"",
                                    "header":"Header 1",
                                    "question":"Question 1",
                                    "options":[{"label":"A","description":"first"}],
                                    "multi_select":false
                                },
                                {
                                    "id":"",
                                    "header":"",
                                    "question":"Question 2",
                                    "options":[],
                                    "multi_select":true
                                }
                            ]
                        })),
                        risk: lucarne::event::Risk::Low,
                        questions: vec![
                            PermissionQuestion {
                                id: String::new(),
                                header: "Header 1".into(),
                                question: "Question 1".into(),
                                options: vec![PermissionQuestionOption {
                                    label: "A".into(),
                                    description: "first".into(),
                                }],
                                multi_select: false,
                                is_other: false,
                                is_secret: false,
                            },
                            PermissionQuestion {
                                id: String::new(),
                                header: String::new(),
                                question: "Question 2".into(),
                                options: vec![],
                                multi_select: true,
                                is_other: false,
                                is_secret: false,
                            },
                        ],
                    },
                ))]
            }
            "submit"
                if value["text"]
                    .as_str()
                    .is_some_and(|text| text.starts_with("flood:")) =>
            {
                let count = value["text"]
                    .as_str()
                    .and_then(|text| text.split_once(':'))
                    .and_then(|(_, count)| count.parse::<usize>().ok())
                    .expect("flood count");
                (0..count)
                    .map(|idx| {
                        CanonicalEvent::new(Payload::Timeline(Timeline {
                            item: event::new_timeline_user(
                                &format!("flood-{idx}"),
                                &format!("flood:{idx}"),
                            ),
                        }))
                    })
                    .collect()
            }
            "submit" => vec![CanonicalEvent::new(Payload::Timeline(Timeline {
                item: event::new_timeline_user(
                    &self.next_id(),
                    &format!(
                        "submit:{}{}",
                        value["text"].as_str().unwrap(),
                        image_debug_suffix(value.get("images"))
                    ),
                ),
            }))],
            "resolve" => vec![CanonicalEvent::new(Payload::Timeline(Timeline {
                item: event::new_timeline_reasoning(
                    &self.next_id(),
                    &format!(
                        "resolve:{}",
                        serde_json::to_string(&value["payload"]).unwrap()
                    ),
                ),
            }))],
            "interrupt" => vec![CanonicalEvent::new(Payload::Timeline(Timeline {
                item: event::new_timeline_reasoning(&self.next_id(), "interrupt"),
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

    fn command_catalog(&self) -> AgentCommandCatalog {
        AgentCommandCatalog {
            commands: vec![
                AgentCommand {
                    name: "model".into(),
                    description: Some("List models".into()),
                    aliases: Vec::new(),
                    source: AgentCommandSource::AdapterMapped,
                    input: AgentCommandInput::None,
                    completion: AgentCommandCompletion::CommandResult,
                },
                AgentCommand {
                    name: "quiet".into(),
                    description: Some("No-output command".into()),
                    aliases: Vec::new(),
                    source: AgentCommandSource::ProviderNative,
                    input: AgentCommandInput::None,
                    completion: AgentCommandCompletion::NoOutputAck,
                },
            ],
            complete: true,
            revision: 1,
        }
    }

    fn handle_system_command(
        &mut self,
        command: &lucarne::agent_runtime::AgentCommandInvocation,
    ) -> lucarne::Result<CommandDispatch> {
        match command.name.as_str() {
            "model" => Ok(CommandDispatch::ready(CommandResult::Models(
                lucarne::agent_runtime::AgentModelCatalog {
                    current_model: Some("echo-model".into()),
                    current_reasoning: None,
                    models: vec![AgentModelOption {
                        id: "echo-model".into(),
                        display_name: Some("Echo Model".into()),
                        description: None,
                        supported_reasoning: Vec::new(),
                    }],
                },
            ))),
            other => Err(lucarne::LucarneError::dialect(format!(
                "unsupported test system command {other}"
            ))),
        }
    }

    fn handle_native_command(
        &mut self,
        command: &lucarne::agent_runtime::AgentCommandInvocation,
    ) -> lucarne::Result<CommandDispatch> {
        match command.name.as_str() {
            "quiet" => Ok(CommandDispatch::deferred(vec![OutFrame::stdin(
                frame_line(json!({"kind":"quiet"})),
            )])),
            other => Err(lucarne::LucarneError::dialect(format!(
                "unsupported test native command {other}"
            ))),
        }
    }

    fn encode_permission_response(
        &mut self,
        req_id: &str,
        resp: &PermissionResponse,
    ) -> lucarne::Result<Vec<OutFrame>> {
        if req_id == "question-retry"
            && resp
                .answers
                .values()
                .all(|answer| answer.answers == vec!["fail-once".to_string()])
        {
            return Err(lucarne::LucarneError::dialect(
                "forced retryable resolve failure",
            ));
        }
        let decision = match resp.decision {
            lucarne::event::Decision::Allow => "allow",
            lucarne::event::Decision::Deny => "deny",
        };
        let mut payload = json!({
            "req_id": req_id,
            "decision": decision,
            "answers": resp.answers,
        });
        if req_id == "question-many-keyless" {
            payload["ordered_answers"] = json!(resp.answers.values().cloned().collect::<Vec<_>>());
        }
        Ok(vec![OutFrame::stdin(frame_line(json!({
            "kind":"resolve",
            "payload": payload
        })))])
    }

    fn encode_interrupt(&mut self) -> lucarne::Result<Vec<OutFrame>> {
        Ok(vec![OutFrame::stdin(frame_line(
            json!({"kind":"interrupt"}),
        ))])
    }
}

impl EchoDialect {
    fn next_id(&mut self) -> String {
        self.seq += 1;
        format!("event-{}", self.seq)
    }
}

#[derive(Default)]
struct DelayedStartDialect {
    seq: usize,
    started: bool,
}

impl Dialect for DelayedStartDialect {
    fn name(&self) -> &'static str {
        "delayed-start-test"
    }

    fn init(&mut self, _cfg: &SessionParams) -> Vec<OutFrame> {
        Vec::new()
    }

    fn translate(&mut self, frame_bytes: &[u8]) -> Vec<CanonicalEvent> {
        let value: Value = serde_json::from_slice(frame_bytes).expect("decode frame");
        match value["kind"].as_str().expect("frame kind") {
            "submit" => {
                let mut events = Vec::new();
                if !self.started {
                    self.started = true;
                    events.push(CanonicalEvent::new(Payload::SessionStarted(
                        SessionStarted {
                            session_id: "provider-session-delayed".into(),
                            model: "echo-model".into(),
                        },
                    )));
                }
                events.push(CanonicalEvent::new(Payload::Timeline(Timeline {
                    item: event::new_timeline_user(
                        &self.next_id(),
                        &format!("submit:{}", value["text"].as_str().unwrap()),
                    ),
                })));
                events
            }
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
        _req_id: &str,
        _resp: &PermissionResponse,
    ) -> lucarne::Result<Vec<OutFrame>> {
        Ok(Vec::new())
    }

    fn encode_interrupt(&mut self) -> lucarne::Result<Vec<OutFrame>> {
        Ok(Vec::new())
    }
}

impl DelayedStartDialect {
    fn next_id(&mut self) -> String {
        self.seq += 1;
        format!("delayed-event-{}", self.seq)
    }
}

async fn start_echo_session() -> runtime::Session {
    runtime::start(RuntimeConfig {
        launcher: Arc::new(LocalLauncher::new()),
        spec: LaunchSpec {
            bin: "/bin/sh".into(),
            args: vec!["-c".into(), "cat".into()],
            cwd: std::env::current_dir()
                .expect("current dir")
                .display()
                .to_string(),
            ..Default::default()
        },
        framer: Framer::newline_json(),
        dialect: Box::new(EchoDialect::default()),
        session_params: SessionParams::default(),
        buffer_size: 32,
        interrupt_grace: Duration::from_millis(100),
    })
    .await
    .expect("start runtime session")
}

async fn start_delayed_start_session() -> runtime::Session {
    runtime::start(RuntimeConfig {
        launcher: Arc::new(LocalLauncher::new()),
        spec: LaunchSpec {
            bin: "/bin/sh".into(),
            args: vec!["-c".into(), "cat".into()],
            cwd: std::env::current_dir()
                .expect("current dir")
                .display()
                .to_string(),
            ..Default::default()
        },
        framer: Framer::newline_json(),
        dialect: Box::new(DelayedStartDialect::default()),
        session_params: SessionParams::default(),
        buffer_size: 32,
        interrupt_grace: Duration::from_millis(100),
    })
    .await
    .expect("start delayed-start runtime session")
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
