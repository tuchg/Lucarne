#![allow(clippy::useless_conversion)]

use async_trait::async_trait;
use lucarne::agent_runtime::{
    AgentError, AgentErrorKind, AgentInput, AgentRuntime, AgentSession, AgentSessionFacade,
    CommandId, Event, InstanceId, MessageEvent, MessageRole, OpenSession, ResumeSession,
    RuntimeBusFilter, RuntimeBusOutput, RuntimeCommand, SessionId, SessionOpenedEvent, SessionRef,
};
use lucarne::event::{self, Event as CanonicalEvent, Payload, SessionStarted, Timeline};
use lucarne::runtime;
use lucarne::ProviderId;
use smol_str::SmolStr;
use std::sync::{
    atomic::{AtomicUsize, Ordering},
    Arc, Mutex as StdMutex,
};
use tokio::{
    sync::{mpsc, Notify},
    time::{timeout, Duration},
};

const TEST_PROVIDER_ID: ProviderId = ProviderId::from_static("test");

#[test]
fn agent_runtime_bus_metadata_uses_synchronous_locks_only() {
    let source = std::fs::read_to_string(
        std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("src/agent_runtime/runtime.rs"),
    )
    .expect("read runtime source");

    for forbidden in [
        "events_rx: Mutex<Option<RuntimeBusStream>>",
        "sessions: Mutex<HashMap<InstanceId, Arc<ManagedSessionInner>>>",
        "filter: Arc<Mutex<RuntimeBusFilter>>",
        "use tokio::sync::{mpsc, oneshot, Mutex}",
    ] {
        assert!(
            !source.contains(forbidden),
            "runtime bus metadata should not require async mutexes: {forbidden}"
        );
    }
}

#[test]
fn runtime_bus_filter_is_copy_and_not_cloned_on_runtime_hot_paths() {
    fn assert_copy<T: Copy>() {}
    assert_copy::<RuntimeBusFilter>();

    let source = std::fs::read_to_string(
        std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("src/agent_runtime/runtime.rs"),
    )
    .expect("read runtime source");

    assert!(
        !source.contains("filter.clone()"),
        "RuntimeBusFilter is a bool-only policy and should be passed by value on runtime hot paths"
    );
}

#[tokio::test]
async fn agent_runtime_bus_rejects_unknown_provider() {
    let runtime = AgentRuntime::new();
    let bus = runtime.bus();
    let mut events = bus.take_events().await.expect("take bus events");

    bus.command(RuntimeCommand::Open {
        command_id: CommandId("cmd-open-unknown".into()),
        provider_id: ProviderId::from_static("missing"),
        req: OpenSession::default(),
    })
    .await
    .expect("enqueue open command");

    let output = recv_bus_output(&mut events).await;
    assert!(matches!(
        output,
        RuntimeBusOutput::CommandRejected(rejected)
            if rejected.command_id == Some(CommandId("cmd-open-unknown".into()))
                && rejected.instance_id.is_none()
                && rejected.session_id.is_none()
                && rejected.message.contains("unknown provider")
    ));
}

#[tokio::test]
async fn agent_runtime_bus_rejects_unknown_instance() {
    let runtime = AgentRuntime::new();
    let bus = runtime.bus();
    let mut events = bus.take_events().await.expect("take bus events");

    bus.command(RuntimeCommand::Submit {
        instance_id: InstanceId("missing-instance".into()),
        input: AgentInput {
            text: "hello".into(),
            images: Vec::new(),
        },
    })
    .await
    .expect("enqueue submit command");

    let output = recv_bus_output(&mut events).await;
    assert!(matches!(
        output,
        RuntimeBusOutput::CommandRejected(rejected)
            if rejected.command_id.is_none()
                && rejected.session_id.is_none()
                && rejected.instance_id == Some(InstanceId("missing-instance".into()))
                && rejected.message.contains("unknown instance")
    ));
}

#[tokio::test]
async fn agent_runtime_bus_take_events_is_single_consumer() {
    let runtime = AgentRuntime::new();
    let provider = Arc::new(TestProvider::new("test"));
    runtime.register(provider);
    let bus = runtime.bus();

    let mut events = bus.take_events().await.expect("first take bus events");
    bus.command(RuntimeCommand::Open {
        command_id: CommandId("cmd-open-single-consumer".into()),
        provider_id: TEST_PROVIDER_ID,
        req: OpenSession::default(),
    })
    .await
    .expect("enqueue open command");

    let opened = expect_session_opened(&mut events, "cmd-open-single-consumer").await;
    assert_eq!(opened.provider_id, TEST_PROVIDER_ID);

    let err = bus
        .take_events()
        .await
        .expect_err("second take_events should fail");

    assert_eq!(err.kind, AgentErrorKind::InvalidState);
    assert!(err.message.contains("already taken"));
}

#[tokio::test]
async fn agent_runtime_bus_emits_lifecycle_outputs_with_default_filter() {
    let runtime = AgentRuntime::new();
    let provider = Arc::new(TestProvider::new("test"));
    runtime.register(provider);
    let bus = runtime.bus();
    let mut events = bus.take_events().await.expect("take bus events");

    bus.command(RuntimeCommand::Open {
        command_id: CommandId("cmd-open-lifecycle".into()),
        provider_id: TEST_PROVIDER_ID,
        req: OpenSession::default(),
    })
    .await
    .expect("enqueue open command");

    let opened = expect_session_opened(&mut events, "cmd-open-lifecycle").await;

    bus.command(RuntimeCommand::Close {
        instance_id: opened.instance_id.clone(),
    })
    .await
    .expect("enqueue close command");

    let output = recv_bus_output(&mut events).await;
    assert!(matches!(
        output,
        RuntimeBusOutput::SessionClosed(closed)
            if closed.instance_id == opened.instance_id
                && closed.session_id == opened.session_id
                && closed.provider_id == TEST_PROVIDER_ID
                && closed.reason.as_str() == "test session closed"
    ));

    bus.command(RuntimeCommand::Submit {
        instance_id: InstanceId("missing-after-close".into()),
        input: AgentInput {
            text: "ignored".into(),
            images: Vec::new(),
        },
    })
    .await
    .expect("enqueue submit command");

    let output = recv_bus_output(&mut events).await;
    assert!(matches!(
        output,
        RuntimeBusOutput::CommandRejected(rejected)
            if rejected.command_id.is_none()
                && rejected.session_id.is_none()
                && rejected.instance_id == Some(InstanceId("missing-after-close".into()))
                && rejected.message.contains("unknown instance")
    ));
}

#[tokio::test]
async fn agent_runtime_bus_resume_routes_to_provider_resume() {
    let runtime = AgentRuntime::new();
    let provider = Arc::new(TestProvider::new("test"));
    runtime.register(provider.clone());
    let bus = runtime.bus();
    let mut events = bus.take_events().await.expect("take bus events");

    bus.command(RuntimeCommand::UpdateFilter {
        filter: RuntimeBusFilter {
            assistant_messages: true,
            ..Default::default()
        },
    })
    .await
    .expect("enable assistant messages");

    bus.command(RuntimeCommand::Resume {
        command_id: CommandId("cmd-resume-routed".into()),
        provider_id: TEST_PROVIDER_ID,
        req: ResumeSession {
            session_ref: SessionRef("resume-ref-1".into()),
            idle_timeout_ms: None,
            args: serde_json::Value::Null,
        },
    })
    .await
    .expect("enqueue resume command");

    let opened = expect_session_opened(&mut events, "cmd-resume-routed").await;
    assert_eq!(provider.resume_refs(), vec!["resume-ref-1".to_string()]);
    assert_eq!(provider.open_count(), 0);

    bus.command(RuntimeCommand::Submit {
        instance_id: opened.instance_id.clone(),
        input: AgentInput {
            text: "after-resume".into(),
            images: Vec::new(),
        },
    })
    .await
    .expect("enqueue submit after resume");

    let output = recv_bus_output(&mut events).await;
    assert!(matches!(
        output,
        RuntimeBusOutput::Event(bus_event)
            if bus_event.instance_id == opened.instance_id
                && bus_event.session_id == opened.session_id
                && matches!(
                    &bus_event.event,
                    Event::Message(MessageEvent {
                        role: MessageRole::Assistant,
                        text,
                        ..
                    }) if text.as_str() == "submitted:after-resume"
                )
    ));
}

#[tokio::test]
async fn agent_runtime_bus_respects_session_lifecycle_filter() {
    let runtime = AgentRuntime::new();
    let provider = Arc::new(TestProvider::new("test"));
    provider.queue_open_events(vec![assistant_message("still-visible")]);
    runtime.register(provider);
    let bus = runtime.bus();
    let mut events = bus.take_events().await.expect("take bus events");

    bus.command(RuntimeCommand::UpdateFilter {
        filter: RuntimeBusFilter {
            session_lifecycle: false,
            assistant_messages: true,
            ..Default::default()
        },
    })
    .await
    .expect("disable lifecycle outputs");

    bus.command(RuntimeCommand::Open {
        command_id: CommandId("cmd-open-no-lifecycle".into()),
        provider_id: TEST_PROVIDER_ID,
        req: OpenSession::default(),
    })
    .await
    .expect("enqueue open command");

    let output = recv_bus_output(&mut events).await;
    assert!(matches!(
        output,
        RuntimeBusOutput::Event(bus_event)
            if matches!(
                &bus_event.event,
                Event::Message(MessageEvent {
                    role: MessageRole::Assistant,
                    text,
                    ..
                }) if text.as_str() == "still-visible"
            )
    ));

    bus.command(RuntimeCommand::Close {
        instance_id: InstanceId("instance-0".into()),
    })
    .await
    .expect("enqueue close command");

    assert!(
        timeout(Duration::from_millis(100), events.recv())
            .await
            .is_err(),
        "session lifecycle outputs should stay filtered"
    );
}

#[tokio::test]
async fn agent_runtime_bus_rejects_direct_session_instances() {
    let runtime = AgentRuntime::new();
    let provider = Arc::new(TestProvider::new("test"));
    runtime.register(provider);
    let direct = runtime
        .open("test", OpenSession::default())
        .await
        .expect("open direct session");
    let direct_instance_id = direct.instance_id().clone();
    let bus = runtime.bus();
    let mut events = bus.take_events().await.expect("take bus events");

    bus.command(RuntimeCommand::UpdateFilter {
        filter: RuntimeBusFilter {
            assistant_messages: true,
            ..Default::default()
        },
    })
    .await
    .expect("enqueue filter update");

    bus.command(RuntimeCommand::Submit {
        instance_id: direct_instance_id.clone(),
        input: AgentInput {
            text: "bus".into(),
            images: Vec::new(),
        },
    })
    .await
    .expect("enqueue submit command");

    let output = recv_bus_output(&mut events).await;
    assert!(matches!(
        output,
        RuntimeBusOutput::CommandRejected(rejected)
            if rejected.command_id.is_none()
                && rejected.session_id.is_none()
                && rejected.instance_id == Some(direct_instance_id.clone())
                && rejected.message.contains("unknown instance")
    ));

    direct.close().await.expect("close direct session");
}

#[tokio::test]
async fn agent_runtime_bus_filter_updates_only_affect_subsequent_events() {
    let runtime = AgentRuntime::new();
    let provider = Arc::new(TestProvider::new("test"));
    runtime.register(provider.clone());
    let bus = runtime.bus();
    let mut events = bus.take_events().await.expect("take bus events");

    bus.command(RuntimeCommand::Open {
        command_id: CommandId("cmd-open-1".into()),
        provider_id: TEST_PROVIDER_ID,
        req: OpenSession::default(),
    })
    .await
    .expect("enqueue open command");

    let opened = expect_session_opened(&mut events, "cmd-open-1").await;
    let controller = provider.controller(0).await;

    controller
        .emit(Event::Message(MessageEvent {
            role: MessageRole::User,
            text: "before-filter".into(),
            streaming: false,
        }))
        .await;
    assert!(
        timeout(Duration::from_millis(100), events.recv())
            .await
            .is_err(),
        "user event should be filtered before UpdateFilter"
    );

    bus.command(RuntimeCommand::UpdateFilter {
        filter: RuntimeBusFilter {
            user_messages: true,
            ..Default::default()
        },
    })
    .await
    .expect("enqueue filter update");

    controller
        .emit(Event::Message(MessageEvent {
            role: MessageRole::User,
            text: "after-filter".into(),
            streaming: false,
        }))
        .await;

    let output = recv_bus_output(&mut events).await;
    match output {
        RuntimeBusOutput::Event(bus_event) => {
            assert_eq!(bus_event.instance_id, opened.instance_id);
            assert!(matches!(
                &bus_event.event,
                Event::Message(MessageEvent {
                    role: MessageRole::User,
                    text,
                    ..
                }) if text.as_str() == "after-filter"
            ));
        }
        other => panic!("unexpected output: {other:?}"),
    }
}

#[tokio::test]
async fn agent_runtime_bus_update_filter_returns_when_event_queue_is_full() {
    let runtime = AgentRuntime::new();
    let provider = Arc::new(TestProvider::new("test"));
    runtime.register(provider.clone());
    let bus = runtime.bus();
    let mut events = bus.take_events().await.expect("take bus events");

    bus.command(RuntimeCommand::UpdateFilter {
        filter: RuntimeBusFilter {
            assistant_messages: true,
            ..Default::default()
        },
    })
    .await
    .expect("enable assistant messages");

    bus.command(RuntimeCommand::Open {
        command_id: CommandId("cmd-open-full-queue".into()),
        provider_id: TEST_PROVIDER_ID,
        req: OpenSession::default(),
    })
    .await
    .expect("enqueue open command");

    let _opened = expect_session_opened(&mut events, "cmd-open-full-queue").await;
    let controller = provider.controller(0).await;

    for idx in 0..128 {
        controller
            .emit(assistant_message(&format!("queued-{idx}")))
            .await;
    }

    timeout(
        Duration::from_secs(2),
        bus.command(RuntimeCommand::UpdateFilter {
            filter: RuntimeBusFilter {
                user_messages: true,
                ..Default::default()
            },
        }),
    )
    .await
    .expect("update filter should not deadlock")
    .expect("update filter command");
}

#[tokio::test]
async fn agent_runtime_bus_update_filter_returns_while_open_session_is_not_started() {
    let runtime = AgentRuntime::new();
    let provider = Arc::new(TestProvider::new("test"));
    runtime.register(provider.clone());
    let bus = runtime.bus();
    let mut events = bus.take_events().await.expect("take bus events");

    bus.command(RuntimeCommand::UpdateFilter {
        filter: RuntimeBusFilter {
            assistant_messages: true,
            ..Default::default()
        },
    })
    .await
    .expect("enable assistant messages");

    bus.command(RuntimeCommand::Open {
        command_id: CommandId("cmd-open-fill-a".into()),
        provider_id: TEST_PROVIDER_ID,
        req: OpenSession::default(),
    })
    .await
    .expect("enqueue first open");
    let _opened_a = expect_session_opened(&mut events, "cmd-open-fill-a").await;
    let controller_a = provider.controller(0).await;

    for idx in 0..128 {
        controller_a
            .emit(assistant_message(&format!("fill-{idx}")))
            .await;
    }

    bus.command(RuntimeCommand::Open {
        command_id: CommandId("cmd-open-fill-b".into()),
        provider_id: TEST_PROVIDER_ID,
        req: OpenSession::default(),
    })
    .await
    .expect("enqueue second open");
    tokio::time::sleep(Duration::from_millis(50)).await;

    timeout(
        Duration::from_secs(2),
        bus.command(RuntimeCommand::UpdateFilter {
            filter: RuntimeBusFilter {
                user_messages: true,
                ..Default::default()
            },
        }),
    )
    .await
    .expect("update filter should not deadlock on pending SessionOpened")
    .expect("update filter command");
}

#[tokio::test]
async fn agent_runtime_bus_excluded_event_classes_do_not_consume_queue_slots() {
    let runtime = AgentRuntime::new();
    let provider = Arc::new(TestProvider::new("test"));
    runtime.register(provider.clone());
    let bus = runtime.bus();
    let mut events = bus.take_events().await.expect("take bus events");

    bus.command(RuntimeCommand::UpdateFilter {
        filter: RuntimeBusFilter {
            assistant_messages: true,
            ..Default::default()
        },
    })
    .await
    .expect("enqueue filter update");

    bus.command(RuntimeCommand::Open {
        command_id: CommandId("cmd-open-2".into()),
        provider_id: TEST_PROVIDER_ID,
        req: OpenSession::default(),
    })
    .await
    .expect("enqueue open command");

    let _opened = expect_session_opened(&mut events, "cmd-open-2").await;
    let controller = provider.controller(0).await;

    for idx in 0..256 {
        controller
            .emit(Event::Message(MessageEvent {
                role: MessageRole::User,
                text: format!("filtered-{idx}").into(),
                streaming: false,
            }))
            .await;
    }
    controller
        .emit(Event::Message(MessageEvent {
            role: MessageRole::Assistant,
            text: "visible".into(),
            streaming: false,
        }))
        .await;

    let output = recv_bus_output(&mut events).await;
    match output {
        RuntimeBusOutput::Event(bus_event) => {
            assert!(matches!(
                &bus_event.event,
                Event::Message(MessageEvent {
                    role: MessageRole::Assistant,
                    text,
                    ..
                }) if text.as_str() == "visible"
            ));
        }
        other => panic!("unexpected output: {other:?}"),
    }
}

#[tokio::test]
async fn agent_runtime_bus_serializes_commands_per_instance() {
    let runtime = AgentRuntime::new();
    let provider = Arc::new(TestProvider::new("test"));
    runtime.register(provider.clone());
    let bus = runtime.bus();
    let mut events = bus.take_events().await.expect("take bus events");

    bus.command(RuntimeCommand::UpdateFilter {
        filter: RuntimeBusFilter {
            assistant_messages: true,
            ..Default::default()
        },
    })
    .await
    .expect("enqueue filter update");

    bus.command(RuntimeCommand::Open {
        command_id: CommandId("cmd-open-ordered".into()),
        provider_id: TEST_PROVIDER_ID,
        req: OpenSession::default(),
    })
    .await
    .expect("enqueue open command");

    let opened = expect_session_opened(&mut events, "cmd-open-ordered").await;
    let controller = provider.controller(0).await;
    controller.block_submit();

    bus.command(RuntimeCommand::Submit {
        instance_id: opened.instance_id.clone(),
        input: AgentInput {
            text: "ordered".into(),
            images: Vec::new(),
        },
    })
    .await
    .expect("enqueue submit command");
    bus.command(RuntimeCommand::Close {
        instance_id: opened.instance_id.clone(),
    })
    .await
    .expect("enqueue close command");

    controller.release_submit();

    let first = recv_bus_output(&mut events).await;
    assert!(matches!(
        first,
        RuntimeBusOutput::Event(bus_event)
            if bus_event.instance_id == opened.instance_id
                && matches!(
                    &bus_event.event,
                    Event::Message(MessageEvent {
                        role: MessageRole::Assistant,
                        text,
                        ..
                    }) if text.as_str() == "submitted:ordered"
                )
    ));

    let second = recv_bus_output(&mut events).await;
    assert!(matches!(
        second,
        RuntimeBusOutput::SessionClosed(closed)
            if closed.instance_id == opened.instance_id
                && closed.session_id == opened.session_id
    ));
}

#[tokio::test]
async fn agent_runtime_bus_rejects_commands_after_close_is_enqueued() {
    let runtime = AgentRuntime::new();
    let provider = Arc::new(TestProvider::new("test"));
    runtime.register(provider);
    let bus = runtime.bus();
    let mut events = bus.take_events().await.expect("take bus events");

    bus.command(RuntimeCommand::Open {
        command_id: CommandId("cmd-open-closing".into()),
        provider_id: TEST_PROVIDER_ID,
        req: OpenSession::default(),
    })
    .await
    .expect("enqueue open command");

    let opened = expect_session_opened(&mut events, "cmd-open-closing").await;

    bus.command(RuntimeCommand::Close {
        instance_id: opened.instance_id.clone(),
    })
    .await
    .expect("enqueue close command");
    bus.command(RuntimeCommand::Submit {
        instance_id: opened.instance_id.clone(),
        input: AgentInput {
            text: "too-late".into(),
            images: Vec::new(),
        },
    })
    .await
    .expect("enqueue late submit");

    let mut saw_closed = false;
    let mut saw_rejected = false;
    for _ in 0..2 {
        match recv_bus_output(&mut events).await {
            RuntimeBusOutput::SessionClosed(closed)
                if closed.instance_id == opened.instance_id
                    && closed.session_id == opened.session_id =>
            {
                saw_closed = true;
            }
            RuntimeBusOutput::CommandRejected(rejected)
                if rejected.instance_id == Some(opened.instance_id.clone())
                    && rejected.session_id == Some(opened.session_id.clone())
                    && rejected.message.contains("closing") =>
            {
                saw_rejected = true;
            }
            other => panic!("unexpected output: {other:?}"),
        }
    }

    assert!(saw_closed, "expected SessionClosed output");
    assert!(saw_rejected, "expected late command rejection");
}

#[tokio::test]
async fn agent_runtime_bus_rejects_duplicate_close_commands() {
    let runtime = AgentRuntime::new();
    let provider = Arc::new(TestProvider::new("test"));
    runtime.register(provider);
    let bus = runtime.bus();
    let mut events = bus.take_events().await.expect("take bus events");

    bus.command(RuntimeCommand::Open {
        command_id: CommandId("cmd-open-duplicate-close".into()),
        provider_id: TEST_PROVIDER_ID,
        req: OpenSession::default(),
    })
    .await
    .expect("enqueue open command");

    let opened = expect_session_opened(&mut events, "cmd-open-duplicate-close").await;

    bus.command(RuntimeCommand::Close {
        instance_id: opened.instance_id.clone(),
    })
    .await
    .expect("enqueue first close");
    bus.command(RuntimeCommand::Close {
        instance_id: opened.instance_id.clone(),
    })
    .await
    .expect("enqueue second close");

    let mut saw_closed = false;
    let mut saw_rejected = false;
    for _ in 0..2 {
        match recv_bus_output(&mut events).await {
            RuntimeBusOutput::SessionClosed(closed)
                if closed.instance_id == opened.instance_id
                    && closed.session_id == opened.session_id =>
            {
                saw_closed = true;
            }
            RuntimeBusOutput::CommandRejected(rejected)
                if rejected.instance_id == Some(opened.instance_id.clone())
                    && rejected.session_id == Some(opened.session_id.clone())
                    && rejected.message.contains("closing") =>
            {
                saw_rejected = true;
            }
            other => panic!("unexpected output: {other:?}"),
        }
    }

    assert!(saw_closed, "expected SessionClosed output");
    assert!(saw_rejected, "expected duplicate close rejection");
}

#[tokio::test]
async fn agent_runtime_bus_preserves_order_per_instance_while_sessions_interleave() {
    let runtime = AgentRuntime::new();
    let provider = Arc::new(TestProvider::new("test"));
    runtime.register(provider.clone());
    let bus = runtime.bus();
    let mut events = bus.take_events().await.expect("take bus events");

    bus.command(RuntimeCommand::UpdateFilter {
        filter: RuntimeBusFilter {
            assistant_messages: true,
            ..Default::default()
        },
    })
    .await
    .expect("enqueue filter update");

    bus.command(RuntimeCommand::Open {
        command_id: CommandId("cmd-open-a".into()),
        provider_id: TEST_PROVIDER_ID,
        req: OpenSession::default(),
    })
    .await
    .expect("enqueue first open");
    let opened_a = expect_session_opened(&mut events, "cmd-open-a").await;

    bus.command(RuntimeCommand::Open {
        command_id: CommandId("cmd-open-b".into()),
        provider_id: TEST_PROVIDER_ID,
        req: OpenSession::default(),
    })
    .await
    .expect("enqueue second open");
    let opened_b = expect_session_opened(&mut events, "cmd-open-b").await;

    let controller_a = provider.controller(0).await;
    let controller_b = provider.controller(1).await;

    let mut seen = Vec::new();
    for (controller, label, text) in [
        (&controller_a, "a", "a-1"),
        (&controller_b, "b", "b-1"),
        (&controller_a, "a", "a-2"),
        (&controller_b, "b", "b-2"),
    ] {
        controller.emit(assistant_message(text)).await;
        match recv_bus_output(&mut events).await {
            RuntimeBusOutput::Event(bus_event) if bus_event.instance_id == opened_a.instance_id => {
                if let Event::Message(MessageEvent { text, .. }) = bus_event.event {
                    seen.push(("a", text));
                }
            }
            RuntimeBusOutput::Event(bus_event) if bus_event.instance_id == opened_b.instance_id => {
                if let Event::Message(MessageEvent { text, .. }) = bus_event.event {
                    seen.push(("b", text));
                }
            }
            other => panic!("unexpected output: {other:?}"),
        }
        assert_eq!(
            seen.last()
                .map(|(seen_label, seen_text)| (*seen_label, seen_text.as_str())),
            Some((label, text))
        );
    }

    assert_eq!(
        seen,
        vec![
            ("a", "a-1".into()),
            ("b", "b-1".into()),
            ("a", "a-2".into()),
            ("b", "b-2".into()),
        ]
    );
}

#[tokio::test]
async fn agent_runtime_bus_does_not_block_other_instances_behind_slow_commands() {
    let runtime = AgentRuntime::new();
    let provider = Arc::new(TestProvider::new("test"));
    runtime.register(provider.clone());
    let bus = runtime.bus();
    let mut events = bus.take_events().await.expect("take bus events");

    bus.command(RuntimeCommand::UpdateFilter {
        filter: RuntimeBusFilter {
            assistant_messages: true,
            ..Default::default()
        },
    })
    .await
    .expect("enqueue filter update");

    bus.command(RuntimeCommand::Open {
        command_id: CommandId("cmd-open-slow".into()),
        provider_id: TEST_PROVIDER_ID,
        req: OpenSession::default(),
    })
    .await
    .expect("enqueue slow open");
    let opened_slow = expect_session_opened(&mut events, "cmd-open-slow").await;

    bus.command(RuntimeCommand::Open {
        command_id: CommandId("cmd-open-fast".into()),
        provider_id: TEST_PROVIDER_ID,
        req: OpenSession::default(),
    })
    .await
    .expect("enqueue fast open");
    let opened_fast = expect_session_opened(&mut events, "cmd-open-fast").await;

    let slow = provider.controller(0).await;
    let _fast = provider.controller(1).await;
    slow.block_submit();

    bus.command(RuntimeCommand::Submit {
        instance_id: opened_slow.instance_id.clone(),
        input: AgentInput {
            text: "slow".into(),
            images: Vec::new(),
        },
    })
    .await
    .expect("enqueue slow submit");
    bus.command(RuntimeCommand::Submit {
        instance_id: opened_fast.instance_id.clone(),
        input: AgentInput {
            text: "fast".into(),
            images: Vec::new(),
        },
    })
    .await
    .expect("enqueue fast submit");

    let output = timeout(Duration::from_millis(200), events.recv())
        .await
        .expect("fast instance should not be blocked")
        .expect("runtime bus output");
    assert!(matches!(
        output,
        RuntimeBusOutput::Event(bus_event)
            if bus_event.instance_id == opened_fast.instance_id
                && matches!(
                    &bus_event.event,
                    Event::Message(MessageEvent {
                        role: MessageRole::Assistant,
                        text,
                        ..
                    }) if text.as_str() == "submitted:fast"
                )
    ));

    slow.release_submit();
    let output = recv_bus_output(&mut events).await;
    assert!(matches!(
        output,
        RuntimeBusOutput::Event(bus_event)
            if bus_event.instance_id == opened_slow.instance_id
                && matches!(
                    &bus_event.event,
                    Event::Message(MessageEvent {
                        role: MessageRole::Assistant,
                        text,
                        ..
                    }) if text.as_str() == "submitted:slow"
                )
    ));
}

#[tokio::test]
async fn agent_runtime_bus_emits_session_opened_before_buffered_session_events() {
    let runtime = AgentRuntime::new();
    let provider = Arc::new(TestProvider::new("test"));
    provider.queue_open_events(vec![assistant_message("buffered-open")]);
    runtime.register(provider);
    let bus = runtime.bus();
    let mut events = bus.take_events().await.expect("take bus events");

    bus.command(RuntimeCommand::UpdateFilter {
        filter: RuntimeBusFilter {
            assistant_messages: true,
            ..Default::default()
        },
    })
    .await
    .expect("enqueue filter update");

    bus.command(RuntimeCommand::Open {
        command_id: CommandId("cmd-open-buffered".into()),
        provider_id: TEST_PROVIDER_ID,
        req: OpenSession::default(),
    })
    .await
    .expect("enqueue buffered open");

    let first = recv_bus_output(&mut events).await;
    assert!(matches!(
        first,
        RuntimeBusOutput::SessionOpened(opened)
            if opened.command_id == CommandId("cmd-open-buffered".into())
    ));

    let second = recv_bus_output(&mut events).await;
    assert!(matches!(
        second,
        RuntimeBusOutput::Event(bus_event)
            if matches!(
                &bus_event.event,
                Event::Message(MessageEvent {
                    role: MessageRole::Assistant,
                    text,
                    ..
                }) if text.as_str() == "buffered-open"
            )
    ));
}

#[tokio::test]
async fn agent_runtime_bus_filter_updates_drain_facade_backlog_before_switching() {
    let runtime = AgentRuntime::new();
    let provider = Arc::new(FacadeTestProvider::new("test"));
    runtime.register(provider.clone());
    let bus = runtime.bus();
    let mut events = bus.take_events().await.expect("take bus events");

    bus.command(RuntimeCommand::UpdateFilter {
        filter: RuntimeBusFilter {
            assistant_messages: true,
            ..Default::default()
        },
    })
    .await
    .expect("enable assistant messages");

    bus.command(RuntimeCommand::Open {
        command_id: CommandId("cmd-open-facade-filter".into()),
        provider_id: TEST_PROVIDER_ID,
        req: OpenSession::default(),
    })
    .await
    .expect("enqueue open command");

    let opened = expect_session_opened(&mut events, "cmd-open-facade-filter").await;
    let controller = provider.controller(0).await;

    for idx in 0..1025 {
        controller
            .emit(canonical_assistant_message(
                &format!("assistant-{idx}"),
                &format!("before-update-{idx}"),
            ))
            .await;
    }
    controller
        .emit(canonical_user_message(
            "user-before-update",
            "before-update-user",
        ))
        .await;

    timeout(
        Duration::from_secs(2),
        bus.command(RuntimeCommand::UpdateFilter {
            filter: RuntimeBusFilter {
                user_messages: true,
                ..Default::default()
            },
        }),
    )
    .await
    .expect("filter update should not deadlock behind facade backlog")
    .expect("apply user filter");

    for idx in 0..1025 {
        let output = recv_bus_output(&mut events).await;
        assert!(matches!(
            output,
            RuntimeBusOutput::Event(bus_event)
                if bus_event.instance_id == opened.instance_id
                    && matches!(
                        &bus_event.event,
                        Event::Message(MessageEvent {
                            role: MessageRole::Assistant,
                            text,
                            ..
                        }) if text.as_str() == format!("before-update-{idx}")
                    )
        ));
    }
    assert!(
        timeout(Duration::from_millis(100), events.recv())
            .await
            .is_err(),
        "pre-update user event should stay filtered under the old filter"
    );

    controller
        .emit(canonical_user_message(
            "user-after-update",
            "after-update-user",
        ))
        .await;

    let output = recv_bus_output(&mut events).await;
    assert!(matches!(
        output,
        RuntimeBusOutput::Event(bus_event)
            if bus_event.instance_id == opened.instance_id
                && matches!(
                    &bus_event.event,
                    Event::Message(MessageEvent {
                        role: MessageRole::User,
                        text,
                        ..
                    }) if text.as_str() == "after-update-user"
                )
    ));
}

fn assistant_message(text: &str) -> Event {
    Event::Message(MessageEvent {
        role: MessageRole::Assistant,
        text: text.into(),
        streaming: false,
    })
}

async fn expect_session_opened(
    events: &mut mpsc::Receiver<RuntimeBusOutput>,
    command_id: &str,
) -> SessionOpenedEvent {
    match recv_bus_output(events).await {
        RuntimeBusOutput::SessionOpened(opened)
            if opened.command_id == CommandId(command_id.into()) =>
        {
            opened
        }
        other => panic!("unexpected output: {other:?}"),
    }
}

async fn recv_bus_output(events: &mut mpsc::Receiver<RuntimeBusOutput>) -> RuntimeBusOutput {
    timeout(Duration::from_secs(2), events.recv())
        .await
        .expect("runtime bus output timeout")
        .expect("runtime bus output")
}

#[derive(Clone)]
struct SessionController {
    tx: Arc<StdMutex<Option<mpsc::Sender<Event>>>>,
    submit_blocked: Arc<std::sync::atomic::AtomicBool>,
    submit_release: Arc<Notify>,
}

impl SessionController {
    async fn emit(&self, event: Event) {
        let tx = self
            .tx
            .lock()
            .expect("session controller lock")
            .clone()
            .expect("session controller closed");
        tx.send(event).await.expect("send session event");
    }

    fn block_submit(&self) {
        self.submit_blocked.store(true, Ordering::Relaxed);
    }

    fn release_submit(&self) {
        self.submit_blocked.store(false, Ordering::Relaxed);
        self.submit_release.notify_waiters();
    }
}

#[derive(Clone)]
struct CanonicalSessionController {
    tx: Arc<StdMutex<Option<mpsc::Sender<CanonicalEvent>>>>,
}

impl CanonicalSessionController {
    async fn emit(&self, event: CanonicalEvent) {
        let tx = self
            .tx
            .lock()
            .expect("canonical controller lock")
            .clone()
            .expect("canonical controller closed");
        tx.send(event).await.expect("send canonical session event");
    }
}

#[derive(Default)]
struct FacadeProviderState {
    next_id: AtomicUsize,
    controllers: StdMutex<Vec<CanonicalSessionController>>,
}

struct FacadeTestProvider {
    provider_id: &'static str,
    state: Arc<FacadeProviderState>,
}

impl FacadeTestProvider {
    fn new(provider_id: &'static str) -> Self {
        Self {
            provider_id,
            state: Arc::new(FacadeProviderState::default()),
        }
    }

    async fn controller(&self, idx: usize) -> CanonicalSessionController {
        self.state.controllers.lock().expect("controllers lock")[idx].clone()
    }

    async fn new_session(&self) -> Result<Box<dyn AgentSession>, AgentError> {
        let idx = self.state.next_id.fetch_add(1, Ordering::Relaxed);
        let (tx, rx) = mpsc::channel(2048);
        let tx = Arc::new(StdMutex::new(Some(tx)));
        let controller = CanonicalSessionController { tx: tx.clone() };
        self.state
            .controllers
            .lock()
            .expect("controllers lock")
            .push(controller);

        let sender = tx
            .lock()
            .expect("session tx lock")
            .clone()
            .expect("session tx");
        sender
            .send(CanonicalEvent::new(Payload::SessionStarted(
                SessionStarted {
                    session_id: format!("facade-session-{idx}").into(),
                    model: "model".into(),
                },
            )))
            .await
            .expect("send session started");

        let session = runtime::Session::new_synthetic(rx, sender);
        let session = AgentSessionFacade::attach(
            ProviderId::from_static(self.provider_id),
            InstanceId(format!("facade-instance-{idx}").into()),
            session,
            None,
        )
        .await?;
        Ok(Box::new(session))
    }
}

#[async_trait]
impl lucarne::agent_runtime::AgentProvider for FacadeTestProvider {
    fn id(&self) -> ProviderId {
        ProviderId::from_static(self.provider_id)
    }

    async fn probe(&self) -> Result<lucarne::agent_runtime::ProbeResult, AgentError> {
        Err(AgentError {
            kind: AgentErrorKind::Unsupported,
            message: "probe not used in runtime bus tests".into(),
        })
    }

    async fn open(&self, _req: OpenSession) -> Result<Box<dyn AgentSession>, AgentError> {
        self.new_session().await
    }

    async fn resume(
        &self,
        _req: lucarne::agent_runtime::ResumeSession,
    ) -> Result<Box<dyn AgentSession>, AgentError> {
        self.new_session().await
    }
}

#[derive(Default)]
struct TestProviderState {
    next_id: AtomicUsize,
    open_count: AtomicUsize,
    controllers: StdMutex<Vec<SessionController>>,
    queued_open_events: StdMutex<Vec<Vec<Event>>>,
    resume_refs: StdMutex<Vec<String>>,
}

struct TestProvider {
    provider_id: &'static str,
    state: Arc<TestProviderState>,
}

impl TestProvider {
    fn new(provider_id: &'static str) -> Self {
        Self {
            provider_id,
            state: Arc::new(TestProviderState::default()),
        }
    }

    async fn controller(&self, idx: usize) -> SessionController {
        self.state.controllers.lock().expect("controllers lock")[idx].clone()
    }

    fn open_count(&self) -> usize {
        self.state.open_count.load(Ordering::Relaxed)
    }

    fn queue_open_events(&self, events: Vec<Event>) {
        self.state
            .queued_open_events
            .lock()
            .expect("queued_open_events lock")
            .push(events);
    }

    fn resume_refs(&self) -> Vec<String> {
        self.state
            .resume_refs
            .lock()
            .expect("resume_refs lock")
            .clone()
    }
}

#[async_trait]
impl lucarne::agent_runtime::AgentProvider for TestProvider {
    fn id(&self) -> ProviderId {
        ProviderId::from_static(self.provider_id)
    }

    async fn probe(&self) -> Result<lucarne::agent_runtime::ProbeResult, AgentError> {
        Err(AgentError {
            kind: AgentErrorKind::Unsupported,
            message: "probe not used in runtime bus tests".into(),
        })
    }

    async fn open(&self, _req: OpenSession) -> Result<Box<dyn AgentSession>, AgentError> {
        self.state.open_count.fetch_add(1, Ordering::Relaxed);
        Ok(Box::new(self.new_session()))
    }

    async fn resume(
        &self,
        req: lucarne::agent_runtime::ResumeSession,
    ) -> Result<Box<dyn AgentSession>, AgentError> {
        self.state
            .resume_refs
            .lock()
            .expect("resume_refs lock")
            .push(req.session_ref.0.to_string());
        Ok(Box::new(self.new_session()))
    }
}

impl TestProvider {
    fn new_session(&self) -> TestSession {
        let idx = self.state.next_id.fetch_add(1, Ordering::Relaxed);
        let (tx, rx) = mpsc::channel(2048);
        let tx = Arc::new(StdMutex::new(Some(tx)));
        let submit_blocked = Arc::new(std::sync::atomic::AtomicBool::new(false));
        let submit_release = Arc::new(Notify::new());
        let controller = SessionController {
            tx: tx.clone(),
            submit_blocked: Arc::clone(&submit_blocked),
            submit_release: Arc::clone(&submit_release),
        };
        self.state
            .controllers
            .lock()
            .expect("controllers lock")
            .push(controller);
        let preloaded = self
            .state
            .queued_open_events
            .lock()
            .expect("queued_open_events lock")
            .pop()
            .unwrap_or_default();
        for event in preloaded {
            tx.lock()
                .expect("session tx lock")
                .clone()
                .expect("session tx")
                .try_send(event)
                .expect("preload session event");
        }

        TestSession {
            provider_id: self.provider_id,
            instance_id: InstanceId(format!("instance-{idx}").into()),
            session_id: SessionId(format!("session-{idx}").into()),
            source_tx: tx,
            source_rx: StdMutex::new(Some(rx)),
            close_reason: Arc::new(StdMutex::new(None)),
            submit_blocked,
            submit_release,
        }
    }
}

struct TestSession {
    provider_id: &'static str,
    instance_id: InstanceId,
    session_id: SessionId,
    source_tx: Arc<StdMutex<Option<mpsc::Sender<Event>>>>,
    source_rx: StdMutex<Option<mpsc::Receiver<Event>>>,
    close_reason: Arc<StdMutex<Option<SmolStr>>>,
    submit_blocked: Arc<std::sync::atomic::AtomicBool>,
    submit_release: Arc<Notify>,
}

#[async_trait]
impl AgentSession for TestSession {
    fn id(&self) -> &SessionId {
        &self.session_id
    }

    fn instance_id(&self) -> &InstanceId {
        &self.instance_id
    }

    fn provider_id(&self) -> ProviderId {
        ProviderId::from_static(self.provider_id)
    }

    async fn submit(&self, input: AgentInput) -> Result<(), AgentError> {
        while self.submit_blocked.load(Ordering::Relaxed) {
            self.submit_release.notified().await;
        }
        let tx = { self.source_tx.lock().expect("source tx lock").clone() };
        if let Some(tx) = tx {
            tx.send(assistant_message(&format!("submitted:{}", input.text)))
                .await
                .map_err(|_| AgentError {
                    kind: AgentErrorKind::InvalidState,
                    message: "test session event stream closed".into(),
                })?;
        }
        Ok(())
    }

    async fn interrupt(&self) -> Result<(), AgentError> {
        Ok(())
    }

    async fn resolve(
        &self,
        _req_id: &str,
        _response: lucarne::agent_runtime::InterventionResponse,
    ) -> Result<(), AgentError> {
        Ok(())
    }

    async fn take_events(&self) -> Result<mpsc::Receiver<Event>, AgentError> {
        self.source_rx
            .lock()
            .expect("source rx lock")
            .take()
            .ok_or_else(|| AgentError {
                kind: AgentErrorKind::InvalidState,
                message: "test session event stream already taken".into(),
            })
    }

    async fn close(&self) -> Result<(), AgentError> {
        *self.close_reason.lock().expect("close reason lock") = Some("test session closed".into());
        self.source_tx.lock().expect("source tx lock").take();
        Ok(())
    }

    async fn observed_close_reason(&self) -> Option<SmolStr> {
        self.close_reason.lock().expect("close reason lock").clone()
    }
}

fn canonical_assistant_message(item_id: &str, text: &str) -> CanonicalEvent {
    CanonicalEvent::new(Payload::Timeline(Timeline {
        item: event::new_timeline_assistant(item_id, text, true),
    }))
}

fn canonical_user_message(item_id: &str, text: &str) -> CanonicalEvent {
    CanonicalEvent::new(Payload::Timeline(Timeline {
        item: event::new_timeline_user(item_id, text),
    }))
}
