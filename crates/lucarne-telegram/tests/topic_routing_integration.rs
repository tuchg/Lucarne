use std::{
    collections::HashMap,
    ffi::OsString,
    fs,
    path::{Path, PathBuf},
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc, Mutex as StdMutex, OnceLock,
    },
    time::{Duration, Instant},
};

use agent_sessions::{agent_provider, ParseSelection, WatchConfig};
use async_trait::async_trait;
use futures::{
    stream::{self, BoxStream},
    StreamExt,
};
use lucarne::{
    agent_runtime::{
        AgentCapabilities, AgentCommand, AgentCommandCatalog, AgentCommandCompletion,
        AgentCommandInput, AgentCommandSource, AgentError, AgentErrorKind, AgentEventStream,
        AgentForkResult, AgentForkSelection, AgentForkTarget, AgentForkTargetCatalog, AgentInput,
        AgentModelCatalog, AgentModelOption, AgentModelSelection, AgentPermissionCatalog,
        AgentPermissionOption, AgentPermissionSelection, AgentProvider, AgentReasoningOption,
        AgentSession, AgentSkillCatalog, AgentSkillSummary, AgentStatus, ApprovalDecision,
        ApprovalRequest, Attachment as AgentAttachment, CallId, Event, InstanceId,
        InterventionResponse, MessageEvent, MessageRole, OpenSession, ProbeResult, ProviderId,
        ReasoningEvent, ResumeSession, SessionId, SessionRef, ToolCallEvent, ToolResultEvent,
    },
    control_plane::{
        ChannelBinding, ChannelBindingId, ControlPlaneSqliteStore, ProviderSessionId, TurnSource,
        WorkspaceId as ControlWorkspaceId,
    },
    core_service::{LucarneCore, OpenWorkspaceRequest, ResumeWorkspaceRequest, SubmitTurnRequest},
};
use lucarne_channel::{
    agent_message::compact_path, Attachment as ChannelAttachment, Channel, ChannelError,
    ChannelEvent, ChatId, CommandQuery, CommandQueryResult, FileUpload, IncomingAttachment,
    IncomingMessage, MessageId, OutgoingMessage, Result, TextFormat, WorkspaceHandle, WorkspaceId,
};
use lucarne_telegram::state::WorkSession;
use lucarne_telegram::{bot::Bot, state::BotState};
use serde_json::json;
use tempfile::TempDir;
use tokio::{
    sync::{mpsc, Mutex as TokioMutex},
    time::{sleep, timeout},
};

static ENV_LOCK: StdMutex<()> = StdMutex::new(());
static TEST_LOCK: StdMutex<()> = StdMutex::new(());
static DEFAULT_HISTORY_ENV: OnceLock<TempDir> = OnceLock::new();
const TEST_HISTORY_ENV_MARKER: &str = "LUCARNE_TELEGRAM_TEST_HISTORY_ENV";

fn test_lock() -> std::sync::MutexGuard<'static, ()> {
    TEST_LOCK.lock().unwrap_or_else(|err| err.into_inner())
}

fn ensure_default_history_env() {
    if std::env::var_os(TEST_HISTORY_ENV_MARKER).is_some() {
        return;
    }
    let tmp = DEFAULT_HISTORY_ENV.get_or_init(|| TempDir::new().expect("default history temp dir"));
    let codex_home = tmp.path().join("codex");
    std::env::set_var("HOME", tmp.path());
    std::env::set_var("PATH", tmp.path());
    std::env::set_var("CODEX_HOME", &codex_home);
    std::env::set_var("CLAUDE_CONFIG_DIR", tmp.path().join("claude"));
    std::env::set_var("GEMINI_HOME", tmp.path().join("gemini"));
    std::env::set_var("GEMINI_CONFIG_DIR", tmp.path().join("gemini"));
    std::env::set_var("COPILOT_HOME", tmp.path().join("copilot"));
    std::env::set_var(TEST_HISTORY_ENV_MARKER, "default");
}

#[tokio::test(flavor = "current_thread")]
async fn unbound_topic_message_does_not_reuse_matching_control_workspace_id() {
    let _test_lock = test_lock();
    let provider = Arc::new(ProviderProbe::default());
    let core = core_with_provider(Arc::clone(&provider));
    core.upsert_workspace_binding(
        ControlWorkspaceId::new("2"),
        OpenWorkspaceRequest {
            provider_id: "codex",
            project_path: Some(PathBuf::from("/tmp/project")),
            title: "codex unbound".into(),
        },
        Some("thread-1"),
    )
    .expect("persist unbound workspace");

    let channel = Arc::new(RecordingChannel::with_events(vec![ChannelEvent::Message(
        topic_message("2", "m-unbound", "should not reach the provider"),
    )]));
    let bot = bot_with_state(Arc::clone(&channel), Arc::clone(&core));

    timeout(Duration::from_secs(2), bot.run())
        .await
        .expect("bot run should finish after finite event stream");

    wait_until(|| {
        channel.sent_messages().iter().any(|sent| {
            sent.message
                .body
                .contains("isn't bound to an agent session")
        })
    })
    .await;
    assert!(
        provider.inputs().is_empty(),
        "unbound Telegram topics must not fall back to a same-named control workspace"
    );
    assert!(
        channel.acknowledged_topics().is_empty(),
        "unbound topics should be rejected before message acknowledgement"
    );
}

#[tokio::test(flavor = "current_thread")]
async fn persisted_topic_binding_routes_user_turn_to_provider_and_reply_topic() {
    let _test_lock = test_lock();
    let provider = Arc::new(ProviderProbe::default());
    let core = core_with_provider(Arc::clone(&provider));
    core.upsert_workspace_binding(
        ControlWorkspaceId::new("workspace-a"),
        OpenWorkspaceRequest {
            provider_id: "codex",
            project_path: Some(PathBuf::from("/tmp/project-a")),
            title: "codex project".into(),
        },
        Some("thread-1"),
    )
    .expect("persist workspace binding");
    core.upsert_channel_binding(ChannelBinding::new(
        ChannelBindingId::new("telegram:100:9"),
        ControlWorkspaceId::new("workspace-a"),
        "telegram",
        "100",
        Some("9"),
    ))
    .expect("persist topic binding");

    let channel = Arc::new(RecordingChannel::with_events(vec![ChannelEvent::Message(
        topic_message("9", "m-bound", "hello from topic"),
    )]));
    let bot = bot_with_state(Arc::clone(&channel), Arc::clone(&core));

    timeout(Duration::from_secs(2), bot.run())
        .await
        .expect("bot run should finish after finite event stream");

    wait_until(|| provider.inputs().len() == 1).await;
    assert_eq!(provider.inputs()[0].text.as_str(), "hello from topic");
    assert_eq!(channel.acknowledged_topics(), vec!["9".to_string()]);

    wait_until(|| {
        channel.sent_messages().iter().any(|sent| {
            sent.topic == "9"
                && sent.message.body.contains("ok")
                && sent
                    .message
                    .reply_to
                    .as_ref()
                    .is_some_and(|reply| reply.as_str() == "m-bound")
        }) || channel
            .edited_messages()
            .iter()
            .any(|sent| sent.topic == "9" && sent.message.body.contains("ok"))
    })
    .await;
}

#[tokio::test(flavor = "current_thread")]
async fn same_topic_id_in_different_chats_routes_to_chat_scoped_sessions() {
    let _test_lock = test_lock();
    let _env = IsolatedHistoryEnv::new();
    let provider = Arc::new(ProviderProbe::default());
    let core = core_with_provider(Arc::clone(&provider));
    bind_workspace_in_chat(&core, "workspace-a", "100", "9", "thread-a");
    bind_workspace_in_chat(&core, "workspace-b", "200", "9", "thread-b");

    let channel = Arc::new(RecordingChannel::with_events_and_gap(
        vec![
            ChannelEvent::Message(topic_message_in_chat("100", "9", "m-chat-a", "from chat a")),
            ChannelEvent::Message(topic_message_in_chat("200", "9", "m-chat-b", "from chat b")),
        ],
        Duration::from_millis(150),
    ));
    let bot = bot_with_state(Arc::clone(&channel), Arc::clone(&core));

    timeout(Duration::from_secs(4), bot.run())
        .await
        .expect("multi-chat topic routing should finish after finite event stream");

    wait_until(|| provider.inputs().len() == 2).await;
    assert_eq!(
        provider.resumes(),
        vec!["thread-a".to_string(), "thread-b".to_string()],
        "same Telegram topic ids in different chats must resolve through chat+topic bindings"
    );
    assert_eq!(
        channel.acknowledged_handles(),
        vec![
            ("100".to_string(), "9".to_string()),
            ("200".to_string(), "9".to_string())
        ]
    );
    assert!(
        topic_messages_in_chat(&channel, "100", "9")
            .iter()
            .any(|sent| sent.message.body.contains("ok")),
        "chat 100 topic 9 should receive its own assistant reply; sent={:?}; edits={:?}",
        channel.sent_messages(),
        channel.edited_messages()
    );
    assert!(
        topic_messages_in_chat(&channel, "200", "9")
            .iter()
            .any(|sent| sent.message.body.contains("ok")),
        "chat 200 topic 9 should receive its own assistant reply; sent={:?}; edits={:?}",
        channel.sent_messages(),
        channel.edited_messages()
    );
}

#[tokio::test(flavor = "current_thread")]
async fn fork_target_selection_runs_through_topic_scoped_text_command_flow() {
    let _test_lock = test_lock();
    let provider = Arc::new(ProviderProbe::with_fork_targets(vec![AgentForkTarget {
        id: "target-a".into(),
        label: Some("First rollback".into()),
        description: Some("turn 1".into()),
    }]));
    let core = core_with_provider(Arc::clone(&provider));
    core.upsert_workspace_binding(
        ControlWorkspaceId::new("workspace-a"),
        OpenWorkspaceRequest {
            provider_id: "codex",
            project_path: Some(PathBuf::from("/tmp/project-a")),
            title: "codex project".into(),
        },
        Some("thread-1"),
    )
    .expect("persist workspace binding");
    core.upsert_channel_binding(ChannelBinding::new(
        ChannelBindingId::new("telegram:100:9"),
        ControlWorkspaceId::new("workspace-a"),
        "telegram",
        "100",
        Some("9"),
    ))
    .expect("persist topic binding");
    let state = BotState::new_with_core(Arc::clone(&core));

    let list_channel = Arc::new(RecordingChannel::with_events(vec![ChannelEvent::Message(
        topic_message("9", "m-fork-list", "/fork"),
    )]));
    let list_bot = Arc::new(Bot::new_with_state(
        Arc::clone(&list_channel) as Arc<dyn Channel>,
        Arc::clone(&core),
        WorkspaceHandle::new(ChatId::new("100"), WorkspaceId::new("")),
        Arc::clone(&state),
    ));
    timeout(Duration::from_secs(2), list_bot.run())
        .await
        .expect("fork list run should finish after finite event stream");

    let rendered_selector = eventually(|| {
        fork_selector_rendered_without_buttons(&list_channel.sent_messages())
            || fork_selector_rendered_without_buttons(&list_channel.edited_messages())
    })
    .await;
    assert!(
        rendered_selector,
        "fork list should render /fN text selectors without buttons; sent={:?}; edits={:?}",
        list_channel.sent_messages(),
        list_channel.edited_messages()
    );

    let select_channel = Arc::new(RecordingChannel::with_events(vec![ChannelEvent::Message(
        topic_message("9", "m-fork-select", "/f1"),
    )]));
    let select_bot = Arc::new(Bot::new_with_state(
        select_channel,
        Arc::clone(&core),
        WorkspaceHandle::new(ChatId::new("100"), WorkspaceId::new("")),
        state,
    ));
    timeout(Duration::from_secs(2), select_bot.run())
        .await
        .expect("fork selection run should finish after finite event stream");

    wait_until(|| provider.fork_selections() == vec!["target-a".to_string()]).await;
}

#[tokio::test(flavor = "current_thread")]
async fn fork_selection_rebinds_current_topic_without_creating_fork_topic() {
    let _test_lock = test_lock();
    let _env = IsolatedHistoryEnv::new();
    write_codex_history_session_with_reply(
        _env.codex_home(),
        "fork-session",
        "/tmp/workspace-a",
        "fork inherited prompt",
        "fork inherited answer",
    );
    let provider = Arc::new(ProviderProbe::with_fork_targets(vec![AgentForkTarget {
        id: "target-a".into(),
        label: Some("First rollback".into()),
        description: Some("turn 1".into()),
    }]));
    let core = core_with_provider(Arc::clone(&provider));
    bind_workspace(&core, "workspace-a", "9", "session-test");
    let project_path = PathBuf::from("/tmp/workspace-a");
    assert!(
        core.history_entry_for_provider_session("codex", "fork-session", Some(&project_path))
            .is_some(),
        "fork history fixture should be discoverable by exact provider session ref"
    );
    let state = BotState::new_with_core(Arc::clone(&core));

    let list_channel = Arc::new(RecordingChannel::with_events(vec![ChannelEvent::Message(
        topic_message("9", "m-fork-list", "/fork"),
    )]));
    let list_bot = bot_with_existing_state(
        Arc::clone(&list_channel),
        Arc::clone(&core),
        Arc::clone(&state),
    );
    timeout(Duration::from_secs(2), list_bot.run())
        .await
        .expect("fork list run should finish after finite event stream");
    let rendered_selector = eventually(|| {
        fork_selector_rendered_without_buttons(&list_channel.sent_messages())
            || fork_selector_rendered_without_buttons(&list_channel.edited_messages())
    })
    .await;
    assert!(
        rendered_selector,
        "fork list should render a selectable /fN target before selection"
    );

    let select_channel = Arc::new(RecordingChannel::with_events(vec![ChannelEvent::Message(
        topic_message("9", "m-fork-select", "/f1"),
    )]));
    let select_bot = bot_with_existing_state(
        Arc::clone(&select_channel),
        Arc::clone(&core),
        Arc::clone(&state),
    );
    timeout(Duration::from_secs(3), select_bot.run())
        .await
        .expect("fork selection run should finish after finite event stream");

    wait_until(|| provider.fork_calls() == vec![("session-test".into(), "target-a".into())]).await;
    wait_until(|| {
        provider.resumes() == vec!["session-test".to_string(), "fork-session".to_string()]
    })
    .await;
    assert!(
        select_channel.created_workspaces().is_empty(),
        "fork selection should reuse the current topic instead of creating a fork topic"
    );
    let fork_topic = "9".to_string();
    eventually_topic_message(&select_channel, &fork_topic, |message| {
        message.body.contains("forked") && message.body.contains("fork-session")
    })
    .await;
    let selected_messages = topic_messages(&select_channel, &fork_topic);
    assert!(
        !selected_messages.iter().any(|message| message
            .message
            .body
            .contains("fork inherited prompt")
            || message.message.body.contains("fork inherited answer")),
        "current-topic fork must not replay inherited history into the existing Telegram topic: {selected_messages:?}"
    );
    let fork_session = state
        .all()
        .into_iter()
        .find(|session| session.resume_ref.as_deref() == Some("fork-session"))
        .expect("fork workspace should be tracked by provider-native fork ref");
    assert_eq!(fork_session.provider_id, "codex");
    assert_eq!(
        state
            .workspace_for_handle(&WorkspaceHandle::new(
                ChatId::new("100"),
                WorkspaceId::new(fork_topic.as_str()),
            ))
            .as_ref(),
        Some(&fork_session.workspace),
        "current topic should route to the fork workspace"
    );

    let followup_channel = Arc::new(RecordingChannel::with_events(vec![ChannelEvent::Message(
        topic_message(&fork_topic, "m-fork-followup", "fork followup"),
    )]));
    let followup_bot = bot_with_existing_state(followup_channel, Arc::clone(&core), state);
    timeout(Duration::from_secs(3), followup_bot.run())
        .await
        .expect("fork topic follow-up run should finish after finite event stream");
    wait_until(|| provider.submit_calls() == vec![("fork-session".into(), "fork followup".into())])
        .await;
}

#[tokio::test(flavor = "current_thread")]
async fn fork_target_list_preserves_provider_order_labels_and_text_selectors() {
    let _test_lock = test_lock();
    let provider = Arc::new(ProviderProbe::with_fork_targets(vec![
        AgentForkTarget {
            id: "target-first".into(),
            label: Some("First user message".into()),
            description: Some("turn 1".into()),
        },
        AgentForkTarget {
            id: "target-second".into(),
            label: Some("Second user message".into()),
            description: Some("turn 2".into()),
        },
    ]));
    let core = core_with_provider(Arc::clone(&provider));
    bind_workspace(&core, "workspace-a", "9", "thread-1");

    let channel = Arc::new(RecordingChannel::with_events(vec![ChannelEvent::Message(
        topic_message("9", "m-fork-list", "/fork"),
    )]));
    let bot = bot_with_state(Arc::clone(&channel), Arc::clone(&core));
    timeout(Duration::from_secs(2), bot.run())
        .await
        .expect("fork list run should finish after finite event stream");

    let fork_targets = eventually_topic_message(&channel, "9", |message| {
        message.body.contains("fork targets") && message.body.contains("`target-first`")
    })
    .await;
    assert_eq!(fork_targets.format, TextFormat::Markdown);
    assert!(fork_targets.buttons.is_empty());
    assert!(fork_targets.body.contains(
        "/f1  `target-first` — First user message — turn 1\n\n/f2  `target-second` — Second user message — turn 2"
    ));
    assert!(
        !fork_targets.body.contains("item/CommandExecution"),
        "fork list must show the actual selector content, not a generic approval label"
    );
}

#[tokio::test(flavor = "current_thread")]
async fn fork_without_targets_does_not_register_a_selectable_shortcut() {
    let _test_lock = test_lock();
    let provider = Arc::new(ProviderProbe::default());
    let core = core_with_provider(Arc::clone(&provider));
    bind_workspace(&core, "workspace-a", "9", "thread-1");
    let state = BotState::new_with_core(Arc::clone(&core));

    let list_channel = Arc::new(RecordingChannel::with_events(vec![ChannelEvent::Message(
        topic_message("9", "m-fork-empty-list", "/fork"),
    )]));
    let list_bot = bot_with_existing_state(
        Arc::clone(&list_channel),
        Arc::clone(&core),
        Arc::clone(&state),
    );
    timeout(Duration::from_secs(2), list_bot.run())
        .await
        .expect("empty fork list run should finish after finite event stream");

    let fork_targets = eventually_topic_message(&list_channel, "9", |message| {
        message.body.contains("fork targets")
    })
    .await;
    assert_eq!(fork_targets.format, TextFormat::Markdown);
    assert!(fork_targets.buttons.is_empty());
    assert!(fork_targets.body.contains("(none)"));
    assert!(!fork_targets.body.contains("/f1"));

    let select_channel = Arc::new(RecordingChannel::with_events(vec![ChannelEvent::Message(
        topic_message("9", "m-fork-empty-select", "/f1"),
    )]));
    let select_bot = bot_with_existing_state(select_channel, Arc::clone(&core), state);
    timeout(Duration::from_secs(2), select_bot.run())
        .await
        .expect("empty fork selection run should finish after finite event stream");

    assert!(
        provider.fork_selections().is_empty(),
        "empty fork target lists must not leave a stale /f1 behind"
    );
}

#[tokio::test(flavor = "current_thread")]
async fn status_topic_command_uses_session_trait_when_provider_catalog_is_empty() {
    let _test_lock = test_lock();
    let provider = Arc::new(ProviderProbe::with_status(AgentStatus {
        model: Some("gpt-5.5".into()),
        reasoning: Some("xhigh".into()),
        directory: Some("/tmp/project-a".into()),
        permissions: Some("Default".into()),
        ..Default::default()
    }));
    let core = core_with_provider(Arc::clone(&provider));
    bind_workspace(&core, "workspace-a", "9", "thread-1");

    let channel = Arc::new(RecordingChannel::with_events(vec![ChannelEvent::Message(
        topic_message("9", "m-status", "/status"),
    )]));
    let bot = bot_with_state(Arc::clone(&channel), Arc::clone(&core));
    timeout(Duration::from_secs(2), bot.run())
        .await
        .expect("status run should finish after finite event stream");

    wait_until(|| provider.status_calls() == 1).await;
    assert!(
        topic_messages(&channel, "9")
            .iter()
            .all(|sent| !sent.message.body.contains("Unsupported command /status")),
        "/status must not depend on the provider command catalog"
    );
    let status = topic_messages(&channel, "9")
        .into_iter()
        .find(|sent| sent.message.body.contains("status") && sent.message.body.contains("gpt-5.5"))
        .expect("status response should be visible in the topic");
    assert_eq!(status.message.format, TextFormat::Markdown);
    assert!(status.message.body.contains("Model: `gpt-5.5`"));
    assert!(status.message.body.contains("reasoning xhigh"));
}

#[tokio::test(flavor = "current_thread")]
async fn status_topic_command_renders_process_resources_for_current_workspace() {
    let _test_lock = test_lock();
    let pid = std::process::id() as i32;
    let mut probe = ProviderProbe::with_status(AgentStatus {
        model: Some("gpt-5.5".into()),
        directory: Some("/tmp/project-a".into()),
        ..Default::default()
    });
    probe.process_id = Some(pid);
    let provider = Arc::new(probe);
    let core = core_with_provider(Arc::clone(&provider));
    bind_workspace(&core, "workspace-a", "9", "thread-1");
    core.resume_workspace_with_events(ResumeWorkspaceRequest {
        workspace_id: ControlWorkspaceId::new("workspace-a"),
        force_bypass_permissions: false,
    })
    .await
    .expect("resume workspace");

    let channel = Arc::new(RecordingChannel::with_events(vec![ChannelEvent::Message(
        topic_message("9", "m-status-resources", "/status"),
    )]));
    let bot = bot_with_state(Arc::clone(&channel), Arc::clone(&core));
    timeout(Duration::from_secs(2), bot.run())
        .await
        .expect("status resources run should finish after finite event stream");

    wait_until(|| provider.status_calls() == 1).await;
    let status = eventually_topic_message(&channel, "9", |message| {
        message.body.contains("status") && message.body.contains("Process identity:")
    })
    .await;
    assert_eq!(status.format, TextFormat::Markdown);
    assert!(status.body.contains("Model: `gpt-5.5`"));
    assert!(status
        .body
        .contains(&format!("Process identity: `thread-1:{pid}`")));
    assert!(status.body.contains(&format!("PID: `{pid}`")));
    assert!(status.body.contains("Processes: `"));
    assert!(status.body.contains("CPU: `"));
    assert!(status.body.contains("Memory: `"));
}

#[tokio::test(flavor = "current_thread")]
async fn entry_status_command_renders_all_managed_agent_resources() {
    let _test_lock = test_lock();
    let pid = std::process::id() as i32;
    let provider = Arc::new(ProviderProbe::with_process_id(pid));
    let core = core_with_provider(Arc::clone(&provider));
    bind_workspace(&core, "workspace-a", "9", "thread-1");
    core.resume_workspace_with_events(ResumeWorkspaceRequest {
        workspace_id: ControlWorkspaceId::new("workspace-a"),
        force_bypass_permissions: false,
    })
    .await
    .expect("resume workspace");

    let channel = Arc::new(RecordingChannel::with_events(vec![ChannelEvent::Message(
        entry_message("m-status-all", "/status"),
    )]));
    let bot = bot_with_state(Arc::clone(&channel), Arc::clone(&core));
    timeout(Duration::from_secs(2), bot.run())
        .await
        .expect("entry status run should finish after finite event stream");

    let status = eventually_topic_message(&channel, "", |message| {
        message.body.contains("agent resources") && message.body.contains("managed agents: `1`")
    })
    .await;
    assert_eq!(status.format, TextFormat::Markdown);
    assert!(status.body.contains("observed recent: `0`"));
    assert!(status.body.contains("actual processes: `"));
    assert!(status.body.contains(&format!("thread-1:{pid}")));
    assert!(status.body.contains(&format!("pid: `{pid}`")));
    assert!(status.body.contains("cpu: `"));
    assert!(status.body.contains("memory: `"));
}

#[tokio::test(flavor = "current_thread")]
async fn entry_kill_all_detaches_live_session_and_reports_identity() {
    let _test_lock = test_lock();
    let pid = std::process::id() as i32;
    let provider = Arc::new(ProviderProbe::with_process_id(pid));
    let core = core_with_provider(Arc::clone(&provider));
    bind_workspace(&core, "workspace-a", "9", "thread-1");
    core.resume_workspace_with_events(ResumeWorkspaceRequest {
        workspace_id: ControlWorkspaceId::new("workspace-a"),
        force_bypass_permissions: false,
    })
    .await
    .expect("resume workspace");
    assert!(core.has_live_session(&ControlWorkspaceId::new("workspace-a")));

    let channel = Arc::new(RecordingChannel::with_events(vec![ChannelEvent::Message(
        entry_message("m-kill-all", "/kill all"),
    )]));
    let bot = bot_with_state(Arc::clone(&channel), Arc::clone(&core));
    timeout(Duration::from_secs(2), bot.run())
        .await
        .expect("entry kill run should finish after finite event stream");

    let report = eventually_topic_message(&channel, "", |message| {
        message.body.contains("kill agent") && message.body.contains("killed: `1`")
    })
    .await;
    assert_eq!(report.format, TextFormat::Markdown);
    assert!(report.body.contains(&format!("thread-1:{pid}")));
    assert!(report.body.contains("workspace: `workspace-a`"));
    assert!(report.body.contains("session: `thread-1`"));
    assert!(report.body.contains(&format!("pid: `{pid}`")));
    assert!(!core.has_live_session(&ControlWorkspaceId::new("workspace-a")));
}

#[tokio::test(flavor = "current_thread")]
async fn commands_topic_command_renders_provider_catalog_without_injecting_trait_commands() {
    let _test_lock = test_lock();
    let provider = Arc::new(ProviderProbe::with_command_catalog(AgentCommandCatalog {
        commands: vec![
            command_catalog_entry("lint", "Run lint checks"),
            command_catalog_entry("deploy", "Deploy current workspace"),
        ],
        complete: true,
        revision: 42,
    }));
    let core = core_with_provider(Arc::clone(&provider));
    bind_workspace(&core, "workspace-a", "9", "thread-1");

    let channel = Arc::new(RecordingChannel::with_events(vec![ChannelEvent::Message(
        topic_message("9", "m-commands", "/commands"),
    )]));
    let bot = bot_with_state(Arc::clone(&channel), Arc::clone(&core));
    timeout(Duration::from_secs(2), bot.run())
        .await
        .expect("commands run should finish after finite event stream");

    let commands = eventually_topic_message(&channel, "9", |message| {
        message.body.contains("agent commands") && message.body.contains("`/lint`")
    })
    .await;
    assert_eq!(commands.format, TextFormat::Markdown);
    assert!(commands.body.contains("1. `/lint`"));
    assert!(commands.body.contains("\n\n2. `/deploy`"));
    assert!(!commands.body.contains("`/fork`"));
    assert!(!commands.body.contains("`/status`"));
}

#[tokio::test(flavor = "current_thread")]
async fn journey_07_send_prompt_renders_reasoning_tool_calls_without_footer() {
    let _test_lock = test_lock();
    let provider = Arc::new(ProviderProbe::with_pending_submit_events(Vec::new()));
    let core = core_with_provider(Arc::clone(&provider));
    bind_workspace_with_project_path(&core, "workspace-a", "9", "thread-1", "/tmp/project-a");

    let channel = Arc::new(RecordingChannel::streaming());
    let bot = bot_with_state(Arc::clone(&channel), Arc::clone(&core));
    let run = tokio::spawn(Arc::clone(&bot).run());
    channel.push_event(ChannelEvent::Message(topic_message(
        "9",
        "m-prompt",
        "run tests",
    )));

    wait_until(|| provider.submit_calls() == vec![("thread-1".into(), "run tests".into())]).await;
    provider
        .emit_to_session(
            "thread-1",
            Event::Reasoning(ReasoningEvent {
                text: "thinking through the fix".into(),
            }),
        )
        .await;
    let thought = eventually_topic_message(&channel, "9", |message| {
        message.body.contains("thinking through the fix")
    })
    .await;
    assert_eq!(thought.format, TextFormat::Markdown);
    provider
        .emit_to_session(
            "thread-1",
            Event::ToolCall(ToolCallEvent {
                call_id: CallId("call-1".into()),
                name: "shell".into(),
                input: json!({ "command": "cargo test" }),
            }),
        )
        .await;
    wait_until(|| {
        channel.edited_messages().iter().any(|message| {
            message.message.body.contains("🔧 shell(cargo test)")
                && message.message.body.contains("1 steps")
        })
    })
    .await;
    provider
        .emit_to_session(
            "thread-1",
            Event::ToolResult(ToolResultEvent {
                call_id: CallId("call-1".into()),
                output: json!("ok"),
                is_error: Some(false),
            }),
        )
        .await;
    provider
        .emit_to_session(
            "thread-1",
            Event::Message(MessageEvent {
                role: MessageRole::Assistant,
                text: "final answer".into(),
                streaming: false,
            }),
        )
        .await;
    provider
        .emit_to_session(
            "thread-1",
            Event::TurnCompleted(lucarne::agent_runtime::events::TurnCompletedEvent {
                turn_id: "turn-1".into(),
                usage: None,
            }),
        )
        .await;
    let final_answer = eventually_topic_message(&channel, "9", |message| {
        message.body.contains("final answer") && message.format == TextFormat::Markdown
    })
    .await;
    assert_eq!(final_answer.format, TextFormat::Markdown);
    assert!(!final_answer.body.contains("\n\n---\n\n"));
    assert!(!final_answer.body.contains("session: `thread-1`"));
    assert!(!final_answer.body.contains("cwd: `/tmp/project-a`"));
    channel.close_events();
    timeout(Duration::from_secs(2), run)
        .await
        .expect("prompt journey run should stop")
        .expect("prompt journey task should not panic");
}

#[tokio::test(flavor = "current_thread")]
async fn process_draft_is_visible_omitted_then_deleted_after_turn_completion() {
    let _test_lock = test_lock();
    let provider = Arc::new(ProviderProbe::with_pending_submit_events(Vec::new()));
    let core = core_with_provider(Arc::clone(&provider));
    bind_workspace_with_project_path(&core, "workspace-a", "9", "thread-1", "/tmp/project-a");

    let channel = Arc::new(RecordingChannel::streaming());
    let bot = bot_with_state(Arc::clone(&channel), Arc::clone(&core));
    let run = tokio::spawn(Arc::clone(&bot).run());
    channel.push_event(ChannelEvent::Message(topic_message(
        "9",
        "m-prompt",
        "show progress",
    )));

    wait_until(|| provider.submit_calls() == vec![("thread-1".into(), "show progress".into())])
        .await;
    let long_reasoning = format!("{}LATEST_PROCESS_TAIL", "old process detail ".repeat(160));
    provider
        .emit_to_session(
            "thread-1",
            Event::Reasoning(ReasoningEvent {
                text: long_reasoning.into(),
            }),
        )
        .await;

    assert!(
        eventually(|| {
            topic_messages(&channel, "9").iter().any(|sent| {
                sent.message.body.contains("earlier updates omitted")
                    && sent.message.body.contains("LATEST_PROCESS_TAIL")
                    && !sent
                        .message
                        .body
                        .starts_with("old process detail old process detail")
            })
        })
        .await,
        "process draft should be visible with old content omitted; sent={:?}; edits={:?}",
        channel.sent_messages(),
        channel.edited_messages()
    );
    let process_message_id = topic_messages(&channel, "9")
        .into_iter()
        .find(|sent| sent.message.body.contains("LATEST_PROCESS_TAIL"))
        .expect("process draft message")
        .id;

    provider
        .emit_to_session(
            "thread-1",
            Event::TurnCompleted(lucarne::agent_runtime::events::TurnCompletedEvent {
                turn_id: "turn-1".into(),
                usage: None,
            }),
        )
        .await;

    assert!(
        eventually(|| {
            channel
                .deleted_messages()
                .iter()
                .any(|id| id == &process_message_id)
        })
        .await,
        "process draft should be deleted after turn completion; process_message_id={process_message_id}; sent={:?}; edits={:?}; deleted={:?}",
        channel.sent_messages(),
        channel.edited_messages(),
        channel.deleted_messages()
    );
    assert!(
        active_topic_messages(&channel, "9")
            .iter()
            .all(|sent| !sent.message.body.contains("LATEST_PROCESS_TAIL")),
        "completed turn must delete process draft: sent={:?} edits={:?} deleted={:?}",
        channel.sent_messages(),
        channel.edited_messages(),
        channel.deleted_messages()
    );
    assert!(
        active_topic_messages(&channel, "9")
            .iter()
            .any(|sent| sent.message.body.contains("✓ 完成")),
        "completion status should remain visible"
    );

    channel.close_events();
    timeout(Duration::from_secs(2), run)
        .await
        .expect("process cleanup run should stop")
        .expect("process cleanup task should not panic");
}

#[tokio::test(flavor = "current_thread")]
async fn topic_prompt_reports_agent_start_error_to_telegram_user() {
    let _test_lock = test_lock();
    let provider = Arc::new(ProviderProbe::with_resume_error(
        "adapter: resolve command \"pi\" in PATH",
    ));
    let core = core_with_provider(Arc::clone(&provider));
    bind_workspace_with_project_path(&core, "workspace-a", "9", "thread-1", "/tmp/project-a");

    let channel = Arc::new(RecordingChannel::streaming());
    let bot = bot_with_state(Arc::clone(&channel), Arc::clone(&core));
    let run = tokio::spawn(Arc::clone(&bot).run());
    channel.push_event(ChannelEvent::Message(topic_message(
        "9",
        "m-prompt",
        "run tests",
    )));

    let failure = eventually_topic_message(&channel, "9", |message| {
        message.body.contains("⚠ agent start failed")
            && message.body.contains("resolve command \"pi\" in PATH")
    })
    .await;
    assert_eq!(failure.format, TextFormat::Markdown);
    assert_eq!(provider.resumes(), vec!["thread-1".to_string()]);

    channel.close_events();
    timeout(Duration::from_secs(2), run)
        .await
        .expect("error reporting run should stop")
        .expect("error reporting task should not panic");
}

#[tokio::test(flavor = "current_thread")]
async fn image_attachment_long_caption_is_split_at_telegram_delivery() {
    use base64::Engine as _;

    let _test_lock = test_lock();
    let provider = Arc::new(ProviderProbe::with_pending_submit_events(Vec::new()));
    let core = core_with_provider(Arc::clone(&provider));
    bind_workspace_with_project_path(&core, "workspace-a", "9", "thread-1", "/tmp/project-a");

    let channel = Arc::new(RecordingChannel::streaming());
    let bot = bot_with_state(Arc::clone(&channel), Arc::clone(&core));
    let run = tokio::spawn(Arc::clone(&bot).run());
    channel.push_event(ChannelEvent::Message(topic_message(
        "9",
        "m-prompt",
        "generate image",
    )));

    wait_until(|| provider.submit_calls() == vec![("thread-1".into(), "generate image".into())])
        .await;
    let caption = format!("{}OVERFLOW_TAIL", "x".repeat(900));
    provider
        .emit_to_session(
            "thread-1",
            Event::Attachment(AgentAttachment {
                id: "ig_long".into(),
                filename: "codex-image-ig_long.png".into(),
                media_type: "image/png".into(),
                data_base64: base64::engine::general_purpose::STANDARD.encode([1u8, 2, 3]),
                caption: Some(caption.into()),
            }),
        )
        .await;
    provider
        .emit_to_session(
            "thread-1",
            Event::TurnCompleted(lucarne::agent_runtime::events::TurnCompletedEvent {
                turn_id: "turn-1".into(),
                usage: None,
            }),
        )
        .await;

    assert!(
        eventually(|| {
            channel.sent_attachments().len() == 1
                && topic_messages(&channel, "9")
                    .iter()
                    .any(|sent| sent.message.body == "OVERFLOW_TAIL")
        })
        .await,
        "long caption should produce one attachment and one overflow message; attachments={:?}; sent={:?}",
        channel.sent_attachments(),
        channel.sent_messages()
    );
    let attachment = channel
        .sent_attachments()
        .into_iter()
        .next()
        .expect("sent attachment");
    assert_eq!(attachment.chat, "100");
    assert_eq!(attachment.topic, "9");
    let attached_caption = attachment
        .attachment
        .caption
        .as_deref()
        .expect("attachment caption");
    assert_eq!(attached_caption.chars().count(), 900);
    assert!(attached_caption.chars().all(|ch| ch == 'x'));
    let overflow = topic_messages(&channel, "9")
        .into_iter()
        .find(|sent| sent.message.body == "OVERFLOW_TAIL")
        .expect("overflow message");
    assert_eq!(overflow.message.format, TextFormat::Plain);
    assert_eq!(
        overflow.message.reply_to,
        Some(MessageId::new(attachment.id))
    );

    channel.close_events();
    timeout(Duration::from_secs(2), run)
        .await
        .expect("image caption split run should stop")
        .expect("image caption split task should not panic");
}

#[tokio::test(flavor = "current_thread")]
async fn topic_help_command_renders_workspace_help_without_expanding_provider_commands() {
    let _test_lock = test_lock();
    let provider = Arc::new(ProviderProbe::with_command_catalog(AgentCommandCatalog {
        commands: vec![command_catalog_entry("lint", "Run lint checks")],
        complete: true,
        revision: 42,
    }));
    let core = core_with_provider(Arc::clone(&provider));
    bind_workspace(&core, "workspace-a", "9", "thread-1");

    let channel = Arc::new(RecordingChannel::with_events(vec![ChannelEvent::Message(
        topic_message("9", "m-help", "/help"),
    )]));
    let bot = bot_with_state(Arc::clone(&channel), Arc::clone(&core));
    timeout(Duration::from_secs(2), bot.run())
        .await
        .expect("topic help run should finish after finite event stream");

    let help = eventually_topic_message(&channel, "9", |message| {
        message.body.contains("workspace commands") && message.body.contains("`/commands`")
    })
    .await;
    assert_eq!(help.format, TextFormat::Markdown);
    assert!(help.body.contains("`/status`"));
    assert!(help.body.contains("`/help`"));
    assert!(!help.body.contains("`/lint`"));
    assert!(
        provider.inputs().is_empty(),
        "/help must not be submitted as a provider prompt"
    );
}

#[tokio::test(flavor = "current_thread")]
async fn topic_command_help_covers_provider_topic_and_session_trait_commands() {
    let _test_lock = test_lock();
    let provider = Arc::new(ProviderProbe::with_command_catalog(AgentCommandCatalog {
        commands: vec![AgentCommand {
            name: "lint".into(),
            description: Some("Run lint checks".into()),
            aliases: vec!["l".into()],
            source: AgentCommandSource::ProviderNative,
            input: AgentCommandInput::None,
            completion: AgentCommandCompletion::CommandResult,
        }],
        complete: true,
        revision: 42,
    }));
    let core = core_with_provider(Arc::clone(&provider));
    bind_workspace(&core, "workspace-a", "9", "thread-1");

    for (message_id, text, expected) in [
        ("m-provider-help", "/lint help", "`/lint`"),
        ("m-provider-alias-help", "/l help", "aliases: `/l`"),
        ("m-topic-help", "/rename help", "usage: `/rename <name>`"),
        (
            "m-trait-help",
            "/model help",
            "Show or set the agent model.",
        ),
        (
            "m-commands-help",
            "/commands help",
            "List or invoke bound agent commands.",
        ),
        (
            "m-config-help",
            "/config help",
            "Show or set scoped config.",
        ),
    ] {
        let channel = Arc::new(RecordingChannel::with_events(vec![ChannelEvent::Message(
            topic_message("9", message_id, text),
        )]));
        let bot = bot_with_state(Arc::clone(&channel), Arc::clone(&core));

        timeout(Duration::from_secs(2), bot.run())
            .await
            .expect("topic command help run should finish after finite event stream");

        let help = eventually_topic_message(&channel, "9", |message| {
            message.format == TextFormat::Markdown && message.body.contains(expected)
        })
        .await;
        if text == "/commands help" {
            assert!(
                !help.body.contains("`/lint`"),
                "{text} must not expand the provider command catalog: {:?}",
                help.body
            );
        }
    }
    assert!(
        provider.inputs().is_empty(),
        "help requests must not be submitted as provider prompts"
    );
    assert!(
        provider.submit_calls().is_empty(),
        "help requests must not be submitted to the live session"
    );
}

#[tokio::test(flavor = "current_thread")]
async fn command_query_uses_last_topic_workspace_catalog_for_user() {
    let _test_lock = test_lock();
    let provider = Arc::new(ProviderProbe::with_command_catalog(AgentCommandCatalog {
        commands: vec![
            command_catalog_entry("lint", "Run lint checks"),
            command_catalog_entry("deploy", "Deploy current workspace"),
        ],
        complete: true,
        revision: 7,
    }));
    let core = core_with_provider(Arc::clone(&provider));
    bind_workspace(&core, "workspace-a", "9", "thread-1");
    let state = BotState::new_with_core(Arc::clone(&core));

    let turn_channel = Arc::new(RecordingChannel::with_events(vec![ChannelEvent::Message(
        topic_message("9", "m-command-query-context", "remember this topic"),
    )]));
    let turn_bot = bot_with_existing_state(
        Arc::clone(&turn_channel),
        Arc::clone(&core),
        Arc::clone(&state),
    );
    timeout(Duration::from_secs(2), turn_bot.run())
        .await
        .expect("context turn should finish after finite event stream");
    wait_until(|| provider.inputs().len() == 1).await;

    let query_channel = Arc::new(RecordingChannel::with_events(vec![
        ChannelEvent::CommandQuery(CommandQuery {
            id: "inline-1".into(),
            user: "alice".into(),
            query: "lin".into(),
            chat_type: Some("supergroup".into()),
        }),
    ]));
    let query_bot = bot_with_existing_state(query_channel.clone(), Arc::clone(&core), state);
    timeout(Duration::from_secs(2), query_bot.run())
        .await
        .expect("command query run should finish after finite event stream");

    wait_until(|| query_channel.command_query_answers().len() == 1).await;
    let answers = query_channel.command_query_answers();
    assert_eq!(answers[0].0, "inline-1");
    assert_eq!(answers[0].1.len(), 1);
    assert_eq!(answers[0].1[0].message_text, "/lint");
    assert!(
        answers[0]
            .1
            .iter()
            .all(|result| result.message_text != "/status"),
        "inline command query must use the provider catalog, not injected session-trait commands"
    );
}

#[tokio::test(flavor = "current_thread")]
async fn watch_notification_topic_receives_agent_message_and_reply_routes_to_session() {
    let _test_lock = test_lock();
    let provider = Arc::new(ProviderProbe::default());
    let core = core_with_provider(Arc::clone(&provider));
    bind_workspace(&core, "workspace-a", "9", "thread-1");
    core.resume_workspace_with_events(ResumeWorkspaceRequest {
        workspace_id: ControlWorkspaceId::new("workspace-a"),
        force_bypass_permissions: false,
    })
    .await
    .expect("resume workspace for watched events");

    let channel = Arc::new(RecordingChannel::streaming());
    let state = BotState::new_with_core(Arc::clone(&core));
    let bot = bot_with_existing_state(Arc::clone(&channel), Arc::clone(&core), Arc::clone(&state));
    let run = tokio::spawn(Arc::clone(&bot).run());
    wait_for_channel_subscription(&channel).await;

    provider
        .emit_to_session("thread-1", assistant_message("background finished"))
        .await;
    wait_until(|| {
        channel.sent_messages().into_iter().any(|sent| {
            sent.message.body.contains("background finished")
                && sent.message.body.contains("session: `thread-1`")
                && sent.message.body.contains("cwd: `/tmp/workspace-a`")
        })
    })
    .await;
    let notification = channel
        .sent_messages()
        .into_iter()
        .find(|sent| {
            sent.message.body.contains("background finished")
                && sent.message.body.contains("session: `thread-1`")
                && sent.message.body.contains("cwd: `/tmp/workspace-a`")
        })
        .expect("notification message");
    let notification_topics = channel
        .created_workspaces()
        .into_iter()
        .filter(|(_, title, _)| title == "agent notifications")
        .collect::<Vec<_>>();
    assert_eq!(
        notification_topics.len(),
        1,
        "watch notifications must use one dedicated topic"
    );
    assert_eq!(notification.topic, notification_topics[0].2);

    channel.push_event(ChannelEvent::Message(reply_to_topic_message(
        &notification.topic,
        "m-notification-reply",
        &notification.id,
        "please continue from notification",
    )));
    wait_until(|| {
        provider.submit_calls().contains(&(
            "thread-1".into(),
            "please continue from notification".into(),
        ))
    })
    .await;
    wait_until(|| {
        provider.resume_args().iter().any(|args| {
            args.get("permission_mode").and_then(|mode| mode.as_str()) == Some("bypass")
        })
    })
    .await;
    let reply = eventually_topic_message(&channel, &notification.topic, |message| {
        message.body.contains("ok")
            && message.body.contains("\n\n---\n\ncost: ")
            && message.format == TextFormat::Markdown
    })
    .await;
    assert_eq!(reply.format, TextFormat::Markdown);
    let footer = reply
        .body
        .split_once("\n\n---\n\ncost: ")
        .map(|(_, footer)| footer)
        .unwrap_or_else(|| {
            panic!(
                "notification reply footer missing cost block: {}",
                reply.body
            )
        });
    assert!(
        footer.contains("\nsession: `thread-1`\ncwd: `/tmp/workspace-a`"),
        "notification reply footer must keep cost/session/cwd together: {}",
        reply.body
    );
    assert!(
        channel.sent_messages().iter().any(|sent| {
            sent.topic == notification.topic
                && sent.message.body.contains("ok")
                && sent
                    .message
                    .reply_to
                    .as_ref()
                    .is_some_and(|reply| reply.as_str() == "m-notification-reply")
        }),
        "notification reply turn should be anchored to the user instruction"
    );

    channel.close_events();
    timeout(Duration::from_secs(2), run)
        .await
        .expect("watch notification run should stop after channel close")
        .expect("watch notification task should join");
}

#[tokio::test(flavor = "current_thread")]
async fn notification_topic_reply_status_uses_workspace_command_path_not_agent_text() {
    let _test_lock = test_lock();
    let provider = Arc::new(ProviderProbe {
        status: AgentStatus {
            version: Some("codex-test".into()),
            model: Some("gpt-5".into()),
            model_detail: Some("high".into()),
            reasoning: Some("max".into()),
            permissions: Some("danger-full-access".into()),
            directory: Some("/tmp/workspace-a".into()),
            ..Default::default()
        },
        process_id: Some(4242),
        ..ProviderProbe::default()
    });
    let core = core_with_provider(Arc::clone(&provider));
    bind_workspace(&core, "workspace-a", "9", "thread-1");
    core.resume_workspace_with_events(ResumeWorkspaceRequest {
        workspace_id: ControlWorkspaceId::new("workspace-a"),
        force_bypass_permissions: false,
    })
    .await
    .expect("resume workspace for watched events");

    let channel = Arc::new(RecordingChannel::streaming());
    let state = BotState::new_with_core(Arc::clone(&core));
    let bot = bot_with_existing_state(Arc::clone(&channel), Arc::clone(&core), Arc::clone(&state));
    let run = tokio::spawn(Arc::clone(&bot).run());
    wait_for_channel_subscription(&channel).await;

    provider
        .emit_to_session("thread-1", assistant_message("background status source"))
        .await;
    let notification = eventually_topic_sent_message(&channel, |sent| {
        sent.message.body.contains("background status source")
            && sent.message.body.contains("session: `thread-1`")
            && sent.topic != "9"
    })
    .await;

    channel.push_event(ChannelEvent::Message(reply_to_topic_message(
        &notification.topic,
        "m-notification-status-reply",
        &notification.id,
        "/status",
    )));

    wait_until(|| provider.status_calls() == 1).await;
    assert!(
        provider
            .submit_calls()
            .into_iter()
            .all(|(_, text)| text != "/status"),
        "notification /status reply must not be submitted as agent text"
    );
    let status = eventually_topic_sent_message(&channel, |sent| {
        sent.topic == notification.topic
            && sent
                .message
                .reply_to
                .as_ref()
                .is_some_and(|reply| reply.as_str() == "m-notification-status-reply")
            && sent.message.body.contains("status\n")
            && sent.message.body.contains("Version: `codex-test`")
            && sent
                .message
                .body
                .contains("Model: `gpt-5` (`high`, reasoning max)")
            && sent
                .message
                .body
                .contains("Permissions: `danger-full-access`")
            && sent
                .message
                .body
                .contains("Process identity: `thread-1:4242`")
    })
    .await;

    channel.push_event(ChannelEvent::Message(reply_to_topic_message(
        &notification.topic,
        "m-notification-after-status",
        &status.id,
        "continue after status",
    )));
    wait_until(|| {
        provider
            .submit_calls()
            .contains(&("thread-1".into(), "continue after status".into()))
    })
    .await;

    channel.close_events();
    timeout(Duration::from_secs(2), run)
        .await
        .expect("notification status run should stop after channel close")
        .expect("notification status task should join");
}

#[tokio::test(flavor = "current_thread")]
async fn bot_run_receives_history_watch_notifications_started_by_supervisor() {
    let _test_lock = test_lock();
    let history_env = IsolatedHistoryEnv::new();
    let root = history_env.codex_home().join("sessions");
    let project = history_env.codex_home().join("project");
    fs::create_dir_all(&project).expect("create project dir");
    let session_path = write_live_watch_codex_session(
        &root,
        "watch-thread-bot-run",
        &project,
        "initial bot-run watch prompt",
    );
    let provider = Arc::new(ProviderProbe::default());
    let core = core_with_provider(Arc::clone(&provider));
    let channel = Arc::new(RecordingChannel::streaming());
    let bot = bot_with_state_and_history_watch(Arc::clone(&channel), Arc::clone(&core));
    let run = tokio::spawn(Arc::clone(&bot).run());
    wait_for_channel_subscription(&channel).await;
    sleep(Duration::from_millis(50)).await;

    let codex = agent_provider("codex").expect("codex provider descriptor");
    core.start_history_session_watch_with_config(
        WatchConfig::new()
            .providers([codex])
            .provider_roots(codex, [root.clone()])
            .selection(ParseSelection::empty().with_meta().with_messages())
            .debounce(Duration::from_millis(25)),
    )
    .expect("supervisor starts live history watch");

    let deadline = Instant::now() + Duration::from_secs(10);
    let mut attempt = 0;
    loop {
        attempt += 1;
        let text = format!("bot run watch complete {attempt}");
        append_codex_assistant_response(&session_path, "2026-05-06T00:00:02.000Z", &text);
        if eventually_for(Duration::from_secs(1), || {
            channel.sent_messages().iter().any(|sent| {
                sent.message.body.contains(text.as_str())
                    && sent
                        .message
                        .body
                        .contains("session: `watch-thread-bot-run`")
                    && sent
                        .message
                        .body
                        .contains(compact_cwd_footer(&project).as_str())
            })
        })
        .await
        {
            break;
        }
        assert!(
            Instant::now() < deadline,
            "bot-run history watcher did not notify for appended session changes"
        );
    }

    channel.close_events();
    timeout(Duration::from_secs(2), run)
        .await
        .expect("bot-run watch notification run should stop")
        .expect("bot-run watch notification task should join");
}

#[tokio::test(flavor = "current_thread")]
async fn live_history_watch_file_append_reaches_notification_topic() {
    let _test_lock = test_lock();
    let temp = TempDir::new().expect("live watch temp dir");
    let root = temp.path().join("codex-home").join("sessions");
    let project = temp.path().join("project");
    fs::create_dir_all(&project).expect("create project dir");
    let session_path = write_live_watch_codex_session(
        &root,
        "watch-thread-telegram",
        &project,
        "initial telegram prompt",
    );
    let provider = Arc::new(ProviderProbe::default());
    let core = core_with_provider(Arc::clone(&provider));
    let channel = Arc::new(RecordingChannel::streaming());
    let bot = bot_with_state(Arc::clone(&channel), Arc::clone(&core));
    let run = tokio::spawn(Arc::clone(&bot).run());
    wait_for_channel_subscription(&channel).await;
    sleep(Duration::from_millis(50)).await;

    let codex = agent_provider("codex").expect("codex provider descriptor");
    core.start_history_session_watch_with_config(
        WatchConfig::new()
            .providers([codex])
            .provider_roots(codex, [root.clone()])
            .selection(ParseSelection::empty().with_meta().with_messages())
            .debounce(Duration::from_millis(25)),
    )
    .expect("start live history watch");
    append_codex_assistant_response(
        &session_path,
        "2026-05-06T00:00:02.000Z",
        "telegram live watch complete",
    );

    let notification =
        eventually_topic_sent_message_with_timeout(&channel, Duration::from_secs(3), |sent| {
            sent.message.body.contains("telegram live watch complete")
                && sent
                    .message
                    .body
                    .contains("session: `watch-thread-telegram`")
                && sent
                    .message
                    .body
                    .contains(compact_cwd_footer(&project).as_str())
        })
        .await;
    assert_ne!(notification.topic, "9");
    assert_eq!(
        channel
            .created_workspaces()
            .into_iter()
            .filter(|(_, title, _)| title == "agent notifications")
            .count(),
        1
    );

    channel.close_events();
    timeout(Duration::from_secs(2), run)
        .await
        .expect("live watch notification run should stop")
        .expect("live watch notification task should join");
}

#[tokio::test(flavor = "current_thread")]
async fn background_notification_reply_approval_buttons_resolve_in_notification_topic() {
    let _test_lock = test_lock();
    let provider = Arc::new(ProviderProbe::with_pending_submit_events(vec![
        Event::InterventionRequest(lucarne::agent_runtime::InterventionRequest::Approval(
            ApprovalRequest {
                req_id: "approval-notification-1".into(),
                tool_name: "edit".into(),
                message: Some("needs approval from notification".into()),
                input: None,
            },
        )),
    ]));
    let core = core_with_provider(Arc::clone(&provider));
    bind_workspace(&core, "workspace-a", "9", "thread-1");
    core.resume_workspace_with_events(ResumeWorkspaceRequest {
        workspace_id: ControlWorkspaceId::new("workspace-a"),
        force_bypass_permissions: false,
    })
    .await
    .expect("resume workspace for watched events");

    let channel = Arc::new(RecordingChannel::streaming());
    let state = BotState::new_with_core(Arc::clone(&core));
    let bot = bot_with_existing_state(Arc::clone(&channel), Arc::clone(&core), Arc::clone(&state));
    let run = tokio::spawn(Arc::clone(&bot).run());
    wait_for_channel_subscription(&channel).await;
    assert!(
        state
            .get(&WorkspaceId::new("workspace-a"))
            .is_none_or(|session| session.live.is_none()),
        "this journey must start from a background session, not an active Telegram topic turn"
    );

    provider
        .emit_to_session("thread-1", assistant_message("background approval source"))
        .await;
    let notification = eventually_topic_sent_message(&channel, |sent| {
        sent.message.body.contains("background approval source")
            && sent.message.body.contains("session: `thread-1`")
            && sent.message.body.contains("cwd: `/tmp/workspace-a`")
            && sent.topic != "9"
    })
    .await;

    channel.push_event(ChannelEvent::Message(reply_to_topic_message(
        &notification.topic,
        "m-notification-approval-reply",
        &notification.id,
        "please run approval from notification",
    )));
    wait_until(|| {
        provider.submit_calls().contains(&(
            "thread-1".into(),
            "please run approval from notification".into(),
        ))
    })
    .await;

    let approval = eventually_topic_sent_message(&channel, |sent| {
        sent.topic == notification.topic
            && sent
                .message
                .body
                .contains("needs approval from notification")
    })
    .await;
    let approve_data = approval
        .message
        .buttons
        .iter()
        .flatten()
        .find(|button| button.data.starts_with("intv:c:"))
        .map(|button| button.data.clone())
        .unwrap_or_else(|| panic!("missing approval button: {:?}", approval.message));

    channel.push_event(ChannelEvent::Button {
        chat: ChatId::new("100"),
        workspace: Some(WorkspaceId::new(notification.topic.clone())),
        user: "alice".into(),
        data: approve_data,
        source_message: MessageId::new(approval.id.clone()),
    });

    wait_until(|| provider.resolved_interventions().len() == 1).await;
    assert_eq!(
        provider.resolved_interventions(),
        vec![(
            "approval-notification-1".to_string(),
            InterventionResponse::Approval(ApprovalDecision::Allow)
        )]
    );
    eventually_topic_sent_message(&channel, |sent| {
        sent.topic == notification.topic && sent.message.body.contains("resolved")
    })
    .await;

    channel.close_events();
    timeout(Duration::from_secs(2), run)
        .await
        .expect("notification approval run should stop after channel close")
        .expect("notification approval task should join");
}

#[tokio::test(flavor = "current_thread")]
async fn watch_notification_topic_is_shared_hub_for_multiple_background_sessions() {
    let _test_lock = test_lock();
    let provider = Arc::new(ProviderProbe::default());
    let core = core_with_provider(Arc::clone(&provider));
    bind_workspace(&core, "workspace-a", "9", "thread-a");
    bind_workspace(&core, "workspace-b", "10", "thread-b");
    core.resume_workspace_with_events(ResumeWorkspaceRequest {
        workspace_id: ControlWorkspaceId::new("workspace-a"),
        force_bypass_permissions: false,
    })
    .await
    .expect("resume first background workspace");
    core.resume_workspace_with_events(ResumeWorkspaceRequest {
        workspace_id: ControlWorkspaceId::new("workspace-b"),
        force_bypass_permissions: false,
    })
    .await
    .expect("resume second background workspace");

    let channel = Arc::new(RecordingChannel::streaming());
    let state = BotState::new_with_core(Arc::clone(&core));
    let bot = bot_with_existing_state(Arc::clone(&channel), Arc::clone(&core), Arc::clone(&state));
    let run = tokio::spawn(Arc::clone(&bot).run());
    wait_for_channel_subscription(&channel).await;

    provider
        .emit_to_session(
            "thread-a",
            assistant_message("background session a finished"),
        )
        .await;
    provider
        .emit_to_session(
            "thread-b",
            assistant_message("background session b finished"),
        )
        .await;

    let notification_a = eventually_topic_sent_message(&channel, |sent| {
        sent.message.body.contains("background session a finished")
            && sent.message.body.contains("session: `thread-a`")
            && sent.message.body.contains("cwd: `/tmp/workspace-a`")
            && sent.topic != "9"
            && sent.topic != "10"
    })
    .await;
    let notification_b = eventually_topic_sent_message(&channel, |sent| {
        sent.message.body.contains("background session b finished")
            && sent.message.body.contains("session: `thread-b`")
            && sent.message.body.contains("cwd: `/tmp/workspace-b`")
            && sent.topic != "9"
            && sent.topic != "10"
    })
    .await;
    assert_eq!(
        notification_a.topic, notification_b.topic,
        "notifications from different sessions must share the notification hub topic"
    );
    let notification_topics = channel
        .created_workspaces()
        .into_iter()
        .filter(|(_, title, _)| title == "agent notifications")
        .collect::<Vec<_>>();
    assert_eq!(
        notification_topics.len(),
        1,
        "the notification hub topic should be created once"
    );
    assert_eq!(notification_a.topic, notification_topics[0].2);

    channel.push_event(ChannelEvent::Message(reply_to_topic_message(
        &notification_a.topic,
        "m-notification-reply-a",
        &notification_a.id,
        "continue session a",
    )));
    channel.push_event(ChannelEvent::Message(reply_to_topic_message(
        &notification_b.topic,
        "m-notification-reply-b",
        &notification_b.id,
        "continue session b",
    )));
    wait_until(|| {
        let calls = provider.submit_calls();
        calls.contains(&("thread-a".into(), "continue session a".into()))
            && calls.contains(&("thread-b".into(), "continue session b".into()))
    })
    .await;
    eventually_topic_sent_message(&channel, |sent| {
        sent.topic == notification_a.topic
            && sent.message.body.contains("ok")
            && sent
                .message
                .reply_to
                .as_ref()
                .is_some_and(|reply| reply.as_str() == "m-notification-reply-a")
    })
    .await;
    eventually_topic_sent_message(&channel, |sent| {
        sent.topic == notification_b.topic
            && sent.message.body.contains("ok")
            && sent
                .message
                .reply_to
                .as_ref()
                .is_some_and(|reply| reply.as_str() == "m-notification-reply-b")
    })
    .await;
    assert!(
        topic_messages(&channel, "9")
            .iter()
            .chain(topic_messages(&channel, "10").iter())
            .all(|sent| !sent.message.body.contains("ok")),
        "notification reply output must stay in the notification hub topic"
    );

    channel.close_events();
    timeout(Duration::from_secs(2), run)
        .await
        .expect("shared notification hub run should stop after channel close")
        .expect("shared notification hub task should join");
}

#[tokio::test(flavor = "current_thread")]
async fn concurrent_background_notifications_register_one_notification_topic() {
    let _test_lock = test_lock();
    let provider = Arc::new(ProviderProbe::default());
    let core = core_with_provider(Arc::clone(&provider));
    bind_workspace(&core, "workspace-a", "9", "thread-a");
    bind_workspace(&core, "workspace-b", "10", "thread-b");
    core.resume_workspace_with_events(ResumeWorkspaceRequest {
        workspace_id: ControlWorkspaceId::new("workspace-a"),
        force_bypass_permissions: false,
    })
    .await
    .expect("resume first background workspace");
    core.resume_workspace_with_events(ResumeWorkspaceRequest {
        workspace_id: ControlWorkspaceId::new("workspace-b"),
        force_bypass_permissions: false,
    })
    .await
    .expect("resume second background workspace");

    let channel = Arc::new(RecordingChannel::streaming_with_create_workspace_delay(
        Duration::from_millis(80),
    ));
    let bot = bot_with_state(Arc::clone(&channel), Arc::clone(&core));
    let run = tokio::spawn(Arc::clone(&bot).run());
    wait_for_channel_subscription(&channel).await;

    provider
        .emit_to_session("thread-a", assistant_message("concurrent notify a"))
        .await;
    provider
        .emit_to_session("thread-b", assistant_message("concurrent notify b"))
        .await;

    let notification_a = eventually_topic_sent_message(&channel, |sent| {
        sent.message.body.contains("concurrent notify a")
    })
    .await;
    let notification_b = eventually_topic_sent_message(&channel, |sent| {
        sent.message.body.contains("concurrent notify b")
    })
    .await;
    assert_eq!(
        notification_a.topic, notification_b.topic,
        "concurrent notifications must share the same notification topic"
    );
    let notification_topics = channel
        .created_workspaces()
        .into_iter()
        .filter(|(_, title, _)| title == "agent notifications")
        .collect::<Vec<_>>();
    assert_eq!(
        notification_topics.len(),
        1,
        "Telegram must register exactly one agent notification topic even when notifications arrive concurrently"
    );

    channel.close_events();
    timeout(Duration::from_secs(2), run)
        .await
        .expect("concurrent notification task should stop after channel close")
        .expect("concurrent notification task should join");
}

#[tokio::test(flavor = "current_thread")]
async fn notification_topic_is_reused_after_bot_state_restart() {
    let _test_lock = test_lock();
    let provider = Arc::new(ProviderProbe::default());
    let core = core_with_provider(Arc::clone(&provider));
    bind_workspace(&core, "workspace-a", "9", "thread-a");
    core.resume_workspace_with_events(ResumeWorkspaceRequest {
        workspace_id: ControlWorkspaceId::new("workspace-a"),
        force_bypass_permissions: false,
    })
    .await
    .expect("resume first background workspace");

    let first_channel = Arc::new(RecordingChannel::streaming());
    let first_bot = bot_with_state(Arc::clone(&first_channel), Arc::clone(&core));
    let first_run = tokio::spawn(Arc::clone(&first_bot).run());
    wait_for_channel_subscription(&first_channel).await;

    provider
        .emit_to_session("thread-a", assistant_message("first restart notify"))
        .await;
    let first_notification = eventually_topic_sent_message(&first_channel, |sent| {
        sent.message.body.contains("first restart notify")
    })
    .await;
    assert_eq!(
        first_channel
            .created_workspaces()
            .into_iter()
            .filter(|(_, title, _)| title == "agent notifications")
            .count(),
        1,
        "first bot run should create the shared notification topic once"
    );

    first_channel.close_events();
    timeout(Duration::from_secs(2), first_run)
        .await
        .expect("first notification task should stop after channel close")
        .expect("first notification task should join");

    let second_channel = Arc::new(RecordingChannel::streaming());
    let second_bot = bot_with_state(Arc::clone(&second_channel), Arc::clone(&core));
    let second_run = tokio::spawn(Arc::clone(&second_bot).run());
    wait_for_channel_subscription(&second_channel).await;

    provider
        .emit_to_session("thread-a", assistant_message("second restart notify"))
        .await;
    let second_notification = eventually_topic_sent_message(&second_channel, |sent| {
        sent.message.body.contains("second restart notify")
    })
    .await;
    assert_eq!(
        second_notification.topic, first_notification.topic,
        "restarted Telegram bot state must reuse the persisted notification topic"
    );
    assert!(
        second_channel
            .created_workspaces()
            .into_iter()
            .all(|(_, title, _)| title != "agent notifications"),
        "restarted Telegram bot state must not create another notification topic"
    );

    second_channel.close_events();
    timeout(Duration::from_secs(2), second_run)
        .await
        .expect("second notification task should stop after channel close")
        .expect("second notification task should join");
}

#[tokio::test(flavor = "current_thread")]
async fn reset_notifications_command_recreates_notification_topic() {
    let _test_lock = test_lock();
    let provider = Arc::new(ProviderProbe::default());
    let core = core_with_provider(Arc::clone(&provider));
    bind_workspace(&core, "workspace-a", "9", "thread-a");
    core.resume_workspace_with_events(ResumeWorkspaceRequest {
        workspace_id: ControlWorkspaceId::new("workspace-a"),
        force_bypass_permissions: false,
    })
    .await
    .expect("resume background workspace");

    let channel = Arc::new(RecordingChannel::streaming());
    let state = BotState::new_with_core(Arc::clone(&core));
    let bot = bot_with_existing_state(Arc::clone(&channel), Arc::clone(&core), Arc::clone(&state));
    let run = tokio::spawn(Arc::clone(&bot).run());
    wait_for_channel_subscription(&channel).await;

    provider
        .emit_to_session("thread-a", assistant_message("before reset notify"))
        .await;
    let old_notification = eventually_topic_sent_message(&channel, |sent| {
        sent.message.body.contains("before reset notify")
    })
    .await;

    channel.push_event(ChannelEvent::Message(topic_message(
        &old_notification.topic,
        "m-reset-notifications",
        "/reset_notifications",
    )));

    wait_until(|| {
        channel
            .deleted_workspaces()
            .iter()
            .any(|topic| topic == &old_notification.topic)
    })
    .await;
    wait_until(|| {
        channel
            .created_workspaces()
            .iter()
            .filter(|(_, title, _)| title == "agent notifications")
            .count()
            == 2
    })
    .await;
    let notification_topics = channel
        .created_workspaces()
        .into_iter()
        .filter(|(_, title, _)| title == "agent notifications")
        .collect::<Vec<_>>();
    let new_topic = notification_topics
        .last()
        .expect("new notification topic")
        .2
        .clone();
    assert_ne!(
        old_notification.topic, new_topic,
        "reset must create a fresh notification topic"
    );
    assert_eq!(
        state
            .notification_handle()
            .expect("persisted notification handle")
            .workspace
            .as_str(),
        new_topic
    );
    let confirmation = eventually_topic_sent_message(&channel, |sent| {
        sent.topic == new_topic
            && sent
                .message
                .body
                .contains("reset agent notifications topic")
    })
    .await;
    assert!(
        confirmation
            .message
            .body
            .contains("reopen Telegram to refresh the topic list"),
        "reset confirmation should explain Telegram mobile tab cache: {}",
        confirmation.message.body
    );

    provider
        .emit_to_session("thread-a", assistant_message("after reset notify"))
        .await;
    let new_notification = eventually_topic_sent_message(&channel, |sent| {
        sent.message.body.contains("after reset notify")
    })
    .await;
    assert_eq!(
        new_notification.topic, new_topic,
        "future notifications must use the rebuilt notification topic"
    );

    channel.close_events();
    timeout(Duration::from_secs(2), run)
        .await
        .expect("reset notification task should stop after channel close")
        .expect("reset notification task should join");
}

#[tokio::test(flavor = "current_thread")]
async fn watched_unbound_control_workspace_notifies_and_reply_routes() {
    let _test_lock = test_lock();
    let provider = Arc::new(ProviderProbe::default());
    let core = core_with_provider(Arc::clone(&provider));
    bind_workspace_without_topic(&core, "workspace-external", "thread-external");
    core.resume_workspace_with_events(ResumeWorkspaceRequest {
        workspace_id: ControlWorkspaceId::new("workspace-external"),
        force_bypass_permissions: false,
    })
    .await
    .expect("resume watched workspace");

    let channel = Arc::new(RecordingChannel::streaming());
    let state = BotState::new_with_core(Arc::clone(&core));
    let bot = bot_with_existing_state(Arc::clone(&channel), Arc::clone(&core), Arc::clone(&state));
    let run = tokio::spawn(Arc::clone(&bot).run());
    wait_for_channel_subscription(&channel).await;

    provider
        .emit_to_session(
            "thread-external",
            assistant_message("external session finished"),
        )
        .await;
    let notification = eventually_topic_sent_message(&channel, |sent| {
        sent.message.body.contains("external session finished")
            && sent.message.body.contains("session: `thread-external`")
            && sent.message.body.contains("cwd: `/tmp/workspace-external`")
    })
    .await;
    assert_eq!(
        channel
            .created_workspaces()
            .iter()
            .filter(|(_, title, _)| title == "agent notifications")
            .count(),
        1,
        "unbound watched sessions should use the shared notification hub"
    );

    channel.push_event(ChannelEvent::Message(reply_to_topic_message(
        &notification.topic,
        "m-external-reply",
        &notification.id,
        "continue external session",
    )));
    wait_until(|| {
        provider
            .submit_calls()
            .contains(&("thread-external".into(), "continue external session".into()))
    })
    .await;
    eventually_topic_sent_message(&channel, |sent| {
        sent.topic == notification.topic
            && sent.message.body.contains("ok")
            && sent
                .message
                .reply_to
                .as_ref()
                .is_some_and(|reply| reply.as_str() == "m-external-reply")
    })
    .await;

    channel.close_events();
    timeout(Duration::from_secs(2), run)
        .await
        .expect("unbound watch run should stop after channel close")
        .expect("unbound watch task should join");
}

#[tokio::test(flavor = "current_thread")]
async fn notification_reply_uses_bound_session_not_rebound_workspace() {
    let _test_lock = test_lock();
    let provider = Arc::new(ProviderProbe::default());
    let core = core_with_provider(Arc::clone(&provider));
    bind_workspace(&core, "workspace-a", "9", "thread-old");
    core.resume_workspace_with_events(ResumeWorkspaceRequest {
        workspace_id: ControlWorkspaceId::new("workspace-a"),
        force_bypass_permissions: false,
    })
    .await
    .expect("resume old workspace session");

    let channel = Arc::new(RecordingChannel::streaming());
    let state = BotState::new_with_core(Arc::clone(&core));
    let bot = bot_with_existing_state(Arc::clone(&channel), Arc::clone(&core), Arc::clone(&state));
    let run = tokio::spawn(Arc::clone(&bot).run());
    wait_for_channel_subscription(&channel).await;

    provider
        .emit_to_session("thread-old", assistant_message("old session finished"))
        .await;
    let notification = eventually_topic_sent_message(&channel, |sent| {
        sent.message.body.contains("old session finished")
            && sent.message.body.contains("session: `thread-old`")
            && sent.message.body.contains("cwd: `/tmp/workspace-a`")
            && sent.topic != "9"
    })
    .await;

    bind_workspace(&core, "workspace-a", "9", "thread-new");
    state
        .upsert_with_topic(
            WorkSession {
                workspace: WorkspaceId::new("workspace-a"),
                chat: ChatId::new("100"),
                provider_id: "codex",
                project_path: Some(PathBuf::from("/tmp/workspace-a")),
                title: "codex workspace-a rebound".into(),
                live: None,
                resume_ref: Some("thread-new".into()),
            },
            WorkspaceId::new("9"),
        )
        .expect("rebind workspace to a new provider session");

    channel.push_event(ChannelEvent::Message(reply_to_topic_message(
        &notification.topic,
        "m-old-notification-reply",
        &notification.id,
        "continue old notification",
    )));
    eventually_topic_message(&channel, &notification.topic, |message| {
        message.body.contains("no longer routable")
            && message
                .reply_to
                .as_ref()
                .is_some_and(|reply| reply.as_str() == "m-old-notification-reply")
    })
    .await;
    assert!(
        !provider
            .submit_calls()
            .contains(&("thread-new".into(), "continue old notification".into())),
        "old notification replies must not be routed through a rebound workspace"
    );

    channel.close_events();
    timeout(Duration::from_secs(2), run)
        .await
        .expect("notification rebind run should stop after channel close")
        .expect("notification rebind task should join");
}

#[tokio::test(flavor = "current_thread")]
async fn watch_notification_live_replay_routes_background_message_and_reply_without_real_agent() {
    let _test_lock = test_lock();
    let cassette = LiveReplayCassette::watch_notification();
    let session_ref = cassette.session_ref.clone();
    let background_text = first_assistant_text(&cassette.background_events);
    let reply_text = first_assistant_text(&cassette.submit_events);
    let provider = Arc::new(LiveReplayProvider::from_cassette(cassette.clone()));
    let core = core_with_live_replay_provider(Arc::clone(&provider));
    bind_workspace(&core, "workspace-live", "9", session_ref.as_str());

    let channel = Arc::new(RecordingChannel::streaming());
    let bot = bot_with_state(Arc::clone(&channel), Arc::clone(&core));
    let run = tokio::spawn(Arc::clone(&bot).run());
    wait_for_channel_subscription(&channel).await;

    core.resume_workspace_with_events(ResumeWorkspaceRequest {
        workspace_id: ControlWorkspaceId::new("workspace-live"),
        force_bypass_permissions: false,
    })
    .await
    .expect("resume live replay workspace for watched events");

    let notification = eventually_topic_sent_message(&channel, |sent| {
        sent.message.body.contains(background_text.as_str())
            && sent
                .message
                .body
                .contains(format!("session: `{session_ref}`").as_str())
            && sent.message.body.contains("cwd: `/tmp/workspace-live`")
            && sent.topic != "9"
    })
    .await;
    let notification_topics = channel
        .created_workspaces()
        .into_iter()
        .filter(|(_, title, _)| title == "agent notifications")
        .collect::<Vec<_>>();
    assert_eq!(notification_topics.len(), 1);
    assert_eq!(notification.topic, notification_topics[0].2);

    channel.push_event(ChannelEvent::Message(reply_to_topic_message(
        &notification.topic,
        "m-live-replay-notification-reply",
        &notification.id,
        cassette.reply_prompt.as_str(),
    )));
    wait_until(|| {
        provider
            .submit_calls()
            .contains(&(session_ref.clone(), cassette.reply_prompt.clone()))
    })
    .await;
    eventually_topic_message(&channel, &notification.topic, |message| {
        message.body.contains(reply_text.as_str()) && message.format == TextFormat::Markdown
    })
    .await;
    assert!(
        channel.sent_messages().iter().any(|sent| {
            sent.topic == notification.topic
                && sent.message.body.contains(reply_text.as_str())
                && sent
                    .message
                    .reply_to
                    .as_ref()
                    .is_some_and(|reply| reply.as_str() == "m-live-replay-notification-reply")
        }),
        "live replay notification reply turn should be anchored to the user instruction"
    );

    channel.close_events();
    timeout(Duration::from_secs(2), run)
        .await
        .expect("live replay notification run should stop after channel close")
        .expect("live replay notification task should join");
}

#[tokio::test(flavor = "current_thread")]
async fn live_record_watch_notification_replay_cassette_codex() {
    let Some(_guard) = live_watch_recording_provider("codex") else {
        return;
    };
    let _test_lock = test_lock();
    let temp = TempDir::new().expect("live recording temp dir");
    let project = temp.path().join("project");
    fs::create_dir_all(&project).expect("create live recording project");
    fs::write(
        project.join("README.md"),
        "lucarne telegram watch live recording\n",
    )
    .expect("write live recording project readme");

    let real_runtime = lucarne::agent_runtime::AgentRuntime::new();
    real_runtime.register_defaults();
    let real_provider = real_runtime
        .provider("codex")
        .expect("codex provider must be registered for live recording");
    let provider = Arc::new(LiveRecordingProvider::new("codex", real_provider));
    let core = core_with_live_recording_provider(Arc::clone(&provider));
    let workspace_id = ControlWorkspaceId::new("workspace-live-record");
    core.open_workspace_binding_with_events(
        workspace_id.clone(),
        OpenWorkspaceRequest {
            provider_id: "codex",
            project_path: Some(project.clone()),
            title: "codex live watch recording".into(),
        },
    )
    .await
    .expect("open real provider workspace for live recording");
    core.upsert_channel_binding(ChannelBinding::new(
        ChannelBindingId::new("telegram:100:9"),
        workspace_id.clone(),
        "telegram",
        "100",
        Some("9"),
    ))
    .expect("persist live recording topic binding");

    let channel = Arc::new(RecordingChannel::streaming());
    let bot = bot_with_state(Arc::clone(&channel), Arc::clone(&core));
    let run = tokio::spawn(Arc::clone(&bot).run());
    wait_for_channel_subscription(&channel).await;

    let background_prompt = "Reply with exactly: LIVE_LUCARNE_WATCH_BACKGROUND_RECORDING";
    timeout(
        live_watch_recording_timeout(),
        core.submit_turn(SubmitTurnRequest {
            workspace_id: workspace_id.clone(),
            source: TurnSource::UserMessage,
            input: AgentInput {
                text: background_prompt.into(),
                images: Vec::new(),
            },
            reply_to_channel_message_id: None,
        }),
    )
    .await
    .expect("live background submit timed out")
    .expect("live background submit failed");

    let notification = eventually_topic_sent_message_with_timeout(
        &channel,
        live_watch_recording_timeout(),
        |sent| {
            sent.message
                .body
                .contains("LIVE_LUCARNE_WATCH_BACKGROUND_RECORDING")
                && sent
                    .message
                    .body
                    .contains(compact_cwd_footer(&project).as_str())
                && sent.topic != "9"
        },
    )
    .await;
    provider.wait_for_submit_completed(1).await;

    let reply_prompt = "Reply with exactly: LIVE_LUCARNE_WATCH_REPLY_RECORDING";
    channel.push_event(ChannelEvent::Message(reply_to_topic_message(
        &notification.topic,
        "m-live-record-notification-reply",
        &notification.id,
        reply_prompt,
    )));
    eventually_topic_message_with_timeout(
        &channel,
        &notification.topic,
        live_watch_recording_timeout(),
        |message| message.body.contains("LIVE_LUCARNE_WATCH_REPLY_RECORDING"),
    )
    .await;
    provider.wait_for_submit_completed(2).await;
    provider.write_watch_notification_cassette(
        watch_notification_live_replay_cassette_path().as_path(),
    );

    channel.close_events();
    timeout(Duration::from_secs(2), run)
        .await
        .expect("live recording run should stop after channel close")
        .expect("live recording task should join");
}

#[tokio::test(flavor = "current_thread")]
async fn config_notifications_respect_global_workspace_and_session_toggles() {
    let _test_lock = test_lock();
    let provider = Arc::new(ProviderProbe::default());
    let core = core_with_provider(Arc::clone(&provider));
    bind_workspace(&core, "workspace-a", "9", "thread-1");
    core.resume_workspace_with_events(ResumeWorkspaceRequest {
        workspace_id: ControlWorkspaceId::new("workspace-a"),
        force_bypass_permissions: false,
    })
    .await
    .expect("resume workspace for watched events");

    let channel = Arc::new(RecordingChannel::streaming());
    let bot = bot_with_state(Arc::clone(&channel), Arc::clone(&core));
    let run = tokio::spawn(Arc::clone(&bot).run());
    wait_for_channel_subscription(&channel).await;

    channel.push_event(ChannelEvent::Message(entry_message(
        "m-global-off",
        "/config global notifications off",
    )));
    eventually_topic_message(&channel, "", |message| {
        message.body.contains("scope: `global`") && message.body.contains("notifications: `off`")
    })
    .await;
    provider
        .emit_to_session("thread-1", assistant_message("muted globally"))
        .await;
    sleep(Duration::from_millis(50)).await;
    assert!(
        channel
            .sent_messages()
            .iter()
            .all(|sent| !sent.message.body.contains("muted globally")),
        "global off must suppress watched agent messages"
    );

    channel.push_event(ChannelEvent::Message(entry_message(
        "m-global-on",
        "/config global notifications on",
    )));
    eventually_topic_message(&channel, "", |message| {
        message.body.contains("scope: `global`") && message.body.contains("notifications: `on`")
    })
    .await;
    channel.push_event(ChannelEvent::Message(topic_message(
        "9",
        "m-workspace-off",
        "/config workspace notifications off",
    )));
    eventually_topic_message(&channel, "9", |message| {
        message.body.contains("scope: `workspace`") && message.body.contains("notifications: `off`")
    })
    .await;
    provider
        .emit_to_session("thread-1", assistant_message("muted workspace"))
        .await;
    sleep(Duration::from_millis(50)).await;
    assert!(
        channel
            .sent_messages()
            .iter()
            .all(|sent| !sent.message.body.contains("muted workspace")),
        "workspace off must suppress watched agent messages"
    );

    channel.push_event(ChannelEvent::Message(topic_message(
        "9",
        "m-session-on",
        "/config session notifications on",
    )));
    eventually_topic_message(&channel, "9", |message| {
        message.body.contains("scope: `session`") && message.body.contains("notifications: `on`")
    })
    .await;
    provider
        .emit_to_session(
            "thread-1",
            assistant_message("visible after session toggle"),
        )
        .await;
    wait_until(|| {
        channel
            .sent_messages()
            .iter()
            .any(|sent| sent.message.body.contains("visible after session toggle"))
    })
    .await;

    let provider_session_id = ProviderSessionId::new("codex:thread-1");
    assert!(
        core.effective_settings(
            Some(Path::new("/tmp/workspace-a")),
            Some(&provider_session_id)
        )
        .notifications
        .enabled
    );

    channel.close_events();
    timeout(Duration::from_secs(2), run)
        .await
        .expect("config notification toggle run should stop after channel close")
        .expect("config notification toggle task should join");
}

#[tokio::test(flavor = "current_thread")]
async fn watch_does_not_duplicate_notifications_for_active_bot_conversation() {
    let _test_lock = test_lock();
    let provider = Arc::new(ProviderProbe::with_pending_submit_events(vec![
        assistant_message("normal topic answer"),
        turn_completed("normal-turn"),
    ]));
    let core = core_with_provider(Arc::clone(&provider));
    bind_workspace(&core, "workspace-a", "9", "thread-1");

    let channel = Arc::new(RecordingChannel::streaming());
    let bot = bot_with_state(Arc::clone(&channel), Arc::clone(&core));
    let run = tokio::spawn(Arc::clone(&bot).run());
    wait_for_channel_subscription(&channel).await;

    channel.push_event(ChannelEvent::Message(topic_message(
        "9",
        "m-normal-turn",
        "normal bot turn",
    )));
    eventually_topic_message(&channel, "9", |message| {
        message.body.contains("normal topic answer")
            && message
                .reply_to
                .as_ref()
                .is_some_and(|reply| reply.as_str() == "m-normal-turn")
    })
    .await;
    sleep(Duration::from_millis(50)).await;
    assert!(
        channel
            .created_workspaces()
            .iter()
            .all(|(_, title, _)| title != "agent notifications"),
        "normal topic conversations should not create notification topics"
    );
    let mut answer_message_ids = active_topic_messages(&channel, "9")
        .iter()
        .filter(|sent| sent.message.body.contains("normal topic answer"))
        .map(|sent| sent.id.clone())
        .collect::<Vec<_>>();
    answer_message_ids.sort();
    answer_message_ids.dedup();
    assert_eq!(
        answer_message_ids.len(),
        1,
        "normal topic answer should only use one visible message in the active topic"
    );

    channel.close_events();
    timeout(Duration::from_secs(2), run)
        .await
        .expect("normal suppression run should stop after channel close")
        .expect("normal suppression task should join");
}

#[tokio::test(flavor = "current_thread")]
async fn live_turn_history_echo_does_not_reenter_telegram_notifications() {
    let _test_lock = test_lock();
    let temp = TempDir::new().expect("live echo temp dir");
    let root = temp.path().join("codex-home").join("sessions");
    let project = PathBuf::from("/tmp/workspace-a");
    let session_path =
        write_live_watch_codex_session(&root, "thread-1", &project, "active telegram turn");
    let provider = Arc::new(ProviderProbe::default());
    let core = core_with_provider(Arc::clone(&provider));
    bind_workspace(&core, "workspace-a", "9", "thread-1");

    let channel = Arc::new(RecordingChannel::streaming());
    let bot = bot_with_state(Arc::clone(&channel), Arc::clone(&core));
    let run = tokio::spawn(Arc::clone(&bot).run());
    wait_for_channel_subscription(&channel).await;
    sleep(Duration::from_millis(50)).await;

    let codex = agent_provider("codex").expect("codex provider descriptor");
    core.start_history_session_watch_with_config(
        WatchConfig::new()
            .providers([codex])
            .provider_roots(codex, [root.clone()])
            .selection(ParseSelection::empty().with_meta().with_messages())
            .debounce(Duration::from_millis(25)),
    )
    .expect("start live history watch");

    channel.push_event(ChannelEvent::Message(topic_message(
        "9",
        "m-active-turn",
        "active telegram turn",
    )));
    eventually_topic_message(&channel, "9", |message| {
        message.body.contains("ok")
            && message
                .reply_to
                .as_ref()
                .is_some_and(|reply| reply.as_str() == "m-active-turn")
    })
    .await;

    append_codex_task_complete(
        &session_path,
        "2026-05-06T00:00:02.000Z",
        "turn-test",
        "persisted telegram echo",
    );
    assert!(
        !eventually_for(Duration::from_millis(250), || {
            channel
                .sent_messages()
                .iter()
                .chain(channel.edited_messages().iter())
                .any(|sent| sent.message.body.contains("persisted telegram echo"))
        })
        .await,
        "live turn history echo must not be delivered as a Telegram message"
    );

    append_codex_task_complete(
        &session_path,
        "2026-05-06T00:00:03.000Z",
        "external-turn",
        "external telegram history update",
    );
    eventually_topic_sent_message(&channel, |sent| {
        sent.message
            .body
            .contains("external telegram history update")
            && sent.topic == "9"
    })
    .await;
    assert!(
        channel
            .created_workspaces()
            .iter()
            .filter(|(_, title, _)| title == "agent notifications")
            .count()
            == 0,
        "an open Telegram workspace should receive visible history updates in its topic"
    );

    channel.close_events();
    timeout(Duration::from_secs(2), run)
        .await
        .expect("live echo run should stop after channel close")
        .expect("live echo task should join");
}

#[tokio::test(flavor = "current_thread")]
async fn watch_suppression_is_scoped_to_the_active_workspace() {
    let _test_lock = test_lock();
    let provider = Arc::new(ProviderProbe::with_pending_submit_events(vec![
        assistant_message("active workspace answer"),
        turn_completed("active-turn"),
    ]));
    let core = core_with_provider(Arc::clone(&provider));
    bind_workspace(&core, "workspace-a", "9", "thread-1");
    bind_workspace(&core, "workspace-b", "10", "thread-2");
    core.resume_workspace_with_events(ResumeWorkspaceRequest {
        workspace_id: ControlWorkspaceId::new("workspace-b"),
        force_bypass_permissions: false,
    })
    .await
    .expect("resume background workspace for watched events");

    let channel = Arc::new(RecordingChannel::streaming());
    let bot = bot_with_state(Arc::clone(&channel), Arc::clone(&core));
    let run = tokio::spawn(Arc::clone(&bot).run());
    wait_for_channel_subscription(&channel).await;

    channel.push_event(ChannelEvent::Message(topic_message(
        "9",
        "m-active-turn",
        "active bot turn",
    )));
    eventually_topic_message(&channel, "9", |message| {
        message.body.contains("active workspace answer")
    })
    .await;
    provider
        .emit_to_session(
            "thread-2",
            assistant_message("background workspace finished"),
        )
        .await;
    wait_until(|| {
        channel.sent_messages().iter().any(|sent| {
            sent.message.body.contains("background workspace finished")
                && sent.message.body.contains("session: `thread-2`")
                && sent.message.body.contains("cwd: `/tmp/workspace-b`")
        })
    })
    .await;
    let notification_topics = channel
        .created_workspaces()
        .into_iter()
        .filter(|(_, title, _)| title == "agent notifications")
        .map(|(_, _, topic)| topic)
        .collect::<Vec<_>>();
    assert_eq!(notification_topics.len(), 1);
    assert!(
        topic_messages(&channel, &notification_topics[0])
            .iter()
            .all(|sent| !sent.message.body.contains("active workspace answer")),
        "active workspace output must not leak into watch notifications"
    );

    channel.close_events();
    timeout(Duration::from_secs(2), run)
        .await
        .expect("workspace scoped suppression run should stop after channel close")
        .expect("workspace scoped suppression task should join");
}

#[tokio::test(flavor = "current_thread")]
async fn open_telegram_session_keeps_later_watched_output_out_of_notifications() {
    let _test_lock = test_lock();
    let provider = Arc::new(ProviderProbe::with_pending_submit_events(vec![
        assistant_message("active session first answer"),
        turn_completed("active-session-first-turn"),
    ]));
    let core = core_with_provider(Arc::clone(&provider));
    bind_workspace(&core, "workspace-a", "9", "thread-1");

    let channel = Arc::new(RecordingChannel::streaming());
    let state = BotState::new_with_core(Arc::clone(&core));
    let bot = bot_with_existing_state(Arc::clone(&channel), Arc::clone(&core), Arc::clone(&state));
    let run = tokio::spawn(Arc::clone(&bot).run());
    wait_for_channel_subscription(&channel).await;

    channel.push_event(ChannelEvent::Message(topic_message(
        "9",
        "m-open-active-session",
        "open active session",
    )));
    eventually_topic_message(&channel, "9", |message| {
        message.body.contains("active session first answer")
    })
    .await;
    assert!(
        state
            .get(&WorkspaceId::new("workspace-a"))
            .is_some_and(|session| session.live.is_some()),
        "the workspace should stay bound to a live session until the user closes it"
    );

    provider
        .emit_to_session(
            "thread-1",
            assistant_message("late active session watched output"),
        )
        .await;
    eventually_topic_message(&channel, "9", |message| {
        message.body.contains("late active session watched output")
    })
    .await;
    sleep(Duration::from_millis(50)).await;
    assert!(
        channel
            .created_workspaces()
            .iter()
            .all(|(_, title, _)| title != "agent notifications"),
        "a still-open Telegram session should receive watched output in its topic, not the notification hub"
    );

    channel.close_events();
    timeout(Duration::from_secs(2), run)
        .await
        .expect("open active session run should stop after channel close")
        .expect("open active session task should join");
}

#[tokio::test(flavor = "current_thread")]
async fn watched_output_recreates_deleted_live_session_topic() {
    let _test_lock = test_lock();
    let provider = Arc::new(ProviderProbe::with_pending_submit_events(vec![
        assistant_message("active session before delete"),
        turn_completed("active-session-before-delete"),
    ]));
    let core = core_with_provider(Arc::clone(&provider));
    bind_workspace(&core, "workspace-a", "9", "thread-1");

    let channel = Arc::new(RecordingChannel::streaming_with_create_workspace_delay(
        Duration::ZERO,
    ));
    channel.next_workspace_id.store(10, Ordering::SeqCst);
    let state = BotState::new_with_core(Arc::clone(&core));
    let bot = bot_with_existing_state(Arc::clone(&channel), Arc::clone(&core), Arc::clone(&state));
    let run = tokio::spawn(Arc::clone(&bot).run());
    wait_for_channel_subscription(&channel).await;

    channel.push_event(ChannelEvent::Message(topic_message(
        "9",
        "m-open-before-delete",
        "open before delete",
    )));
    eventually_topic_message(&channel, "9", |message| {
        message.body.contains("active session before delete")
    })
    .await;
    assert!(
        state
            .get(&WorkspaceId::new("workspace-a"))
            .is_some_and(|session| session.live.is_some()),
        "workspace should remain live before topic deletion"
    );

    channel.mark_workspace_missing("9");
    provider
        .emit_to_session("thread-1", assistant_message("watched output after delete"))
        .await;

    eventually_topic_message(&channel, "10", |message| {
        message.body.contains("watched output after delete")
    })
    .await;
    assert_eq!(
        channel.created_workspaces(),
        vec![("100".into(), "codex workspace-a".into(), "10".into())],
        "watched output should recreate the deleted live session topic before sending"
    );
    assert_eq!(
        state
            .topic_for_workspace(&WorkspaceId::new("workspace-a"))
            .as_ref()
            .map(|topic| topic.as_str()),
        Some("10"),
        "replacement topic must be rebound for later messages"
    );
    assert!(
        topic_messages(&channel, "9")
            .iter()
            .all(|sent| !sent.message.body.contains("watched output after delete")),
        "watched output must not be sent to the deleted topic"
    );

    channel.close_events();
    timeout(Duration::from_secs(2), run)
        .await
        .expect("deleted-topic watched output run should stop after channel close")
        .expect("deleted-topic watched output task should join");
}

#[tokio::test(flavor = "current_thread")]
async fn new_and_quit_topic_commands_run_through_public_bot_flow() {
    let _test_lock = test_lock();
    let _env = IsolatedHistoryEnv::new();
    let provider = Arc::new(ProviderProbe::default());
    let core = core_with_provider(Arc::clone(&provider));
    bind_workspace(&core, "workspace-a", "9", "thread-1");
    let state = BotState::new_with_core(Arc::clone(&core));
    let events = vec![
        ChannelEvent::Message(topic_message("9", "m-new", "/new")),
        ChannelEvent::Message(topic_message("9", "m-quit", "/quit")),
        ChannelEvent::Message(topic_message(
            "9",
            "m-after-quit",
            "continue after lifecycle",
        )),
    ];
    let channel = Arc::new(RecordingChannel::with_events_and_gap(
        events,
        Duration::from_millis(150),
    ));
    let bot = bot_with_existing_state(Arc::clone(&channel), Arc::clone(&core), state);

    timeout(Duration::from_secs(4), bot.run())
        .await
        .expect("lifecycle command journey should finish after finite event stream");

    wait_until(|| provider.lifecycle_calls().len() == 2).await;
    assert_eq!(
        provider.lifecycle_calls(),
        vec![
            ("thread-1".to_string(), "new".to_string()),
            ("session-test".to_string(), "quit".to_string())
        ],
        "/new and /quit must execute on the topic-bound live session, not an unrelated session"
    );
    assert_eq!(
        provider.resumes(),
        vec!["thread-1".to_string()],
        "/new should resume the original bound session before invoking the typed lifecycle command"
    );
    assert_eq!(
        provider.opens(),
        1,
        "/quit after a fixture /new with no replacement provider session id opens one fresh live session"
    );
    wait_until(|| {
        provider
            .inputs()
            .iter()
            .any(|input| input.text.as_str() == "continue after lifecycle")
    })
    .await;
    assert_eq!(
        provider.submit_calls(),
        vec![(
            "session-test".to_string(),
            "continue after lifecycle".to_string()
        )],
        "the follow-up turn after /quit must run against the current provider session"
    );
    assert!(
        topic_messages(&channel, "9")
            .iter()
            .all(|sent| !sent.message.body.contains("Unsupported command")),
        "/new and /quit must stay on the typed session-trait path"
    );
}

#[tokio::test(flavor = "current_thread")]
async fn model_alias_and_text_selection_use_topic_bound_session_trait() {
    let _test_lock = test_lock();
    let _env = IsolatedHistoryEnv::new();
    let provider = Arc::new(ProviderProbe::with_models(AgentModelCatalog {
        current_model: Some("gpt-5.5".into()),
        current_reasoning: Some("medium".into()),
        models: vec![
            AgentModelOption {
                id: "gpt-5.5".into(),
                display_name: Some("GPT-5.5".into()),
                description: Some("Frontier model".into()),
                supported_reasoning: vec![
                    AgentReasoningOption {
                        value: "medium".into(),
                        description: None,
                        is_default: Some(true),
                    },
                    AgentReasoningOption {
                        value: "high".into(),
                        description: None,
                        is_default: None,
                    },
                ],
            },
            AgentModelOption {
                id: "gpt-5.4".into(),
                display_name: None,
                description: None,
                supported_reasoning: Vec::new(),
            },
        ],
    }));
    let core = core_with_provider(Arc::clone(&provider));
    bind_workspace(&core, "workspace-a", "9", "thread-1");
    let state = BotState::new_with_core(Arc::clone(&core));
    let events = vec![
        ChannelEvent::Message(topic_message("9", "m-model-list", "/models")),
        ChannelEvent::Message(topic_message("9", "m-model-set", "/model gpt-5.4 high")),
        ChannelEvent::Message(topic_message("9", "m-model-status", "/status")),
    ];
    let channel = Arc::new(RecordingChannel::with_events_and_gap(
        events,
        Duration::from_millis(150),
    ));
    let bot = bot_with_existing_state(Arc::clone(&channel), Arc::clone(&core), state);

    timeout(Duration::from_secs(4), bot.run())
        .await
        .expect("model journey should finish after finite event stream");

    let models = eventually_topic_message(&channel, "9", |message| {
        message.body.contains("models") && message.body.contains("`gpt-5.5`")
    })
    .await;
    assert_eq!(models.format, TextFormat::Markdown);
    assert!(models.buttons.is_empty());
    assert!(models.body.contains("set: `/model <model> [reasoning]`"));
    assert!(models.body.contains("reasoning levels: `medium`, `high`"));
    wait_until(|| provider.model_selections().len() == 1).await;
    assert_eq!(
        provider.model_selections(),
        vec![AgentModelSelection {
            model: "gpt-5.4".into(),
            reasoning: Some("high".into()),
        }]
    );
    let status = eventually_topic_message(&channel, "9", |message| {
        message.body.contains("status") && message.body.contains("gpt-5.4")
    })
    .await;
    assert!(status.body.contains("reasoning high"));
    assert!(
        topic_messages(&channel, "9")
            .iter()
            .all(|sent| !sent.message.body.contains("Unsupported command /models")),
        "/models must stay on the typed model path"
    );
}

#[tokio::test(flavor = "current_thread")]
async fn permissions_command_lists_text_modes_and_updates_status_from_text_mode() {
    let _test_lock = test_lock();
    let _env = IsolatedHistoryEnv::new();
    let provider = Arc::new(ProviderProbe::with_permissions(AgentPermissionCatalog {
        current_mode: Some("default".into()),
        modes: vec![
            AgentPermissionOption {
                id: "default".into(),
                display_name: None,
                description: None,
            },
            AgentPermissionOption {
                id: "on-request".into(),
                display_name: None,
                description: None,
            },
        ],
    }));
    let core = core_with_provider(Arc::clone(&provider));
    bind_workspace(&core, "workspace-a", "9", "thread-1");
    let state = BotState::new_with_core(Arc::clone(&core));
    let list_channel = Arc::new(RecordingChannel::with_events(vec![ChannelEvent::Message(
        topic_message("9", "m-permissions-list", "/permissions"),
    )]));
    let list_bot = bot_with_existing_state(
        Arc::clone(&list_channel),
        Arc::clone(&core),
        Arc::clone(&state),
    );

    timeout(Duration::from_secs(2), list_bot.run())
        .await
        .expect("permissions list should finish after finite event stream");

    let permissions = eventually_topic_message(&list_channel, "9", |message| {
        message.body.contains("permission modes") && message.body.contains("`on-request`")
    })
    .await;
    assert_eq!(permissions.format, TextFormat::Markdown);
    assert!(permissions.buttons.is_empty());
    assert!(permissions.body.contains("1. `default`"));
    assert!(permissions.body.contains("\n\n2. `on-request`"));

    let set_and_status_channel = Arc::new(RecordingChannel::with_events_and_gap(
        vec![
            ChannelEvent::Message(topic_message(
                "9",
                "m-permissions-set",
                "/permissions on-request",
            )),
            ChannelEvent::Message(topic_message("9", "m-permissions-status", "/status")),
        ],
        Duration::from_millis(150),
    ));
    let set_bot = bot_with_existing_state(set_and_status_channel.clone(), core, state);
    timeout(Duration::from_secs(4), set_bot.run())
        .await
        .expect("permissions text command should finish after finite event stream");

    wait_until(|| provider.permission_selections().len() == 1).await;
    assert_eq!(
        provider.permission_selections(),
        vec![AgentPermissionSelection {
            mode: "on-request".into(),
        }]
    );
    eventually_topic_message(&set_and_status_channel, "9", |message| {
        message.body.contains("status") && message.body.contains("on-request")
    })
    .await;
}

#[tokio::test(flavor = "current_thread")]
async fn same_topic_id_in_different_chats_accepts_text_permissions_commands() {
    let _test_lock = test_lock();
    let _env = IsolatedHistoryEnv::new();
    let provider = Arc::new(ProviderProbe::with_permissions(AgentPermissionCatalog {
        current_mode: Some("default".into()),
        modes: vec![
            AgentPermissionOption {
                id: "default".into(),
                display_name: None,
                description: None,
            },
            AgentPermissionOption {
                id: "on-request".into(),
                display_name: None,
                description: None,
            },
        ],
    }));
    let core = core_with_provider(Arc::clone(&provider));
    bind_workspace_in_chat(&core, "workspace-a", "100", "9", "thread-a");
    bind_workspace_in_chat(&core, "workspace-b", "200", "9", "thread-b");
    let state = BotState::new_with_core(Arc::clone(&core));

    let list_channel = Arc::new(RecordingChannel::with_events_and_gap(
        vec![
            ChannelEvent::Message(topic_message_in_chat(
                "100",
                "9",
                "m-permissions-a",
                "/permissions",
            )),
            ChannelEvent::Message(topic_message_in_chat(
                "200",
                "9",
                "m-permissions-b",
                "/permissions",
            )),
        ],
        Duration::from_millis(150),
    ));
    let list_bot = bot_with_existing_state(
        Arc::clone(&list_channel),
        Arc::clone(&core),
        Arc::clone(&state),
    );
    timeout(Duration::from_secs(4), list_bot.run())
        .await
        .expect("multi-chat permissions list should finish after finite event stream");

    let permissions_a = eventually_topic_message_in_chat(&list_channel, "100", "9", |message| {
        message.body.contains("permission modes") && message.body.contains("`on-request`")
    })
    .await;
    let permissions_b = eventually_topic_message_in_chat(&list_channel, "200", "9", |message| {
        message.body.contains("permission modes") && message.body.contains("`on-request`")
    })
    .await;
    assert!(permissions_a.buttons.is_empty());
    assert!(permissions_b.buttons.is_empty());

    let set_channel = Arc::new(RecordingChannel::with_events_and_gap(
        vec![
            ChannelEvent::Message(topic_message_in_chat(
                "100",
                "9",
                "m-permissions-set-a",
                "/permissions on-request",
            )),
            ChannelEvent::Message(topic_message_in_chat(
                "200",
                "9",
                "m-permissions-set-b",
                "/permissions on-request",
            )),
        ],
        Duration::from_millis(150),
    ));
    let set_bot = bot_with_existing_state(set_channel, core, state);
    timeout(Duration::from_secs(4), set_bot.run())
        .await
        .expect("multi-chat permissions text commands should finish after finite event stream");

    wait_until(|| provider.permission_selections().len() == 2).await;
    assert_eq!(
        provider.permission_selections(),
        vec![
            AgentPermissionSelection {
                mode: "on-request".into(),
            },
            AgentPermissionSelection {
                mode: "on-request".into(),
            }
        ]
    );
}

#[tokio::test(flavor = "current_thread")]
async fn same_topic_id_in_different_chats_keeps_fork_shortcuts_chat_scoped() {
    let _test_lock = test_lock();
    let _env = IsolatedHistoryEnv::new();
    let provider = Arc::new(ProviderProbe::with_fork_targets(vec![AgentForkTarget {
        id: "target-a".into(),
        label: Some("Rollback point".into()),
        description: Some("turn 1".into()),
    }]));
    let core = core_with_provider(Arc::clone(&provider));
    bind_workspace_in_chat(&core, "workspace-a", "100", "9", "thread-a");
    bind_workspace_in_chat(&core, "workspace-b", "200", "9", "thread-b");
    let state = BotState::new_with_core(Arc::clone(&core));

    let list_channel = Arc::new(RecordingChannel::with_events_and_gap(
        vec![
            ChannelEvent::Message(topic_message_in_chat("100", "9", "m-fork-a", "/fork")),
            ChannelEvent::Message(topic_message_in_chat("200", "9", "m-fork-b", "/fork")),
        ],
        Duration::from_millis(150),
    ));
    let list_bot = bot_with_existing_state(
        Arc::clone(&list_channel),
        Arc::clone(&core),
        Arc::clone(&state),
    );
    timeout(Duration::from_secs(4), list_bot.run())
        .await
        .expect("multi-chat fork list should finish after finite event stream");

    let fork_a = eventually_topic_message_in_chat(&list_channel, "100", "9", |message| {
        message.body.contains("fork targets") && message.body.contains("/f1")
    })
    .await;
    let fork_b = eventually_topic_message_in_chat(&list_channel, "200", "9", |message| {
        message.body.contains("fork targets") && message.body.contains("/f1")
    })
    .await;
    assert!(fork_a.buttons.is_empty());
    assert!(fork_b.buttons.is_empty());

    let select_channel = Arc::new(RecordingChannel::with_events_and_gap(
        vec![
            ChannelEvent::Message(topic_message_in_chat("200", "9", "m-select-b", "/f1")),
            ChannelEvent::Message(topic_message_in_chat("100", "9", "m-select-a", "/f1")),
        ],
        Duration::from_millis(150),
    ));
    let select_bot = bot_with_existing_state(select_channel, core, state);
    timeout(Duration::from_secs(4), select_bot.run())
        .await
        .expect("multi-chat fork selections should finish after finite event stream");

    wait_until(|| provider.fork_calls().len() == 2).await;
    assert_eq!(
        provider.fork_calls(),
        vec![
            ("thread-b".to_string(), "target-a".to_string()),
            ("thread-a".to_string(), "target-a".to_string())
        ],
        "/fN must resolve through the current chat+topic session, not through a topic-only shortcut"
    );
}

#[tokio::test(flavor = "current_thread")]
async fn topic_image_caption_downloads_attachment_and_submits_multimodal_input() {
    let _test_lock = test_lock();
    let provider = Arc::new(ProviderProbe::default());
    let core = core_with_provider(Arc::clone(&provider));
    bind_workspace(&core, "workspace-a", "9", "thread-1");
    let channel = Arc::new(RecordingChannel::with_events(vec![ChannelEvent::Message(
        IncomingMessage {
            message_id: MessageId::new("m-image"),
            chat: ChatId::new("100"),
            workspace: Some(WorkspaceId::new("9")),
            reply_to: None,
            user: "alice".into(),
            text: Some("inspect this".into()),
            attachments: vec![image_attachment("photo-ref")],
        },
    )]));
    channel
        .downloads
        .lock()
        .unwrap()
        .insert("photo-ref".into(), vec![1, 2, 3]);
    let bot = bot_with_state(Arc::clone(&channel), Arc::clone(&core));

    timeout(Duration::from_secs(2), bot.run())
        .await
        .expect("image caption journey should finish after finite event stream");

    wait_until(|| provider.inputs().len() == 1).await;
    let inputs = provider.inputs();
    let input = &inputs[0];
    assert_eq!(input.text.as_str(), "inspect this");
    assert_eq!(input.images.len(), 1);
    assert_eq!(input.images[0].media_type.as_str(), "image/jpeg");
    assert_eq!(input.images[0].data_base64.as_str(), "AQID");
    assert_eq!(channel.acknowledged_topics(), vec!["9".to_string()]);
}

#[tokio::test(flavor = "current_thread")]
async fn journey_51_turn_failure_visible_failure_stops_typing_and_detaches_live() {
    let _test_lock = test_lock();
    let provider = Arc::new(ProviderProbe::with_pending_submit_events(vec![
        Event::TurnFailed(lucarne::agent_runtime::events::TurnFailedEvent {
            turn_id: "turn-failed".into(),
            error: "provider stopped responding".into(),
            code: "timeout".into(),
        }),
    ]));
    let core = core_with_provider(Arc::clone(&provider));
    bind_workspace(&core, "workspace-a", "9", "thread-1");
    let channel = Arc::new(RecordingChannel::with_events(vec![ChannelEvent::Message(
        topic_message("9", "m-fail", "fail please"),
    )]));
    let bot = bot_with_state(Arc::clone(&channel), Arc::clone(&core));
    timeout(Duration::from_secs(2), bot.run())
        .await
        .expect("turn failure run should finish after finite event stream");

    wait_until(|| provider.submit_calls() == vec![("thread-1".into(), "fail please".into())]).await;
    wait_until(|| {
        channel.edited_messages().iter().any(|message| {
            message.topic == "9"
                && message.message.body.contains("⚠ 失败")
                && message.message.body.contains("provider stopped responding")
        })
    })
    .await;
    assert!(
        channel
            .edited_messages()
            .iter()
            .all(|message| !message.message.body.contains("✓ 完成")),
        "failed turn must not leave success status behind"
    );
    assert!(
        !core.has_live_session(&ControlWorkspaceId::new("workspace-a")),
        "provider TurnFailed must detach the stale live session"
    );
}

#[tokio::test(flavor = "current_thread")]
async fn topic_turn_queue_reports_position_and_runs_fifo() {
    let _test_lock = test_lock();
    let provider = Arc::new(ProviderProbe::with_pending_submit_events(Vec::new()));
    let core = core_with_provider(Arc::clone(&provider));
    bind_workspace(&core, "workspace-a", "9", "thread-1");
    let channel = Arc::new(RecordingChannel::streaming());
    let bot = bot_with_state(Arc::clone(&channel), Arc::clone(&core));

    let run = tokio::spawn(Arc::clone(&bot).run());
    channel.push_event(ChannelEvent::Message(topic_message(
        "9", "m-first", "first",
    )));
    wait_until(|| provider.submit_calls() == vec![("thread-1".into(), "first".into())]).await;
    channel.push_event(ChannelEvent::Message(topic_message(
        "9", "m-second", "second",
    )));

    let queued = eventually_topic_message(&channel, "9", |message| {
        message.body.contains("queued · position 1")
    })
    .await;
    assert_eq!(queued.format, TextFormat::Plain);
    assert_eq!(
        queued.reply_to.as_ref().map(|id| id.as_str()),
        Some("m-second")
    );
    assert_eq!(
        provider.submit_calls(),
        vec![("thread-1".into(), "first".into())],
        "queued turn must not submit until the active turn completes"
    );

    provider
        .emit_to_session(
            "thread-1",
            Event::Message(MessageEvent {
                role: MessageRole::Assistant,
                text: "first done".into(),
                streaming: false,
            }),
        )
        .await;
    provider
        .emit_to_session(
            "thread-1",
            Event::TurnCompleted(lucarne::agent_runtime::events::TurnCompletedEvent {
                turn_id: "turn-first".into(),
                usage: None,
            }),
        )
        .await;
    wait_until(|| {
        provider.submit_calls()
            == vec![
                ("thread-1".into(), "first".into()),
                ("thread-1".into(), "second".into()),
            ]
    })
    .await;
    provider
        .emit_to_session(
            "thread-1",
            Event::Message(MessageEvent {
                role: MessageRole::Assistant,
                text: "second done".into(),
                streaming: false,
            }),
        )
        .await;
    provider
        .emit_to_session(
            "thread-1",
            Event::TurnCompleted(lucarne::agent_runtime::events::TurnCompletedEvent {
                turn_id: "turn-second".into(),
                usage: None,
            }),
        )
        .await;

    eventually_topic_message(&channel, "9", |message| message.body.contains("first done")).await;
    eventually_topic_message(&channel, "9", |message| {
        message.body.contains("second done")
    })
    .await;
    channel.close_events();
    timeout(Duration::from_secs(2), run)
        .await
        .expect("turn queue run should stop")
        .expect("turn queue task should not panic");
}

#[tokio::test(flavor = "current_thread")]
async fn topic_interrupt_bypasses_turn_queue_and_calls_live_interrupt() {
    let _test_lock = test_lock();
    let provider = Arc::new(ProviderProbe::with_pending_submit_events(Vec::new()));
    let core = core_with_provider(Arc::clone(&provider));
    bind_workspace(&core, "workspace-a", "9", "thread-1");
    let channel = Arc::new(RecordingChannel::streaming());
    let bot = bot_with_state(Arc::clone(&channel), Arc::clone(&core));

    let run = tokio::spawn(Arc::clone(&bot).run());
    channel.push_event(ChannelEvent::Message(topic_message(
        "9",
        "m-first",
        "long turn",
    )));
    wait_until(|| provider.submit_calls() == vec![("thread-1".into(), "long turn".into())]).await;
    channel.push_event(ChannelEvent::Message(topic_message(
        "9",
        "m-interrupt",
        "/interrupt",
    )));

    wait_until(|| provider.lifecycle_calls() == vec![("thread-1".into(), "interrupt".into())])
        .await;
    let ack = eventually_topic_message(&channel, "9", |message| {
        message.body.contains("interrupted current turn")
    })
    .await;
    assert_eq!(ack.format, TextFormat::Plain);
    assert_eq!(
        provider.submit_calls(),
        vec![("thread-1".into(), "long turn".into())],
        "/interrupt must not be submitted as a provider prompt"
    );
    assert!(
        topic_messages(&channel, "9")
            .iter()
            .all(|message| !message.message.body.contains("queued · position")),
        "/interrupt must bypass the ordinary prompt queue"
    );

    provider
        .emit_to_session(
            "thread-1",
            Event::TurnCompleted(lucarne::agent_runtime::events::TurnCompletedEvent {
                turn_id: "turn-interrupted".into(),
                usage: None,
            }),
        )
        .await;
    channel.close_events();
    timeout(Duration::from_secs(2), run)
        .await
        .expect("interrupt run should stop")
        .expect("interrupt task should not panic");
}

#[tokio::test(flavor = "current_thread")]
async fn topic_text_attachment_without_caption_submits_file_context_as_turn() {
    let _test_lock = test_lock();
    let provider = Arc::new(ProviderProbe::default());
    let core = core_with_provider(Arc::clone(&provider));
    bind_workspace(&core, "workspace-a", "9", "thread-1");
    let channel = Arc::new(RecordingChannel::with_events(vec![ChannelEvent::Message(
        IncomingMessage {
            message_id: MessageId::new("m-text-attachment"),
            chat: ChatId::new("100"),
            workspace: Some(WorkspaceId::new("9")),
            reply_to: None,
            user: "alice".into(),
            text: None,
            attachments: vec![text_attachment(
                "source-ref",
                "main.rs",
                Some("application/octet-stream"),
            )],
        },
    )]));
    channel
        .downloads
        .lock()
        .unwrap()
        .insert("source-ref".into(), b"fn main() {}\n".to_vec());
    let bot = bot_with_state(Arc::clone(&channel), Arc::clone(&core));

    timeout(Duration::from_secs(2), bot.run())
        .await
        .expect("text attachment journey should finish after finite event stream");

    wait_until(|| provider.inputs().len() == 1).await;
    let inputs = provider.inputs();
    let input = &inputs[0];
    assert_eq!(
        input.text.as_str(),
        "[user attached file: main.rs]\n```\nfn main() {}\n```"
    );
    assert!(input.images.is_empty());
    assert_eq!(channel.acknowledged_topics(), vec!["9".to_string()]);
}

#[tokio::test(flavor = "current_thread")]
async fn topic_image_only_message_is_attached_to_next_text_through_public_bot_flow() {
    let _test_lock = test_lock();
    let _env = IsolatedHistoryEnv::new();
    let provider = Arc::new(ProviderProbe::default());
    let core = core_with_provider(Arc::clone(&provider));
    bind_workspace(&core, "workspace-a", "9", "thread-1");
    let channel = Arc::new(RecordingChannel::with_events_and_gap(
        vec![
            ChannelEvent::Message(IncomingMessage {
                message_id: MessageId::new("m-image-only"),
                chat: ChatId::new("100"),
                workspace: Some(WorkspaceId::new("9")),
                reply_to: None,
                user: "alice".into(),
                text: None,
                attachments: vec![image_attachment("photo-ref")],
            }),
            ChannelEvent::Message(topic_message(
                "9",
                "m-image-followup",
                "inspect previous image",
            )),
        ],
        Duration::from_millis(150),
    ));
    channel
        .downloads
        .lock()
        .unwrap()
        .insert("photo-ref".into(), vec![1, 2, 3]);
    let bot = bot_with_state(Arc::clone(&channel), Arc::clone(&core));

    timeout(Duration::from_secs(4), bot.run())
        .await
        .expect("image-only journey should finish after finite event stream");

    wait_until(|| provider.inputs().len() == 1).await;
    let inputs = provider.inputs();
    let input = &inputs[0];
    assert_eq!(input.text.as_str(), "inspect previous image");
    assert_eq!(input.images.len(), 1);
    assert_eq!(input.images[0].media_type.as_str(), "image/jpeg");
    assert_eq!(input.images[0].data_base64.as_str(), "AQID");
    assert_eq!(
        channel.acknowledged_topics(),
        vec!["9".to_string(), "9".to_string()]
    );
}

#[tokio::test(flavor = "current_thread")]
async fn image_only_pending_state_isolated_by_chat_scoped_topic_binding() {
    let _test_lock = test_lock();
    let _env = IsolatedHistoryEnv::new();
    let provider = Arc::new(ProviderProbe::default());
    let core = core_with_provider(Arc::clone(&provider));
    bind_workspace_in_chat(&core, "workspace-a", "100", "9", "thread-a");
    bind_workspace_in_chat(&core, "workspace-b", "200", "9", "thread-b");
    let channel = Arc::new(RecordingChannel::with_events_and_gap(
        vec![
            ChannelEvent::Message(IncomingMessage {
                message_id: MessageId::new("m-image-chat-a"),
                chat: ChatId::new("100"),
                workspace: Some(WorkspaceId::new("9")),
                reply_to: None,
                user: "alice".into(),
                text: None,
                attachments: vec![image_attachment("photo-ref")],
            }),
            ChannelEvent::Message(topic_message_in_chat(
                "200",
                "9",
                "m-text-chat-b",
                "chat b text",
            )),
            ChannelEvent::Message(topic_message_in_chat(
                "100",
                "9",
                "m-text-chat-a",
                "chat a text",
            )),
        ],
        Duration::from_millis(150),
    ));
    channel
        .downloads
        .lock()
        .unwrap()
        .insert("photo-ref".into(), vec![1, 2, 3]);
    let bot = bot_with_state(Arc::clone(&channel), Arc::clone(&core));

    timeout(Duration::from_secs(4), bot.run())
        .await
        .expect("multi-chat image-only journey should finish after finite event stream");

    wait_until(|| provider.inputs().len() == 2).await;
    let inputs = provider.inputs();
    let chat_b = inputs
        .iter()
        .find(|input| input.text.as_str() == "chat b text")
        .expect("chat b text input");
    assert!(
        chat_b.images.is_empty(),
        "image-only state from chat 100 topic 9 must not leak into chat 200 topic 9"
    );
    let chat_a = inputs
        .iter()
        .find(|input| input.text.as_str() == "chat a text")
        .expect("chat a text input");
    assert_eq!(chat_a.images.len(), 1);
    assert_eq!(chat_a.images[0].media_type.as_str(), "image/jpeg");
    assert_eq!(chat_a.images[0].data_base64.as_str(), "AQID");
    assert_eq!(
        channel.acknowledged_handles(),
        vec![
            ("100".to_string(), "9".to_string()),
            ("200".to_string(), "9".to_string()),
            ("100".to_string(), "9".to_string())
        ]
    );
}

#[tokio::test(flavor = "current_thread")]
async fn approval_button_resolves_pending_live_request_through_topic_callback() {
    let _test_lock = test_lock();
    let provider = Arc::new(ProviderProbe::with_pending_submit_events(vec![
        Event::InterventionRequest(lucarne::agent_runtime::InterventionRequest::Approval(
            ApprovalRequest {
                req_id: "approval-1".into(),
                tool_name: "edit".into(),
                message: Some("needs approval".into()),
                input: None,
            },
        )),
    ]));
    let core = core_with_provider(Arc::clone(&provider));
    core.set_force_bypass_permissions(true)
        .expect("enable forced bypass default");
    bind_workspace(&core, "workspace-a", "9", "thread-1");
    let state = BotState::new_with_core(Arc::clone(&core));
    let channel = Arc::new(RecordingChannel::streaming());
    let bot = bot_with_existing_state(Arc::clone(&channel), Arc::clone(&core), state);

    let run = tokio::spawn(Arc::clone(&bot).run());
    channel.push_event(ChannelEvent::Message(topic_message(
        "9",
        "m-approval",
        "please edit",
    )));
    let approve_data = eventually_button_data(
        &channel,
        "9",
        |message| message.body.contains("needs approval"),
        "intv:c:",
    )
    .await;
    assert!(approve_data.len() <= 64, "{approve_data}");
    assert_eq!(
        provider.resolved_interventions(),
        Vec::<(String, InterventionResponse)>::new(),
        "system forced bypass must not locally resolve agent permission requests"
    );

    channel.push_event(ChannelEvent::Button {
        chat: ChatId::new("100"),
        workspace: Some(WorkspaceId::new("9")),
        user: "alice".into(),
        data: approve_data,
        source_message: MessageId::new("approval-prompt"),
    });

    wait_until(|| provider.resolved_interventions().len() == 1).await;
    assert_eq!(
        provider.resolved_interventions(),
        vec![(
            "approval-1".to_string(),
            InterventionResponse::Approval(ApprovalDecision::Allow)
        )]
    );
    channel.close_events();
    timeout(Duration::from_secs(2), run)
        .await
        .expect("approval stream should stop")
        .expect("approval run task should not panic");
}

#[tokio::test(flavor = "current_thread")]
async fn subagent_open_button_creates_child_topic_and_resumes_child_session() {
    let _test_lock = test_lock();
    let provider = Arc::new(ProviderProbe::with_submit_events(vec![Event::ToolCall(
        ToolCallEvent {
            call_id: CallId("call-subagent".into()),
            name: "sub_agent".into(),
            input: json!({
                "tool_name": "Task",
                "prompt": "Inspect parser",
                "child_session_ref": "child-session-1",
                "child_thread_id": "child-thread-1",
                "agent_id": "agent-1",
                "nickname": "Parser",
                "role": "explorer",
                "status": "running"
            }),
        },
    )]));
    let core = core_with_provider(Arc::clone(&provider));
    bind_workspace(&core, "workspace-a", "9", "thread-1");
    let state = BotState::new_with_core(Arc::clone(&core));
    let channel = Arc::new(RecordingChannel::streaming());
    let bot = bot_with_existing_state(Arc::clone(&channel), Arc::clone(&core), state);

    let run = tokio::spawn(Arc::clone(&bot).run());
    channel.push_event(ChannelEvent::Message(topic_message(
        "9",
        "m-subagent",
        "spawn child",
    )));
    let subagent_data = eventually_button_data(
        &channel,
        "9",
        |message| {
            message
                .buttons
                .iter()
                .flatten()
                .any(|button| button.data.starts_with("subagent:c:"))
        },
        "subagent:c:",
    )
    .await;
    assert!(subagent_data.len() <= 64, "{subagent_data}");

    channel.push_event(ChannelEvent::Button {
        chat: ChatId::new("100"),
        workspace: Some(WorkspaceId::new("9")),
        user: "alice".into(),
        data: subagent_data,
        source_message: MessageId::new("subagents"),
    });

    wait_until(|| provider.resumes().contains(&"child-session-1".to_string())).await;
    let created = channel.created_workspaces();
    assert!(
        created.iter().any(|(_, title, _)| title.contains("Parser")),
        "subagent activation should create a dedicated child topic: {created:?}"
    );
    channel.close_events();
    timeout(Duration::from_secs(2), run)
        .await
        .expect("subagent stream should stop")
        .expect("subagent run task should not panic");
}

#[tokio::test(flavor = "current_thread")]
async fn skills_topic_command_preserves_structured_markdown_catalog_shape() {
    let _test_lock = test_lock();
    let provider = Arc::new(ProviderProbe::with_skills(AgentSkillCatalog {
        skills: vec![
            skill_summary("imagegen"),
            skill_summary("skill-installer"),
            skill_summary("superpowers:*"),
            skill_summary("superpowers:brainstorming"),
            skill_summary("superpowers:writing-plans"),
            skill_summary("web-access:web-access"),
        ],
    }));
    let core = core_with_provider(Arc::clone(&provider));
    bind_workspace(&core, "workspace-a", "9", "thread-1");

    let channel = Arc::new(RecordingChannel::with_events(vec![ChannelEvent::Message(
        topic_message("9", "m-skills", "/skills"),
    )]));
    let bot = bot_with_state(Arc::clone(&channel), Arc::clone(&core));
    timeout(Duration::from_secs(2), bot.run())
        .await
        .expect("skills run should finish after finite event stream");

    let skills = eventually_topic_message(&channel, "9", |message| {
        message.body.starts_with("skills") && message.body.contains("`imagegen`")
    })
    .await;
    assert_eq!(skills.format, TextFormat::Markdown);
    assert!(skills.buttons.is_empty());
    assert!(skills.body.contains("- `imagegen`"));
    assert!(skills.body.contains("- `superpowers:*`"));
    assert!(skills.body.contains("  |-- `brainstorming`"));
    assert!(skills.body.contains("  |-- `writing-plans`"));
    assert!(!skills.body.contains('•'));
}

#[tokio::test(flavor = "current_thread")]
async fn fork_shortcut_from_another_topic_is_stale_and_does_not_cross_workspace() {
    let _test_lock = test_lock();
    let provider = Arc::new(ProviderProbe::with_fork_targets(vec![AgentForkTarget {
        id: "target-a".into(),
        label: Some("First rollback".into()),
        description: Some("turn 1".into()),
    }]));
    let core = core_with_provider(Arc::clone(&provider));
    bind_workspace(&core, "workspace-a", "9", "thread-a");
    bind_workspace(&core, "workspace-b", "10", "thread-b");
    let state = BotState::new_with_core(Arc::clone(&core));

    let list_channel = Arc::new(RecordingChannel::with_events(vec![ChannelEvent::Message(
        topic_message("9", "m-fork-list", "/fork"),
    )]));
    let list_bot = Arc::new(Bot::new_with_state(
        Arc::clone(&list_channel) as Arc<dyn Channel>,
        Arc::clone(&core),
        WorkspaceHandle::new(ChatId::new("100"), WorkspaceId::new("")),
        Arc::clone(&state),
    ));
    timeout(Duration::from_secs(2), list_bot.run())
        .await
        .expect("fork list run should finish after finite event stream");
    wait_until(|| {
        fork_selector_rendered_without_buttons(&list_channel.sent_messages())
            || fork_selector_rendered_without_buttons(&list_channel.edited_messages())
    })
    .await;

    let select_channel = Arc::new(RecordingChannel::with_events(vec![ChannelEvent::Message(
        topic_message("10", "m-cross-fork", "/f1"),
    )]));
    let select_bot = Arc::new(Bot::new_with_state(
        Arc::clone(&select_channel) as Arc<dyn Channel>,
        Arc::clone(&core),
        WorkspaceHandle::new(ChatId::new("100"), WorkspaceId::new("")),
        state,
    ));
    timeout(Duration::from_secs(2), select_bot.run())
        .await
        .expect("cross-topic fork selection should finish after finite event stream");

    assert!(
        provider.fork_selections().is_empty(),
        "/fN from another topic must not reuse the previous topic's fork targets"
    );
    let stale = eventually_topic_message(&select_channel, "10", |message| {
        message.body.contains("Stale fork target")
    })
    .await;
    assert!(stale.body.contains("Run /fork to refresh"));
}

#[tokio::test(flavor = "current_thread")]
async fn entry_chat_freeform_message_only_shows_management_hint() {
    let _test_lock = test_lock();
    let provider = Arc::new(ProviderProbe::default());
    let core = core_with_provider(Arc::clone(&provider));
    let channel = Arc::new(RecordingChannel::with_events(vec![ChannelEvent::Message(
        IncomingMessage {
            message_id: MessageId::new("m-root"),
            chat: ChatId::new("100"),
            workspace: None,
            reply_to: None,
            user: "alice".into(),
            text: Some("what is this?".into()),
            attachments: Vec::new(),
        },
    )]));
    let bot = bot_with_state(Arc::clone(&channel), Arc::clone(&core));

    timeout(Duration::from_secs(2), bot.run())
        .await
        .expect("entry chat run should finish after finite event stream");

    assert!(
        provider.inputs().is_empty(),
        "entry chat free-form text must not be submitted to an agent"
    );
    let hint = eventually_topic_message(&channel, "", |message| {
        message.body.contains("management panel")
    })
    .await;
    assert_eq!(hint.format, TextFormat::Markdown);
}

#[tokio::test(flavor = "current_thread")]
async fn entry_panel_renders_user_click_targets_as_text_commands() {
    let _test_lock = test_lock();
    let _env = IsolatedHistoryEnv::new();
    write_codex_history_session_at(
        _env.codex_home(),
        "thread-panel",
        "/tmp/project-panel",
        "panel prompt",
        15,
    );
    let provider = Arc::new(ProviderProbe::default());
    let core = core_with_provider(Arc::clone(&provider));
    let state = BotState::new_with_core(Arc::clone(&core));
    state
        .upsert_with_topic(
            WorkSession {
                workspace: WorkspaceId::new("workspace-panel"),
                chat: ChatId::new("100"),
                provider_id: "codex",
                project_path: Some(PathBuf::from("/tmp/open-project")),
                title: "codex open project".into(),
                live: None,
                resume_ref: Some("thread-open".into()),
            },
            WorkspaceId::new("9"),
        )
        .expect("seed open workspace topic");
    let channel = Arc::new(RecordingChannel::with_events(vec![ChannelEvent::Message(
        entry_message("m-panel", "/panel"),
    )]));
    let bot = bot_with_existing_state(Arc::clone(&channel), Arc::clone(&core), state);

    timeout(Duration::from_secs(2), bot.run())
        .await
        .expect("panel render run should finish after finite event stream");

    let panel =
        eventually_topic_message(&channel, "", |message| message.body.contains("🛠 lucarne")).await;
    assert_eq!(panel.format, TextFormat::Markdown);
    assert!(panel.body.contains("/a1  Codex"));
    assert!(panel.body.contains("/h1  codex"));
    assert!(panel.body.contains("panel prompt"));
    assert!(!panel.body.contains("open workspaces"));
    assert!(panel.buttons.iter().flatten().all(|button| {
        !button.data.starts_with("history:")
            && !button.data.starts_with("panel_workspace:")
            && !button.data.starts_with("newagent:")
    }));
}

#[tokio::test(flavor = "current_thread")]
async fn entry_config_global_bypass_toggle_updates_system_default_and_panel() {
    let _test_lock = test_lock();
    let _env = IsolatedHistoryEnv::new();
    write_codex_history_session_at(
        _env.codex_home(),
        "thread-config-agent",
        "/tmp/config-project",
        "config prompt",
        0,
    );
    let provider = Arc::new(ProviderProbe::default());
    let core = core_with_provider(Arc::clone(&provider));

    let events = vec![
        ChannelEvent::Message(entry_message(
            "m-config-bypass-on",
            "/config global bypass on",
        )),
        ChannelEvent::Message(entry_message("m-open-agent", "/a1")),
        ChannelEvent::Message(topic_message("1", "m-first-turn", "first live turn")),
    ];
    let channel = Arc::new(RecordingChannel::with_events_and_gap(
        events,
        Duration::from_millis(250),
    ));
    let bot = bot_with_state(Arc::clone(&channel), Arc::clone(&core));
    timeout(Duration::from_secs(4), bot.run())
        .await
        .expect("config journey should finish after finite event stream");

    let panel = eventually_topic_message(&channel, "", |message| {
        message.body.contains("🛠 lucarne")
            && message.body.contains("⚙ config: global bypass on")
            && message.body.contains("`/config global bypass off`")
    })
    .await;
    assert_eq!(panel.format, TextFormat::Markdown);
    assert!(core.system_settings().session.force_bypass_permissions);
    wait_until(|| provider.opens() == 1).await;
    assert_eq!(
        provider.open_args(),
        vec![serde_json::json!({ "permission_mode": "bypass" })]
    );
}

#[tokio::test(flavor = "current_thread")]
async fn topic_config_workspace_bypass_toggle_updates_effective_config() {
    let _test_lock = test_lock();
    let _env = IsolatedHistoryEnv::new();
    let provider = Arc::new(ProviderProbe::default());
    let core = core_with_provider(Arc::clone(&provider));
    bind_workspace(&core, "workspace-a", "9", "thread-1");

    let channel = Arc::new(RecordingChannel::with_events_and_gap(
        vec![ChannelEvent::Message(topic_message(
            "9",
            "m-topic-config-bypass-on",
            "/config workspace bypass on",
        ))],
        Duration::from_millis(250),
    ));
    let bot = bot_with_state(Arc::clone(&channel), Arc::clone(&core));
    timeout(Duration::from_secs(4), bot.run())
        .await
        .expect("topic config journey should finish after finite event stream");

    let panel = eventually_topic_message(&channel, "", |message| {
        message.body.contains("🛠 lucarne") && message.body.contains("⚙ config: global bypass off")
    })
    .await;
    assert_eq!(panel.format, TextFormat::Markdown);
    eventually_topic_message(&channel, "9", |message| {
        message.body.contains("⚙ config")
            && message.body.contains("scope: `workspace`")
            && message.body.contains("bypass: `on`")
    })
    .await;
    assert!(!core.system_settings().session.force_bypass_permissions);
    assert!(
        core.effective_settings(Some(Path::new("/tmp/workspace-a")), None)
            .session
            .force_bypass_permissions
    );
    assert_eq!(provider.opens(), 0);
    assert!(provider.resumes().is_empty());
}

#[tokio::test(flavor = "current_thread")]
async fn entry_menu_help_and_pagination_commands_run_through_public_bot_flow() {
    let _test_lock = test_lock();
    let _env = IsolatedHistoryEnv::new();
    for idx in 0..12 {
        write_codex_history_session_at(
            _env.codex_home(),
            &format!("thread-entry-menu-{idx:02}"),
            &format!("/tmp/entry-menu-{idx:02}"),
            &format!("entry menu {idx:02} prompt"),
            idx,
        );
    }
    let provider = Arc::new(ProviderProbe::default());
    let core = core_with_provider(Arc::clone(&provider));
    let state = BotState::new_with_core(Arc::clone(&core));
    let channel = Arc::new(RecordingChannel::streaming());
    let bot = bot_with_existing_state(Arc::clone(&channel), Arc::clone(&core), state);

    let run = tokio::spawn(Arc::clone(&bot).run());

    channel.push_event(ChannelEvent::Message(entry_message("m-start", "/start")));
    wait_until(|| {
        channel
            .sent_messages()
            .iter()
            .filter(|sent| sent.message.body.contains("🛠 lucarne"))
            .count()
            >= 1
    })
    .await;

    channel.push_event(ChannelEvent::Message(entry_message(
        "m-refresh",
        "/refresh",
    )));
    wait_until(|| {
        channel
            .sent_messages()
            .iter()
            .filter(|sent| sent.message.body.contains("🛠 lucarne"))
            .count()
            >= 2
    })
    .await;

    channel.push_event(ChannelEvent::Message(entry_message("m-help", "/help")));
    let help = eventually_topic_message(&channel, "", |message| {
        message.format == TextFormat::Plain && message.body.contains("commands")
    })
    .await;
    assert_eq!(help.format, TextFormat::Plain);

    channel.push_event(ChannelEvent::Message(entry_message("m-next", "/next")));
    eventually_topic_message(&channel, "", |message| {
        message.body.contains("sessions (6-10 of 12)")
    })
    .await;

    channel.push_event(ChannelEvent::Message(entry_message("m-prev", "/prev")));
    wait_until(|| {
        channel
            .sent_messages()
            .iter()
            .filter(|sent| sent.message.body.contains("sessions (1-5 of 12)"))
            .count()
            >= 3
    })
    .await;

    channel.close_events();
    timeout(Duration::from_secs(2), run)
        .await
        .expect("entry menu command run should stop after channel close")
        .expect("entry menu command task should join");
}

#[tokio::test(flavor = "current_thread")]
async fn entry_command_help_covers_static_and_indexed_entry_commands() {
    let _test_lock = test_lock();
    let _env = IsolatedHistoryEnv::new();
    let provider = Arc::new(ProviderProbe::default());
    let core = core_with_provider(Arc::clone(&provider));
    for (message_id, text, expected) in [
        ("m-panel-help", "/panel help", "usage: `/panel`"),
        ("m-agent-help", "/a1 help", "usage: `/aN`"),
        ("m-history-help", "/h1 help", "usage: `/hN`"),
        ("m-workspace-help", "/w1 help", "usage: `/wN`"),
        (
            "m-config-help",
            "/config help",
            "usage: `/config [[global|workspace|session] <setting> <on|off>]`",
        ),
    ] {
        let channel = Arc::new(RecordingChannel::with_events(vec![ChannelEvent::Message(
            entry_message(message_id, text),
        )]));
        let bot = bot_with_state(Arc::clone(&channel), Arc::clone(&core));

        timeout(Duration::from_secs(4), bot.run())
            .await
            .expect("entry command help run should finish after finite event stream");

        wait_until(|| {
            topic_messages(&channel, "")
                .iter()
                .any(|sent| sent.message.body.contains(expected))
        })
        .await;
        let bodies = topic_messages(&channel, "")
            .into_iter()
            .map(|sent| sent.message.body)
            .collect::<Vec<_>>();
        assert!(
            bodies.iter().any(|body| body.contains(expected)),
            "missing {expected}; bodies={bodies:?}"
        );
        assert!(
            bodies
                .iter()
                .all(|body| !body.contains("This is the management panel")),
            "entry help commands should not fall through to the generic hint: {bodies:?}"
        );
    }
}

#[tokio::test(flavor = "current_thread")]
async fn entry_panel_provider_filter_button_scopes_visible_sessions() {
    let _test_lock = test_lock();
    let _env = IsolatedHistoryEnv::new();
    write_codex_history_session_at(
        _env.codex_home(),
        "thread-provider-filter-codex",
        "/tmp/provider-filter-codex",
        "codex filter prompt",
        20,
    );
    let copilot_home = PathBuf::from(std::env::var_os("COPILOT_HOME").expect("copilot home"));
    write_copilot_history_session(
        &copilot_home,
        "copilot-provider-filter",
        "copilot filter prompt",
    );
    let provider = Arc::new(ProviderProbe::default());
    let core = core_with_provider(Arc::clone(&provider));
    let state = BotState::new_with_core(Arc::clone(&core));
    let channel = Arc::new(RecordingChannel::with_events_and_gap(
        vec![
            ChannelEvent::Message(entry_message("m-panel", "/panel")),
            ChannelEvent::Button {
                chat: ChatId::new("100"),
                workspace: None,
                user: "alice".into(),
                data: "panel_provider:1:copilot".into(),
                source_message: MessageId::new("panel-1"),
            },
        ],
        Duration::from_millis(250),
    ));
    let bot = bot_with_existing_state(Arc::clone(&channel), Arc::clone(&core), state);

    timeout(Duration::from_secs(3), bot.run())
        .await
        .expect("provider filter button journey should finish after finite event stream");

    let filtered = eventually_topic_message(&channel, "", |message| {
        message.body.contains("provider: Copilot") && message.body.contains("copilot filter prompt")
    })
    .await;
    assert!(filtered.body.contains("•  copilot"));
    assert!(!filtered.body.contains("/h1  copilot"));
    assert!(!filtered.body.contains("codex filter prompt"));
}

#[tokio::test(flavor = "current_thread")]
async fn history_only_agent_row_shows_visible_unsupported_notice() {
    let _test_lock = test_lock();
    let _env = IsolatedHistoryEnv::new();
    let copilot_home = PathBuf::from(std::env::var_os("COPILOT_HOME").expect("copilot home"));
    write_copilot_history_session(
        &copilot_home,
        "copilot-history-only-agent",
        "copilot history-only prompt",
    );
    let provider = Arc::new(ProviderProbe::default());
    let core = core_with_provider(Arc::clone(&provider));
    let state = BotState::new_with_core(Arc::clone(&core));
    let channel = Arc::new(RecordingChannel::with_events_and_gap(
        vec![
            ChannelEvent::Message(entry_message("m-panel", "/panel")),
            ChannelEvent::Message(entry_message("m-history-only-agent", "/a1")),
        ],
        Duration::from_millis(250),
    ));
    let bot = bot_with_existing_state(Arc::clone(&channel), Arc::clone(&core), state);

    timeout(Duration::from_secs(3), bot.run())
        .await
        .expect("history-only agent row journey should finish after finite event stream");

    eventually_topic_message(&channel, "", |message| {
        message.body.contains("history-only provider")
    })
    .await;
    assert!(channel.created_workspaces().is_empty());
    assert_eq!(provider.opens(), 0);
}

#[tokio::test(flavor = "current_thread")]
async fn entry_action_buttons_help_commands_and_refresh_run_through_public_bot_flow() {
    let _test_lock = test_lock();
    let _env = IsolatedHistoryEnv::new();
    write_codex_history_session_at(
        _env.codex_home(),
        "thread-action-button",
        "/tmp/action-button",
        "action button prompt",
        21,
    );
    let provider = Arc::new(ProviderProbe::default());
    let core = core_with_provider(Arc::clone(&provider));
    let state = BotState::new_with_core(Arc::clone(&core));
    let channel = Arc::new(RecordingChannel::streaming());
    let bot = bot_with_existing_state(Arc::clone(&channel), Arc::clone(&core), state);

    let run = tokio::spawn(Arc::clone(&bot).run());
    channel.push_event(ChannelEvent::Message(entry_message(
        "m-action-panel",
        "/panel",
    )));
    eventually_topic_message(&channel, "", |message| message.body.contains("🛠 lucarne")).await;
    channel.push_event(ChannelEvent::Button {
        chat: ChatId::new("100"),
        workspace: None,
        user: "alice".into(),
        data: "help:commands".into(),
        source_message: MessageId::new("panel-1"),
    });
    eventually_topic_message(&channel, "", |message| {
        message.format == TextFormat::Plain
            && message.body.contains("commands")
            && message.body.contains("/commands")
            && message.body.contains("/status")
            && message.body.contains("/kill all|<session_id:pid>")
            && message.body.contains("/aN")
            && message.body.contains("/hN")
            && message.body.contains("/wN")
            && message.body.contains("/fork [target]")
    })
    .await;
    channel.push_event(ChannelEvent::Button {
        chat: ChatId::new("100"),
        workspace: None,
        user: "alice".into(),
        data: "refresh:1".into(),
        source_message: MessageId::new("panel-1"),
    });
    wait_until(|| {
        channel
            .edited_messages()
            .iter()
            .any(|sent| sent.message.body.contains("action button prompt"))
    })
    .await;
    channel.close_events();
    timeout(Duration::from_secs(2), run)
        .await
        .expect("entry action button run should stop after channel close")
        .expect("entry action button task should join");
}

#[tokio::test(flavor = "current_thread")]
async fn agent_row_click_creates_topic_and_first_topic_message_opens_session() {
    let _test_lock = test_lock();
    let _env = IsolatedHistoryEnv::new();
    write_codex_history_session_at(
        _env.codex_home(),
        "thread-agent-row",
        "/tmp/project-agent-row",
        "agent row prompt",
        12,
    );
    let provider = Arc::new(ProviderProbe::default());
    let core = core_with_provider(Arc::clone(&provider));
    let state = BotState::new_with_core(Arc::clone(&core));
    let events = vec![
        ChannelEvent::Message(entry_message("m-agent-panel", "/panel")),
        ChannelEvent::Message(entry_message("m-agent-open", "/a1")),
        ChannelEvent::Message(topic_message("1", "m-agent-first-turn", "first live turn")),
    ];
    let channel = Arc::new(RecordingChannel::with_events_and_gap(
        events,
        Duration::from_millis(250),
    ));
    let bot = bot_with_existing_state(Arc::clone(&channel), Arc::clone(&core), state);

    timeout(Duration::from_secs(4), bot.run())
        .await
        .expect("agent row journey should finish after finite event stream");

    assert_eq!(
        channel.created_workspaces(),
        vec![(
            "100".to_string(),
            "codex · new".to_string(),
            "1".to_string()
        )]
    );
    eventually_topic_message(&channel, "1", |message| {
        message.body.contains("New codex session")
    })
    .await;
    wait_until(|| provider.opens() == 1).await;
    wait_until(|| {
        provider
            .inputs()
            .iter()
            .any(|input| input.text.as_str() == "first live turn")
    })
    .await;
    assert_eq!(channel.acknowledged_topics(), vec!["1".to_string()]);
}

#[tokio::test(flavor = "current_thread")]
async fn stale_panel_button_rerenders_current_panel_without_applying_old_view() {
    let _test_lock = test_lock();
    let _env = IsolatedHistoryEnv::new();
    write_codex_history_session_at(
        _env.codex_home(),
        "thread-stale-panel",
        "/tmp/project-stale-panel",
        "stale panel prompt",
        13,
    );
    let provider = Arc::new(ProviderProbe::default());
    let core = core_with_provider(Arc::clone(&provider));
    let channel = Arc::new(RecordingChannel::with_events_and_gap(
        vec![
            ChannelEvent::Message(entry_message("m-stale-panel-current", "/panel")),
            ChannelEvent::Button {
                chat: ChatId::new("100"),
                workspace: None,
                user: "alice".into(),
                data: "panel_view:0:workspaces".into(),
                source_message: MessageId::new("old-panel"),
            },
        ],
        Duration::from_millis(250),
    ));
    let bot = bot_with_state(Arc::clone(&channel), Arc::clone(&core));

    timeout(Duration::from_secs(2), bot.run())
        .await
        .expect("stale panel click should finish after finite event stream");

    let stale_response = eventually_topic_message(&channel, "", |message| {
        message.body.contains("view: Overview") && message.body.contains("stale panel prompt")
    })
    .await;
    assert!(
        !stale_response.body.contains("view: Workspaces"),
        "stale view buttons must not apply an old panel state"
    );
}

#[tokio::test(flavor = "current_thread")]
async fn overview_w_command_does_not_reopen_saved_workspace_record() {
    let _test_lock = test_lock();
    let provider = Arc::new(ProviderProbe::default());
    let core = core_with_provider(Arc::clone(&provider));
    let state = BotState::new_with_core(Arc::clone(&core));
    let workspace = WorkspaceId::new("workspace-a");
    state
        .upsert_with_topic(
            WorkSession {
                workspace: workspace.clone(),
                chat: ChatId::new("100"),
                provider_id: "codex",
                project_path: Some(PathBuf::from("/tmp/project-a")),
                title: "codex project-a".into(),
                live: None,
                resume_ref: Some("thread-a".into()),
            },
            WorkspaceId::new("9"),
        )
        .expect("seed workspace topic");

    let open_channel = Arc::new(RecordingChannel::with_events(vec![ChannelEvent::Message(
        IncomingMessage {
            message_id: MessageId::new("m-open-workspace"),
            chat: ChatId::new("100"),
            workspace: None,
            reply_to: None,
            user: "alice".into(),
            text: Some("/w1".into()),
            attachments: Vec::new(),
        },
    )]));
    let open_bot = bot_with_existing_state(
        Arc::clone(&open_channel),
        Arc::clone(&core),
        Arc::clone(&state),
    );
    timeout(Duration::from_secs(2), open_bot.run())
        .await
        .expect("overview /w run should finish after finite event stream");

    assert!(provider.resumes().is_empty());
    assert!(open_channel.created_workspaces().is_empty());
    assert!(
        eventually_for(Duration::from_millis(500), || {
            open_channel
                .sent_messages()
                .iter()
                .any(|sent| sent.message.body.contains("Workspaces view"))
        })
        .await
    );
}

#[tokio::test(flavor = "current_thread")]
async fn topic_rename_command_updates_channel_and_persisted_workspace_title() {
    let _test_lock = test_lock();
    let provider = Arc::new(ProviderProbe::default());
    let core = core_with_provider(Arc::clone(&provider));
    let state = BotState::new_with_core(Arc::clone(&core));
    let workspace = WorkspaceId::new("workspace-rename");
    state
        .upsert_with_topic(
            WorkSession {
                workspace: workspace.clone(),
                chat: ChatId::new("100"),
                provider_id: "codex",
                project_path: Some(PathBuf::from("/tmp/project-rename")),
                title: "old title".into(),
                live: None,
                resume_ref: Some("thread-rename".into()),
            },
            WorkspaceId::new("9"),
        )
        .expect("seed workspace to rename");
    let channel = Arc::new(RecordingChannel::with_events(vec![ChannelEvent::Message(
        topic_message("9", "m-rename", "/rename better title"),
    )]));
    let bot = bot_with_existing_state(Arc::clone(&channel), Arc::clone(&core), Arc::clone(&state));

    timeout(Duration::from_secs(2), bot.run())
        .await
        .expect("rename run should finish after finite event stream");

    wait_until(|| channel.renames() == vec![("9".to_string(), "better title".to_string())]).await;
    assert_eq!(
        channel.renames(),
        vec![("9".to_string(), "better title".to_string())]
    );
    eventually_topic_message(&channel, "9", |message| {
        message.body.contains("Renamed workspace to better title")
    })
    .await;
    assert_eq!(
        state
            .get(&workspace)
            .expect("renamed workspace should remain present")
            .title,
        "better title"
    );
    assert_eq!(
        core.workspace_binding(&ControlWorkspaceId::new("workspace-rename"))
            .expect("workspace binding should remain present")
            .title
            .to_string(),
        "better title"
    );
}

#[tokio::test(flavor = "current_thread")]
async fn history_workspace_summary_row_drills_into_filtered_session_list() {
    let _test_lock = test_lock();
    let _env = IsolatedHistoryEnv::new();
    write_codex_history_session_at(
        _env.codex_home(),
        "thread-project-a",
        "/tmp/project-a",
        "first project prompt",
        20,
    );
    write_codex_history_session_at(
        _env.codex_home(),
        "thread-project-b",
        "/tmp/project-b",
        "second project prompt",
        10,
    );
    let provider = Arc::new(ProviderProbe::default());
    let core = core_with_provider(Arc::clone(&provider));
    let state = BotState::new_with_core(Arc::clone(&core));
    let events = vec![
        ChannelEvent::Message(entry_message("m-workspace-panel", "/panel")),
        ChannelEvent::Button {
            chat: ChatId::new("100"),
            workspace: None,
            user: "alice".into(),
            data: "panel_view:1:workspaces".into(),
            source_message: MessageId::new("panel-1"),
        },
        ChannelEvent::Message(IncomingMessage {
            message_id: MessageId::new("m-workspace-filter"),
            chat: ChatId::new("100"),
            workspace: None,
            reply_to: None,
            user: "alice".into(),
            text: Some("/w1".into()),
            attachments: Vec::new(),
        }),
    ];
    let channel = Arc::new(RecordingChannel::with_events_and_gap(
        events,
        Duration::from_millis(250),
    ));
    let bot = bot_with_existing_state(channel.clone(), Arc::clone(&core), state);

    timeout(Duration::from_secs(4), bot.run())
        .await
        .expect("workspace summary row run should finish after finite event stream");

    let workspace_panel = eventually_topic_message(&channel, "", |message| {
        message.body.contains("view: Workspaces") && message.body.contains("/w1")
    })
    .await;
    assert!(workspace_panel
        .buttons
        .iter()
        .flatten()
        .all(|button| { !button.data.starts_with("panel_workspace:") }));

    let sessions_panel = eventually_topic_message(&channel, "", |message| {
        message.body.contains("view: Sessions") && message.body.contains("first project prompt")
    })
    .await;
    assert!(sessions_panel.body.contains("cwd: /tmp/project-a"));
    assert!(!sessions_panel.body.contains("second project prompt"));
    let notification_toggle = sessions_panel
        .buttons
        .iter()
        .flatten()
        .find(|button| button.data.starts_with("panel_config:"))
        .expect("workspace details should expose a notification toggle");
    assert_eq!(notification_toggle.label, "🔔 notifications on");
    assert!(
        provider.inputs().is_empty(),
        "clicking a workspace summary row should drill into history sessions, not submit to an agent"
    );
}

#[tokio::test(flavor = "current_thread")]
async fn workspace_detail_notification_button_updates_workspace_config() {
    let _test_lock = test_lock();
    let _env = IsolatedHistoryEnv::new();
    write_codex_history_session_at(
        _env.codex_home(),
        "thread-project-a",
        "/tmp/project-a",
        "first project prompt",
        20,
    );
    write_codex_history_session_at(
        _env.codex_home(),
        "thread-project-b",
        "/tmp/project-b",
        "second project prompt",
        10,
    );
    let provider = Arc::new(ProviderProbe::default());
    let core = core_with_provider(Arc::clone(&provider));
    bind_workspace_with_project_path(&core, "workspace-a", "9", "thread-a", "/tmp/project-a");
    bind_workspace_with_project_path(&core, "workspace-b", "10", "thread-b", "/tmp/project-b");
    core.resume_workspace_with_events(ResumeWorkspaceRequest {
        workspace_id: ControlWorkspaceId::new("workspace-a"),
        force_bypass_permissions: false,
    })
    .await
    .expect("resume project a workspace for watched events");
    core.resume_workspace_with_events(ResumeWorkspaceRequest {
        workspace_id: ControlWorkspaceId::new("workspace-b"),
        force_bypass_permissions: false,
    })
    .await
    .expect("resume project b workspace for watched events");

    let channel = Arc::new(RecordingChannel::streaming());
    let bot = bot_with_state(Arc::clone(&channel), Arc::clone(&core));
    let run = tokio::spawn(Arc::clone(&bot).run());
    channel.push_event(ChannelEvent::Message(entry_message(
        "m-workspace-panel",
        "/panel",
    )));
    eventually_topic_message(&channel, "", |message| message.body.contains("🛠 lucarne")).await;

    channel.push_event(ChannelEvent::Button {
        chat: ChatId::new("100"),
        workspace: None,
        user: "alice".into(),
        data: "panel_view:1:workspaces".into(),
        source_message: MessageId::new("panel-1"),
    });
    eventually_topic_message(&channel, "", |message| {
        message.body.contains("view: Workspaces") && message.body.contains("/w1")
    })
    .await;
    channel.push_event(ChannelEvent::Message(entry_message(
        "m-workspace-filter",
        "/w1",
    )));
    let panel_config = eventually_button_data(
        &channel,
        "",
        |message| {
            message.body.contains("view: Sessions") && message.body.contains("first project prompt")
        },
        "panel_config:",
    )
    .await;
    channel.push_event(ChannelEvent::Button {
        chat: ChatId::new("100"),
        workspace: None,
        user: "alice".into(),
        data: panel_config,
        source_message: MessageId::new("panel-1"),
    });
    eventually_topic_message(&channel, "", |message| {
        message
            .buttons
            .iter()
            .flatten()
            .any(|button| button.label == "🔕 notifications off")
    })
    .await;
    assert!(
        !core
            .effective_settings(Some(Path::new("/tmp/project-a")), None)
            .notifications
            .enabled,
        "workspace detail button must write notification state into core workspace config"
    );

    provider
        .emit_to_session("thread-a", assistant_message("muted project a"))
        .await;
    sleep(Duration::from_millis(50)).await;
    assert!(
        channel
            .sent_messages()
            .iter()
            .chain(channel.edited_messages().iter())
            .all(|sent| !sent.message.body.contains("muted project a")),
        "workspace detail toggle must suppress watched messages for that workspace"
    );

    provider
        .emit_to_session("thread-b", assistant_message("visible project b"))
        .await;
    wait_until(|| {
        channel
            .sent_messages()
            .iter()
            .any(|sent| sent.message.body.contains("visible project b"))
    })
    .await;

    channel.close_events();
    timeout(Duration::from_secs(2), run)
        .await
        .expect("workspace notification toggle run should stop after channel close")
        .expect("workspace notification toggle task should join");
}

#[tokio::test(flavor = "current_thread")]
async fn workspace_summary_pagination_keeps_w_rows_scoped_to_current_page() {
    let _test_lock = test_lock();
    let _env = IsolatedHistoryEnv::new();
    for idx in 0..12 {
        write_codex_history_session_at(
            _env.codex_home(),
            &format!("thread-page-{idx:02}"),
            &format!("/tmp/project-page-{idx:02}"),
            &format!("project {idx:02} prompt"),
            idx,
        );
    }
    let provider = Arc::new(ProviderProbe::default());
    let core = core_with_provider(Arc::clone(&provider));
    let state = BotState::new_with_core(Arc::clone(&core));
    let events = vec![
        ChannelEvent::Message(entry_message("m-workspace-page-panel", "/panel")),
        ChannelEvent::Button {
            chat: ChatId::new("100"),
            workspace: None,
            user: "alice".into(),
            data: "panel_view:1:workspaces".into(),
            source_message: MessageId::new("panel-1"),
        },
        ChannelEvent::Button {
            chat: ChatId::new("100"),
            workspace: None,
            user: "alice".into(),
            data: "hist_page:2:5".into(),
            source_message: MessageId::new("panel-1"),
        },
        ChannelEvent::Message(IncomingMessage {
            message_id: MessageId::new("m-workspace-page-filter"),
            chat: ChatId::new("100"),
            workspace: None,
            reply_to: None,
            user: "alice".into(),
            text: Some("/w1".into()),
            attachments: Vec::new(),
        }),
    ];
    let channel = Arc::new(RecordingChannel::with_events_and_gap(
        events,
        Duration::from_millis(250),
    ));
    let bot = bot_with_existing_state(channel.clone(), Arc::clone(&core), state);

    timeout(Duration::from_secs(4), bot.run())
        .await
        .expect("workspace pagination row run should finish after finite event stream");

    eventually_topic_message(&channel, "", |message| {
        message.body.contains("workspaces (6-10 of 12)") && message.body.contains("project-page-06")
    })
    .await;
    let sessions_panel = eventually_topic_message(&channel, "", |message| {
        message.body.contains("view: Sessions") && message.body.contains("project 06 prompt")
    })
    .await;
    assert!(sessions_panel.body.contains("cwd: /tmp/project-page-06"));
    assert!(!sessions_panel.body.contains("project 11 prompt"));
}

#[tokio::test(flavor = "current_thread")]
async fn history_pagination_keeps_h_rows_scoped_to_current_page() {
    let _test_lock = test_lock();
    let _env = IsolatedHistoryEnv::new();
    for idx in 0..12 {
        write_codex_history_session_at(
            _env.codex_home(),
            &format!("thread-history-page-{idx:02}"),
            &format!("/tmp/history-page-{idx:02}"),
            &format!("history page {idx:02} prompt"),
            idx,
        );
    }
    let provider = Arc::new(ProviderProbe::default());
    let core = core_with_provider(Arc::clone(&provider));
    let state = BotState::new_with_core(Arc::clone(&core));
    let channel = Arc::new(RecordingChannel::streaming());
    let bot = bot_with_existing_state(Arc::clone(&channel), Arc::clone(&core), state);

    let run = tokio::spawn(Arc::clone(&bot).run());
    wait_for_channel_subscription(&channel).await;
    channel.push_event(ChannelEvent::Message(entry_message(
        "m-history-page-panel",
        "/panel",
    )));
    eventually_topic_message(&channel, "", |message| {
        message.body.contains("sessions (1-5 of 12)") && message.body.contains("/h1")
    })
    .await;
    channel.push_event(ChannelEvent::Button {
        chat: ChatId::new("100"),
        workspace: None,
        user: "alice".into(),
        data: "hist_page:1:5".into(),
        source_message: MessageId::new("panel-1"),
    });
    let page = eventually_topic_message(&channel, "", |message| {
        message.body.contains("sessions (6-10 of 12)") && message.body.contains("/h1")
    })
    .await;
    let selected_session_id = first_history_session_id(&page.body);
    channel.push_event(ChannelEvent::Message(entry_message(
        "m-history-page-open",
        "/h1",
    )));
    assert!(
        eventually(|| provider.resumes() == vec![selected_session_id.clone()]).await,
        "expected /h1 to follow the currently rendered history page; selected={selected_session_id}; resumes={:?}; sent={:?}; edits={:?}",
        provider.resumes(),
        channel.sent_messages(),
        channel.edited_messages()
    );
    let topic = channel.created_workspaces()[0].2.clone();
    eventually_topic_message(&channel, &topic, |message| {
        message.body.contains("resumed") && message.body.contains(&selected_session_id)
    })
    .await;
    channel.close_events();
    timeout(Duration::from_secs(2), run)
        .await
        .expect("history pagination run should stop")
        .expect("history pagination task should join");
}

#[tokio::test(flavor = "current_thread")]
async fn history_row_click_reuses_bound_topic_and_suppresses_replay_on_repeat() {
    let _test_lock = test_lock();
    let _env = IsolatedHistoryEnv::new();
    write_codex_history_session_with_reply(
        _env.codex_home(),
        "thread-history",
        "/tmp/project-history",
        "history prompt",
        "history answer",
    );
    let provider = Arc::new(ProviderProbe::default());
    let core = core_with_provider(Arc::clone(&provider));
    let state = BotState::new_with_core(Arc::clone(&core));

    let first_channel = Arc::new(RecordingChannel::with_events_and_gap(
        vec![
            ChannelEvent::Message(entry_message("m-history-panel", "/panel")),
            ChannelEvent::Message(entry_message("m-history-open", "/h1")),
        ],
        Duration::from_millis(250),
    ));
    let first_bot = bot_with_existing_state(
        Arc::clone(&first_channel),
        Arc::clone(&core),
        Arc::clone(&state),
    );
    timeout(Duration::from_secs(3), first_bot.run())
        .await
        .expect("history row click run should finish after finite event stream");

    wait_until(|| provider.resumes() == vec!["thread-history".to_string()]).await;
    assert_eq!(
        first_channel.created_workspaces().len(),
        1,
        "the first /hN click should create one topic for the resumed history session"
    );
    let topic = first_channel.created_workspaces()[0].2.clone();
    let resumed = eventually_topic_message(&first_channel, &topic, |message| {
        message.body.contains("resumed") && message.body.contains("thread-history")
    })
    .await;
    assert!(resumed.body.contains("/tmp/project-history"));
    eventually_topic_message(&first_channel, &topic, |message| {
        message.body.contains("history prompt")
    })
    .await;
    eventually_topic_message(&first_channel, &topic, |message| {
        message.body.contains("history answer")
    })
    .await;

    let repeat_channel = Arc::new(RecordingChannel::with_events_and_gap_starting_workspace_id(
        vec![
            ChannelEvent::Message(entry_message("m-history-repeat-panel", "/panel")),
            ChannelEvent::Message(entry_message("m-history-repeat", "/h1")),
        ],
        Duration::from_millis(250),
        2,
    ));
    let repeat_bot = bot_with_existing_state(repeat_channel.clone(), Arc::clone(&core), state);
    timeout(Duration::from_secs(3), repeat_bot.run())
        .await
        .expect("repeat history row click run should finish after finite event stream");

    eventually_topic_message(&repeat_channel, &topic, |message| {
        message.body.contains("resumed") && message.body.contains("thread-history")
    })
    .await;
    let repeat_messages = topic_messages(&repeat_channel, &topic);
    assert!(
        repeat_messages.iter().all(|message| {
            !message.message.body.contains("history prompt")
                && !message.message.body.contains("history answer")
        }),
        "reopening the same /hN entry should not replay an already projected transcript: {repeat_messages:?}"
    );
    assert!(
        repeat_channel.deleted_workspaces().is_empty(),
        "reopening the same /hN entry should reuse the previous bound topic"
    );
    assert!(
        repeat_channel.created_workspaces().is_empty(),
        "reopening the same /hN entry should not create a replacement topic"
    );
}

#[tokio::test(flavor = "current_thread")]
async fn history_row_click_after_restart_recreates_topic_hidden_by_user_deletion() {
    let _test_lock = test_lock();
    let _env = IsolatedHistoryEnv::new();
    let db_dir = TempDir::new().expect("state db temp dir");
    let db = db_dir.path().join("state.sqlite3");
    write_codex_history_session_with_reply(
        _env.codex_home(),
        "thread-history-restart",
        "/tmp/project-history-restart",
        "restart prompt",
        "restart answer",
    );
    let provider = Arc::new(ProviderProbe::with_models(AgentModelCatalog {
        current_model: Some("gpt-5.5".into()),
        current_reasoning: Some("medium".into()),
        models: vec![AgentModelOption {
            id: "gpt-5.5".into(),
            display_name: Some("GPT-5.5".into()),
            description: Some("Frontier model".into()),
            supported_reasoning: vec![AgentReasoningOption {
                value: "medium".into(),
                description: None,
                is_default: Some(true),
            }],
        }],
    }));

    let first_core = core_with_provider_db(Arc::clone(&provider), &db);
    let first_state = BotState::new_with_core(Arc::clone(&first_core));
    let first_channel = Arc::new(RecordingChannel::with_events_and_gap(
        vec![
            ChannelEvent::Message(entry_message("m-history-panel", "/panel")),
            ChannelEvent::Message(entry_message("m-history-open", "/h1")),
            ChannelEvent::Message(topic_message("1", "m-status", "/status")),
        ],
        Duration::from_millis(250),
    ));
    let first_bot = bot_with_existing_state(
        Arc::clone(&first_channel),
        Arc::clone(&first_core),
        first_state,
    );
    timeout(Duration::from_secs(4), first_bot.run())
        .await
        .expect("initial history topic run should finish after finite event stream");

    wait_until(|| {
        provider
            .resumes()
            .iter()
            .any(|resume| resume == "thread-history-restart")
    })
    .await;
    wait_until(|| provider.status_calls() == 1).await;
    assert_eq!(
        first_channel.created_workspaces(),
        vec![(
            "100".into(),
            "codex · project-history-restart".into(),
            "1".into()
        )],
        "first /hN click should create the initial visible topic"
    );

    // This models a user deleting/hiding the topic from Telegram while lucarned
    // is down: the control-plane binding still points at topic 1 and the Bot
    // API may still accept that thread id, so the fix must not rely on probe
    // or send returning WorkspaceNotFound.
    let second_core = core_with_provider_db(Arc::clone(&provider), &db);
    let second_state = BotState::new_with_core(Arc::clone(&second_core));
    let second_channel = Arc::new(RecordingChannel::with_events_and_gap_starting_workspace_id(
        vec![
            ChannelEvent::Message(entry_message("m-history-restart-panel", "/panel")),
            ChannelEvent::Message(entry_message("m-history-restart-open", "/h1")),
            ChannelEvent::Message(topic_message("2", "m-model", "/model")),
        ],
        Duration::from_millis(250),
        2,
    ));
    second_channel.mark_workspace_missing("1");
    let second_bot = bot_with_existing_state(
        Arc::clone(&second_channel),
        Arc::clone(&second_core),
        Arc::clone(&second_state),
    );
    timeout(Duration::from_secs(4), second_bot.run())
        .await
        .expect("restarted history topic run should finish after finite event stream");

    assert_eq!(
        second_channel.created_workspaces(),
        vec![(
            "100".into(),
            "codex · project-history-restart".into(),
            "2".into()
        )],
        "reopening a history entry after restart must create a fresh visible topic"
    );
    let rebound = second_state
        .all()
        .into_iter()
        .find(|session| session.resume_ref.as_deref() == Some("thread-history-restart"))
        .expect("restarted state should hydrate the resumed session");
    assert_eq!(
        second_state
            .topic_for_workspace(&rebound.workspace)
            .as_ref()
            .map(|topic| topic.as_str()),
        Some("2"),
        "newly created topic must be rebound before subsequent topic commands"
    );
    eventually_topic_message(&second_channel, "2", |message| {
        message.body.contains("restart prompt")
    })
    .await;
    eventually_topic_message(&second_channel, "2", |message| {
        message.body.contains("models") && message.body.contains("gpt-5.5")
    })
    .await;
}

#[tokio::test(flavor = "current_thread")]
async fn entry_history_command_inside_new_unbound_topic_still_opens_panel_history() {
    let _test_lock = test_lock();
    let _env = IsolatedHistoryEnv::new();
    for idx in 0..4 {
        write_codex_history_session_at(
            _env.codex_home(),
            &format!("thread-auto-topic-{idx:02}"),
            &format!("/tmp/auto-topic-{idx:02}"),
            &format!("auto topic {idx:02} prompt"),
            idx,
        );
    }
    let provider = Arc::new(ProviderProbe::default());
    let core = core_with_provider(Arc::clone(&provider));
    let state = BotState::new_with_core(Arc::clone(&core));
    let channel = Arc::new(RecordingChannel::streaming());
    let bot = bot_with_existing_state(Arc::clone(&channel), Arc::clone(&core), state);

    let run = tokio::spawn(Arc::clone(&bot).run());
    wait_for_channel_subscription(&channel).await;
    channel.push_event(ChannelEvent::Message(entry_message(
        "m-auto-topic-panel",
        "/panel",
    )));
    eventually_topic_message(&channel, "", |message| {
        message.body.contains("/h4") && message.body.contains("auto topic 00 prompt")
    })
    .await;
    channel.push_event(ChannelEvent::Message(topic_service_message(
        "auto-h4",
        "m-auto-topic-created",
    )));
    channel.push_event(ChannelEvent::Message(topic_message(
        "auto-h4",
        "m-auto-topic-command",
        "/h4",
    )));

    assert!(
        eventually(|| provider.resumes() == vec!["thread-auto-topic-00".to_string()]).await,
        "expected /h4 to resume the fourth visible history session; resumes={:?}; sent={:?}; created={:?}",
        provider.resumes(),
        channel.sent_messages(),
        channel.created_workspaces()
    );
    let created = channel.created_workspaces();
    assert_eq!(
        created.len(),
        1,
        "history command should create the selected session topic, not reject the accidental topic: {created:?}"
    );
    assert_eq!(
        created[0].1, "codex · auto-topic-00",
        "history topic title should still come from the selected history entry"
    );
    eventually_topic_message(&channel, &created[0].2, |message| {
        message.body.contains("resumed") && message.body.contains("thread-auto-topic-00")
    })
    .await;
    eventually_topic_message(&channel, &created[0].2, |message| {
        message.body.contains("auto topic 00 prompt")
    })
    .await;
    let sent = channel.sent_messages();
    assert!(
        sent.iter().all(|message| !message
            .message
            .body
            .contains("isn't bound to an agent session")),
        "auto-created unbound topic should not emit binding errors: {sent:?}"
    );
    assert_eq!(
        channel.deleted_workspaces(),
        vec!["auto-h4".to_string()],
        "the accidental command-named topic should be cleaned up after routing the entry command"
    );

    channel.close_events();
    timeout(Duration::from_secs(2), run)
        .await
        .expect("auto-created topic command run should stop")
        .expect("auto-created topic command task should join");
}

#[tokio::test(flavor = "current_thread")]
async fn history_replay_is_scoped_to_current_topic_binding() {
    let _test_lock = test_lock();
    let _env = IsolatedHistoryEnv::new();
    write_codex_history_session_with_reply(
        _env.codex_home(),
        "thread-history-rebound",
        "/tmp/project-history-rebound",
        "rebound history prompt",
        "rebound history answer",
    );
    let provider = Arc::new(ProviderProbe::default());
    let core = core_with_provider(Arc::clone(&provider));
    let state = BotState::new_with_core(Arc::clone(&core));

    let first_channel = Arc::new(RecordingChannel::with_events_and_gap(
        vec![
            ChannelEvent::Message(entry_message("m-history-panel", "/panel")),
            ChannelEvent::Message(entry_message("m-history-open", "/h1")),
        ],
        Duration::from_millis(250),
    ));
    let first_bot = bot_with_existing_state(
        Arc::clone(&first_channel),
        Arc::clone(&core),
        Arc::clone(&state),
    );
    timeout(Duration::from_secs(3), first_bot.run())
        .await
        .expect("initial history row click should finish after finite event stream");

    wait_until(|| provider.resumes() == vec!["thread-history-rebound".to_string()]).await;
    eventually_topic_message(&first_channel, "1", |message| {
        message.body.contains("rebound history answer")
    })
    .await;
    let workspace = state
        .all()
        .into_iter()
        .find(|session| session.resume_ref.as_deref() == Some("thread-history-rebound"))
        .expect("resumed workspace")
        .workspace;
    let rebound_topic = WorkspaceId::new("rebound-topic");
    let session = state.get(&workspace).expect("workspace session");
    state
        .upsert_with_topic(session, rebound_topic.clone())
        .expect("rebind workspace to replacement topic");

    let rebound_channel = Arc::new(RecordingChannel::with_events_and_gap_starting_workspace_id(
        vec![
            ChannelEvent::Message(entry_message("m-history-reopen-panel", "/panel")),
            ChannelEvent::Message(entry_message("m-history-reopen", "/h1")),
        ],
        Duration::from_millis(250),
        2,
    ));
    let rebound_bot =
        bot_with_existing_state(Arc::clone(&rebound_channel), Arc::clone(&core), state);
    timeout(Duration::from_secs(3), rebound_bot.run())
        .await
        .expect("rebound history row click should finish after finite event stream");
    eventually_topic_message(&rebound_channel, rebound_topic.as_str(), |message| {
        message.body.contains("rebound history prompt")
    })
    .await;

    assert!(
        rebound_channel.deleted_workspaces().is_empty(),
        "panel history re-entry should reuse the currently bound topic when it still exists"
    );
    assert!(
        rebound_channel.created_workspaces().is_empty(),
        "history replay must not create a replacement topic for an existing current binding"
    );
    eventually_topic_message(&rebound_channel, rebound_topic.as_str(), |message| {
        message.body.contains("rebound history answer")
    })
    .await;
}

#[tokio::test(flavor = "current_thread")]
async fn history_load_older_button_runs_through_public_bot_flow() {
    let _test_lock = test_lock();
    let _env = IsolatedHistoryEnv::new();
    write_codex_history_session_with_turns(
        _env.codex_home(),
        "thread-history-older",
        "/tmp/project-history-older",
        12,
    );
    let provider = Arc::new(ProviderProbe::default());
    let core = core_with_provider(Arc::clone(&provider));
    let state = BotState::new_with_core(Arc::clone(&core));
    let channel = Arc::new(RecordingChannel::streaming());
    let bot = bot_with_existing_state(Arc::clone(&channel), Arc::clone(&core), state);

    let run = tokio::spawn(Arc::clone(&bot).run());
    channel.push_event(ChannelEvent::Message(entry_message(
        "m-history-panel",
        "/panel",
    )));
    eventually_topic_message(&channel, "", |message| message.body.contains("🛠 lucarne")).await;
    channel.push_event(ChannelEvent::Message(entry_message(
        "m-history-open",
        "/h1",
    )));

    wait_until(|| channel.created_workspaces().len() == 1).await;
    let topic = channel.created_workspaces()[0].2.clone();
    eventually_topic_message(&channel, &topic, |message| {
        message.body.starts_with("👤 ") && message.body.contains("user 3")
    })
    .await;
    let initial_active_bodies = active_topic_messages(&channel, &topic)
        .into_iter()
        .map(|sent| sent.message.body)
        .collect::<Vec<_>>();
    assert!(
        initial_active_bodies
            .iter()
            .all(|body| !body.contains("\n\nuser 1\n\n")),
        "the initial replay should only show the recent window: {initial_active_bodies:?}"
    );

    let older_data = eventually_button_data(
        &channel,
        &topic,
        |message| {
            message
                .buttons
                .iter()
                .flatten()
                .any(|button| button.data.starts_with("historyolder:c:"))
        },
        "historyolder:c:",
    )
    .await;
    channel.push_event(ChannelEvent::Button {
        chat: ChatId::new("100"),
        workspace: Some(WorkspaceId::new(topic.clone())),
        user: "alice".into(),
        data: older_data,
        source_message: MessageId::new("older"),
    });

    wait_until(|| {
        active_topic_messages(&channel, &topic)
            .iter()
            .filter(|sent| sent.message.body.starts_with("👤 "))
            .count()
            == 12
    })
    .await;
    let active = active_topic_messages(&channel, &topic);
    let user_markers = active
        .iter()
        .filter(|sent| sent.message.body.starts_with("👤 "))
        .collect::<Vec<_>>();
    assert_eq!(user_markers.len(), 12);
    let user_one = user_markers
        .iter()
        .find(|sent| sent.message.body.contains("\n\nuser 1\n\n"))
        .expect("user 1 marker");
    let user_twelve = user_markers
        .iter()
        .find(|sent| sent.message.body.contains("\n\nuser 12\n\n"))
        .expect("user 12 marker");
    let assistant_one = active
        .iter()
        .find(|sent| sent.message.body.starts_with("assistant 1\n\n"))
        .expect("assistant 1 reply");
    let assistant_twelve = active
        .iter()
        .find(|sent| sent.message.body.starts_with("assistant 12\n\n"))
        .expect("assistant 12 reply");
    assert_eq!(
        assistant_one
            .message
            .reply_to
            .as_ref()
            .map(|id| id.as_str()),
        Some(user_one.id.as_str())
    );
    assert_eq!(
        assistant_twelve
            .message
            .reply_to
            .as_ref()
            .map(|id| id.as_str()),
        Some(user_twelve.id.as_str())
    );
    assert!(
        !channel.deleted_messages().is_empty(),
        "load older should delete the previously projected recent window"
    );
    assert!(
        active.iter().all(|sent| {
            !sent
                .message
                .buttons
                .iter()
                .flatten()
                .any(|button| button.data.starts_with("historyolder:c:"))
        }),
        "all history is loaded, so no active older control should remain"
    );

    channel.close_events();
    timeout(Duration::from_secs(2), run)
        .await
        .expect("history stream should stop")
        .expect("history run task should not panic");
}

fn core_with_provider(provider: Arc<ProviderProbe>) -> Arc<LucarneCore> {
    ensure_default_history_env();
    let runtime = Arc::new(lucarne::agent_runtime::AgentRuntime::new());
    runtime.register(provider);
    LucarneCore::from_runtime_and_store(
        runtime,
        ControlPlaneSqliteStore::open_in_memory().expect("open in-memory control-plane store"),
    )
    .expect("build core")
}

fn core_with_provider_db(provider: Arc<ProviderProbe>, db: &Path) -> Arc<LucarneCore> {
    ensure_default_history_env();
    let runtime = Arc::new(lucarne::agent_runtime::AgentRuntime::new());
    runtime.register(provider);
    LucarneCore::from_runtime_and_store(
        runtime,
        ControlPlaneSqliteStore::open(db).expect("open control-plane store"),
    )
    .expect("build core")
}

fn core_with_live_replay_provider(provider: Arc<LiveReplayProvider>) -> Arc<LucarneCore> {
    ensure_default_history_env();
    let runtime = Arc::new(lucarne::agent_runtime::AgentRuntime::new());
    runtime.register(provider);
    LucarneCore::from_runtime_and_store(
        runtime,
        ControlPlaneSqliteStore::open_in_memory().expect("open in-memory control-plane store"),
    )
    .expect("build core")
}

fn core_with_live_recording_provider(provider: Arc<LiveRecordingProvider>) -> Arc<LucarneCore> {
    let runtime = Arc::new(lucarne::agent_runtime::AgentRuntime::new());
    runtime.register(provider);
    LucarneCore::from_runtime_and_store(
        runtime,
        ControlPlaneSqliteStore::open_in_memory().expect("open in-memory control-plane store"),
    )
    .expect("build core")
}

fn bind_workspace(core: &LucarneCore, workspace: &str, topic: &str, resume_ref: &str) {
    bind_workspace_with_project_path(
        core,
        workspace,
        topic,
        resume_ref,
        &format!("/tmp/{workspace}"),
    );
}

fn bind_workspace_with_project_path(
    core: &LucarneCore,
    workspace: &str,
    topic: &str,
    resume_ref: &str,
    project_path: &str,
) {
    bind_workspace_in_chat_with_project_path(
        core,
        workspace,
        "100",
        topic,
        resume_ref,
        project_path,
    );
}

fn bind_workspace_without_topic(core: &LucarneCore, workspace: &str, resume_ref: &str) {
    core.upsert_workspace_binding(
        ControlWorkspaceId::new(workspace),
        OpenWorkspaceRequest {
            provider_id: "codex",
            project_path: Some(PathBuf::from(format!("/tmp/{workspace}"))),
            title: format!("codex {workspace}"),
        },
        Some(resume_ref),
    )
    .expect("persist workspace binding");
}

fn bind_workspace_in_chat(
    core: &LucarneCore,
    workspace: &str,
    chat: &str,
    topic: &str,
    resume_ref: &str,
) {
    bind_workspace_in_chat_with_project_path(
        core,
        workspace,
        chat,
        topic,
        resume_ref,
        &format!("/tmp/{workspace}"),
    );
}

fn bind_workspace_in_chat_with_project_path(
    core: &LucarneCore,
    workspace: &str,
    chat: &str,
    topic: &str,
    resume_ref: &str,
    project_path: &str,
) {
    core.upsert_workspace_binding(
        ControlWorkspaceId::new(workspace),
        OpenWorkspaceRequest {
            provider_id: "codex",
            project_path: Some(PathBuf::from(project_path)),
            title: format!("codex {workspace}"),
        },
        Some(resume_ref),
    )
    .expect("persist workspace binding");
    core.upsert_channel_binding(ChannelBinding::new(
        ChannelBindingId::new(format!("telegram:{chat}:{topic}")),
        ControlWorkspaceId::new(workspace),
        "telegram",
        chat,
        Some(topic),
    ))
    .expect("persist topic binding");
}

fn bot_with_existing_state(
    channel: Arc<RecordingChannel>,
    core: Arc<LucarneCore>,
    state: Arc<BotState>,
) -> Arc<Bot> {
    Arc::new(Bot::new_with_state_and_history_watch(
        channel,
        core,
        WorkspaceHandle::new(ChatId::new("100"), WorkspaceId::new("")),
        state,
        false,
    ))
}

fn bot_with_state(channel: Arc<RecordingChannel>, core: Arc<LucarneCore>) -> Arc<Bot> {
    let state = BotState::new_with_core(Arc::clone(&core));
    bot_with_existing_state(channel, core, state)
}

fn bot_with_state_and_history_watch(
    channel: Arc<RecordingChannel>,
    core: Arc<LucarneCore>,
) -> Arc<Bot> {
    let state = BotState::new_with_core(Arc::clone(&core));
    Arc::new(Bot::new_with_state_and_history_watch(
        channel,
        core,
        WorkspaceHandle::new(ChatId::new("100"), WorkspaceId::new("")),
        state,
        true,
    ))
}

fn topic_message(topic: &str, message_id: &str, text: &str) -> IncomingMessage {
    topic_message_in_chat("100", topic, message_id, text)
}

fn topic_message_in_chat(chat: &str, topic: &str, message_id: &str, text: &str) -> IncomingMessage {
    IncomingMessage {
        message_id: MessageId::new(message_id),
        chat: ChatId::new(chat),
        workspace: Some(WorkspaceId::new(topic)),
        reply_to: None,
        user: "alice".into(),
        text: Some(text.into()),
        attachments: Vec::new(),
    }
}

fn topic_service_message(topic: &str, message_id: &str) -> IncomingMessage {
    IncomingMessage {
        message_id: MessageId::new(message_id),
        chat: ChatId::new("100"),
        workspace: Some(WorkspaceId::new(topic)),
        reply_to: None,
        user: "alice".into(),
        text: None,
        attachments: Vec::new(),
    }
}

fn reply_to_topic_message(
    topic: &str,
    message_id: &str,
    reply_to: &str,
    text: &str,
) -> IncomingMessage {
    IncomingMessage {
        message_id: MessageId::new(message_id),
        chat: ChatId::new("100"),
        workspace: Some(WorkspaceId::new(topic)),
        reply_to: Some(MessageId::new(reply_to)),
        user: "alice".into(),
        text: Some(text.into()),
        attachments: Vec::new(),
    }
}

fn entry_message(message_id: &str, text: &str) -> IncomingMessage {
    IncomingMessage {
        message_id: MessageId::new(message_id),
        chat: ChatId::new("100"),
        workspace: None,
        reply_to: None,
        user: "alice".into(),
        text: Some(text.into()),
        attachments: Vec::new(),
    }
}

fn assistant_message(text: &str) -> Event {
    Event::Message(MessageEvent {
        role: MessageRole::Assistant,
        text: text.into(),
        streaming: false,
    })
}

fn turn_completed(turn_id: &str) -> Event {
    Event::TurnCompleted(lucarne::agent_runtime::events::TurnCompletedEvent {
        turn_id: turn_id.into(),
        usage: None,
    })
}

fn first_assistant_text(events: &[Event]) -> String {
    events
        .iter()
        .find_map(|event| match event {
            Event::Message(MessageEvent {
                role: MessageRole::Assistant,
                text,
                streaming: false,
            }) => Some(text.to_string()),
            _ => None,
        })
        .expect("cassette should contain a final assistant message")
}

fn image_attachment(file_ref: &str) -> IncomingAttachment {
    IncomingAttachment {
        file_ref: file_ref.into(),
        filename: Some("photo.jpg".into()),
        mime_type: Some("image/jpeg".into()),
        size: Some(3),
    }
}

fn text_attachment(file_ref: &str, filename: &str, mime_type: Option<&str>) -> IncomingAttachment {
    IncomingAttachment {
        file_ref: file_ref.into(),
        filename: Some(filename.into()),
        mime_type: mime_type.map(Into::into),
        size: Some(13),
    }
}

fn fork_selector_rendered_without_buttons(messages: &[SentMessage]) -> bool {
    messages.iter().any(|sent| {
        sent.topic == "9" && sent.message.body.contains("/f1") && sent.message.buttons.is_empty()
    })
}

async fn eventually_button_data(
    channel: &RecordingChannel,
    topic: &str,
    predicate: impl Fn(&OutgoingMessage) -> bool,
    data_prefix: &str,
) -> String {
    let found = eventually(|| {
        topic_messages(channel, topic).iter().any(|sent| {
            predicate(&sent.message)
                && sent
                    .message
                    .buttons
                    .iter()
                    .flatten()
                    .any(|button| button.data.starts_with(data_prefix))
        })
    })
    .await;
    assert!(
        found,
        "expected button was not sent; topic={topic}; prefix={data_prefix}; sent={:?}; edits={:?}",
        channel.sent_messages(),
        channel.edited_messages()
    );
    topic_messages(channel, topic)
        .into_iter()
        .find(|sent| predicate(&sent.message))
        .and_then(|sent| {
            sent.message
                .buttons
                .into_iter()
                .flatten()
                .find(|button| button.data.starts_with(data_prefix))
        })
        .map(|button| button.data)
        .expect("button found after wait")
}

fn first_history_session_id(body: &str) -> String {
    let marker = "\n      `";
    let start = body
        .find(marker)
        .expect("history row should include a copyable session id")
        + marker.len();
    let end = body[start..]
        .find('`')
        .expect("session id should close inline code")
        + start;
    body[start..end].to_string()
}

fn topic_messages(channel: &RecordingChannel, topic: &str) -> Vec<SentMessage> {
    channel
        .sent_messages()
        .into_iter()
        .chain(channel.edited_messages())
        .filter(|sent| sent.topic == topic)
        .collect()
}

fn active_topic_messages(channel: &RecordingChannel, topic: &str) -> Vec<SentMessage> {
    let deleted = channel.deleted_messages();
    topic_messages(channel, topic)
        .into_iter()
        .filter(|sent| !deleted.iter().any(|id| id == &sent.id))
        .collect()
}

fn topic_messages_in_chat(channel: &RecordingChannel, chat: &str, topic: &str) -> Vec<SentMessage> {
    channel
        .sent_messages()
        .into_iter()
        .chain(channel.edited_messages())
        .filter(|sent| sent.chat == chat && sent.topic == topic)
        .collect()
}

async fn eventually_topic_message(
    channel: &RecordingChannel,
    topic: &str,
    predicate: impl Fn(&OutgoingMessage) -> bool,
) -> OutgoingMessage {
    let found = eventually(|| {
        topic_messages(channel, topic)
            .iter()
            .any(|sent| predicate(&sent.message))
    })
    .await;
    assert!(
        found,
        "expected topic message was not sent; topic={topic}; sent={:?}; edits={:?}",
        channel.sent_messages(),
        channel.edited_messages()
    );
    topic_messages(channel, topic)
        .into_iter()
        .find(|sent| predicate(&sent.message))
        .expect("message found after wait")
        .message
}

async fn eventually_topic_message_with_timeout(
    channel: &RecordingChannel,
    topic: &str,
    duration: Duration,
    predicate: impl Fn(&OutgoingMessage) -> bool,
) -> OutgoingMessage {
    let found = eventually_for(duration, || {
        topic_messages(channel, topic)
            .iter()
            .any(|sent| predicate(&sent.message))
    })
    .await;
    assert!(
        found,
        "expected topic message was not sent; topic={topic}; sent={:?}; edits={:?}",
        channel.sent_messages(),
        channel.edited_messages()
    );
    topic_messages(channel, topic)
        .into_iter()
        .find(|sent| predicate(&sent.message))
        .expect("message found after wait")
        .message
}

fn compact_cwd_footer(path: &Path) -> String {
    format!("cwd: `{}`", compact_path(&path.display().to_string(), 58))
}

async fn eventually_topic_sent_message(
    channel: &RecordingChannel,
    predicate: impl Fn(&SentMessage) -> bool,
) -> SentMessage {
    let found = eventually(|| channel.sent_messages().iter().any(&predicate)).await;
    assert!(
        found,
        "expected sent topic message was not sent; sent={:?}; edits={:?}",
        channel.sent_messages(),
        channel.edited_messages()
    );
    channel
        .sent_messages()
        .into_iter()
        .find(predicate)
        .expect("sent message found after wait")
}

async fn eventually_topic_sent_message_with_timeout(
    channel: &RecordingChannel,
    duration: Duration,
    predicate: impl Fn(&SentMessage) -> bool,
) -> SentMessage {
    let found = eventually_for(duration, || channel.sent_messages().iter().any(&predicate)).await;
    assert!(
        found,
        "expected sent topic message was not sent; sent={:?}; edits={:?}",
        channel.sent_messages(),
        channel.edited_messages()
    );
    channel
        .sent_messages()
        .into_iter()
        .find(predicate)
        .expect("sent message found after wait")
}

async fn eventually_topic_message_in_chat(
    channel: &RecordingChannel,
    chat: &str,
    topic: &str,
    predicate: impl Fn(&OutgoingMessage) -> bool,
) -> OutgoingMessage {
    let found = eventually(|| {
        topic_messages_in_chat(channel, chat, topic)
            .iter()
            .any(|sent| predicate(&sent.message))
    })
    .await;
    assert!(
        found,
        "expected topic message was not sent; chat={chat}; topic={topic}; sent={:?}; edits={:?}",
        channel.sent_messages(),
        channel.edited_messages()
    );
    topic_messages_in_chat(channel, chat, topic)
        .into_iter()
        .find(|sent| predicate(&sent.message))
        .expect("message found after wait")
        .message
}

fn command_catalog_entry(name: &str, description: &str) -> AgentCommand {
    AgentCommand {
        name: name.into(),
        description: Some(description.into()),
        aliases: Vec::new(),
        source: AgentCommandSource::ProviderNative,
        input: AgentCommandInput::None,
        completion: AgentCommandCompletion::CommandResult,
    }
}

fn skill_summary(name: &str) -> AgentSkillSummary {
    AgentSkillSummary {
        name: name.into(),
        ..Default::default()
    }
}

struct IsolatedHistoryEnv {
    _lock: std::sync::MutexGuard<'static, ()>,
    _tmp: TempDir,
    codex_home: PathBuf,
    old: Vec<(&'static str, Option<OsString>)>,
}

impl IsolatedHistoryEnv {
    fn new() -> Self {
        let lock = ENV_LOCK.lock().expect("env lock");
        let tmp = TempDir::new().expect("history temp dir");
        let codex_home = tmp.path().join("codex");
        let vars = vec![
            ("HOME", tmp.path().as_os_str().to_os_string()),
            ("PATH", tmp.path().as_os_str().to_os_string()),
            (TEST_HISTORY_ENV_MARKER, OsString::from("isolated")),
            ("CODEX_HOME", codex_home.as_os_str().to_os_string()),
            (
                "CLAUDE_CONFIG_DIR",
                tmp.path().join("claude").as_os_str().to_os_string(),
            ),
            (
                "GEMINI_HOME",
                tmp.path().join("gemini").as_os_str().to_os_string(),
            ),
            (
                "GEMINI_CONFIG_DIR",
                tmp.path().join("gemini").as_os_str().to_os_string(),
            ),
            (
                "COPILOT_HOME",
                tmp.path().join("copilot").as_os_str().to_os_string(),
            ),
        ];
        let old = vars
            .iter()
            .map(|(key, _)| (*key, std::env::var_os(key)))
            .collect::<Vec<_>>();
        for (key, value) in vars {
            std::env::set_var(key, value);
        }
        Self {
            _lock: lock,
            _tmp: tmp,
            codex_home,
            old,
        }
    }

    fn codex_home(&self) -> &PathBuf {
        &self.codex_home
    }
}

impl Drop for IsolatedHistoryEnv {
    fn drop(&mut self) {
        for (key, value) in self.old.drain(..) {
            match value {
                Some(value) => std::env::set_var(key, value),
                None => std::env::remove_var(key),
            }
        }
    }
}

fn write_codex_history_session_at(
    codex_home: &PathBuf,
    session_id: &str,
    cwd: &str,
    prompt: &str,
    minute: u8,
) {
    let dir = codex_home.join("sessions/2026/05/05");
    fs::create_dir_all(&dir).expect("create codex session dir");
    let path = dir.join(format!(
        "rollout-2026-05-05T00-{minute:02}-00-{session_id}.jsonl"
    ));
    let content = [
        format!(
            r#"{{"timestamp":"2026-05-05T00:00:00.000Z","type":"session_meta","payload":{{"session_id":"{session_id}","cwd":"{cwd}","originator":"codex-cli","model":"gpt-5.5"}}}}"#
        ),
        format!(
            r#"{{"timestamp":"2026-05-05T00:{minute:02}:00.000Z","type":"response_item","payload":{{"type":"message","role":"user","content":[{{"type":"input_text","text":"{prompt}"}}]}}}}"#
        ),
    ]
    .join("\n");
    fs::write(&path, content).expect("write codex history session");
    let modified =
        std::time::SystemTime::UNIX_EPOCH + Duration::from_secs(1_767_225_000 + u64::from(minute));
    fs::File::open(&path)
        .expect("open codex history session for mtime")
        .set_times(
            fs::FileTimes::new()
                .set_accessed(modified)
                .set_modified(modified),
        )
        .expect("set codex history session mtime");
}

fn write_live_watch_codex_session(
    root: &Path,
    session_id: &str,
    cwd: &Path,
    prompt: &str,
) -> PathBuf {
    let dir = root.join("2026/05/06");
    fs::create_dir_all(&dir).expect("create codex session dir");
    let path = dir.join(format!("rollout-2026-05-06T00-00-00-{session_id}.jsonl"));
    let content = [
        format!(
            r#"{{"timestamp":"2026-05-06T00:00:00.000Z","type":"session_meta","payload":{{"session_id":"{session_id}","cwd":"{}","originator":"codex-cli","model":"gpt-5.5"}}}}"#,
            cwd.display()
        ),
        format!(
            r#"{{"timestamp":"2026-05-06T00:00:01.000Z","type":"response_item","payload":{{"type":"message","role":"user","content":[{{"type":"input_text","text":"{prompt}"}}]}}}}"#
        ),
    ]
    .join("\n");
    fs::write(&path, format!("{content}\n")).expect("write codex live watch session");
    path
}

fn append_codex_assistant_response(path: &Path, timestamp: &str, text: &str) {
    use std::io::Write;

    let mut file = fs::OpenOptions::new()
        .append(true)
        .open(path)
        .expect("open codex live watch session");
    writeln!(
        file,
        r#"{{"timestamp":"{timestamp}","type":"response_item","payload":{{"type":"message","role":"assistant","content":[{{"type":"output_text","text":"{text}"}}]}}}}"#
    )
    .expect("append codex assistant response");
    file.sync_all().expect("sync codex assistant response");
}

fn append_codex_task_complete(path: &Path, timestamp: &str, turn_id: &str, text: &str) {
    use std::io::Write;

    let mut file = fs::OpenOptions::new()
        .append(true)
        .open(path)
        .expect("open codex live watch session");
    writeln!(
        file,
        r#"{{"timestamp":"{timestamp}","type":"event_msg","payload":{{"type":"task_complete","turn_id":"{turn_id}","last_agent_message":"{text}"}}}}"#
    )
    .expect("append codex task complete");
    file.sync_all().expect("sync codex task complete");
}

fn write_codex_history_session_with_reply(
    codex_home: &PathBuf,
    session_id: &str,
    cwd: &str,
    prompt: &str,
    answer: &str,
) {
    let dir = codex_home.join("sessions/2026/05/05");
    fs::create_dir_all(&dir).expect("create codex session dir");
    let path = dir.join(format!("rollout-2026-05-05T00-30-00-{session_id}.jsonl"));
    let content = [
        format!(
            r#"{{"timestamp":"2026-05-05T00:00:00.000Z","type":"session_meta","payload":{{"session_id":"{session_id}","cwd":"{cwd}","originator":"codex-cli","model":"gpt-5.5"}}}}"#
        ),
        format!(
            r#"{{"timestamp":"2026-05-05T00:30:00.000Z","type":"response_item","payload":{{"type":"message","role":"user","content":[{{"type":"input_text","text":"{prompt}"}}]}}}}"#
        ),
        format!(
            r#"{{"timestamp":"2026-05-05T00:30:01.000Z","type":"response_item","payload":{{"type":"message","role":"assistant","content":[{{"type":"output_text","text":"{answer}"}}]}}}}"#
        ),
    ]
    .join("\n");
    fs::write(path, content).expect("write codex history session");
}

fn write_codex_history_session_with_turns(
    codex_home: &PathBuf,
    session_id: &str,
    cwd: &str,
    turns: usize,
) {
    let dir = codex_home.join("sessions/2026/05/05");
    fs::create_dir_all(&dir).expect("create codex session dir");
    let path = dir.join(format!("rollout-2026-05-05T00-40-00-{session_id}.jsonl"));
    let mut lines = vec![format!(
        r#"{{"timestamp":"2026-05-05T00:00:00.000Z","type":"session_meta","payload":{{"session_id":"{session_id}","cwd":"{cwd}","originator":"codex-cli","model":"gpt-5.5"}}}}"#
    )];
    for idx in 1..=turns {
        lines.push(format!(
            r#"{{"timestamp":"2026-05-05T00:{idx:02}:00.000Z","type":"response_item","payload":{{"type":"message","role":"user","content":[{{"type":"input_text","text":"user {idx}"}}]}}}}"#
        ));
        lines.push(format!(
            r#"{{"timestamp":"2026-05-05T00:{idx:02}:01.000Z","type":"response_item","payload":{{"type":"message","role":"assistant","content":[{{"type":"output_text","text":"assistant {idx}"}}]}}}}"#
        ));
    }
    fs::write(path, lines.join("\n")).expect("write codex history session");
}

fn write_copilot_history_session(copilot_home: &Path, session_id: &str, prompt: &str) {
    let dir = copilot_home.join("session-state/session-1");
    fs::create_dir_all(&dir).expect("create copilot session dir");
    let content = [
        format!(
            r#"{{"type":"session.start","timestamp":"2026-05-05T00:00:00.000Z","data":{{"sessionId":"{session_id}","selectedModel":"gpt-5.4"}}}}"#
        ),
        format!(
            r#"{{"type":"user.message","timestamp":"2026-05-05T00:00:01.000Z","data":{{"messageId":"u1","content":"{prompt}"}}}}"#
        ),
    ]
    .join("\n");
    fs::write(dir.join("events.jsonl"), content).expect("write copilot history session");
}

async fn wait_until(mut ready: impl FnMut() -> bool) {
    assert!(
        eventually(&mut ready).await,
        "condition was not reached before timeout"
    );
}

async fn wait_for_channel_subscription(channel: &RecordingChannel) {
    wait_until(|| channel.is_subscribed()).await;
}

async fn eventually(ready: impl FnMut() -> bool) -> bool {
    eventually_for(Duration::from_secs(2), ready).await
}

async fn eventually_for(duration: Duration, mut ready: impl FnMut() -> bool) -> bool {
    let deadline = Instant::now() + duration;
    while Instant::now() < deadline {
        if ready() {
            return true;
        }
        sleep(Duration::from_millis(10)).await;
    }
    ready()
}

#[derive(Clone, Debug)]
struct SentMessage {
    id: String,
    chat: String,
    topic: String,
    message: OutgoingMessage,
}

#[derive(Clone, Debug)]
struct SentAttachment {
    id: String,
    chat: String,
    topic: String,
    attachment: ChannelAttachment,
}

struct RecordingChannel {
    events: StdMutex<Option<Vec<ChannelEvent>>>,
    event_gap: Duration,
    live_tx: StdMutex<Option<mpsc::UnboundedSender<ChannelEvent>>>,
    live_rx: StdMutex<Option<mpsc::UnboundedReceiver<ChannelEvent>>>,
    sent: StdMutex<Vec<SentMessage>>,
    attachments: StdMutex<Vec<SentAttachment>>,
    edits: StdMutex<Vec<SentMessage>>,
    deleted: StdMutex<Vec<String>>,
    deleted_workspaces: StdMutex<Vec<String>>,
    acks: StdMutex<Vec<(String, String)>>,
    created_workspaces: StdMutex<Vec<(String, String, String)>>,
    renames: StdMutex<Vec<(String, String)>>,
    command_query_answers: StdMutex<Vec<(String, Vec<CommandQueryResult>)>>,
    downloads: StdMutex<HashMap<String, Vec<u8>>>,
    missing_workspaces: StdMutex<Vec<String>>,
    create_workspace_delay: Duration,
    next_message_id: AtomicUsize,
    next_workspace_id: AtomicUsize,
}

impl RecordingChannel {
    fn with_events(events: Vec<ChannelEvent>) -> Self {
        Self::with_events_and_gap(events, Duration::ZERO)
    }

    fn with_events_and_gap(events: Vec<ChannelEvent>, event_gap: Duration) -> Self {
        Self {
            events: StdMutex::new(Some(events)),
            event_gap,
            live_tx: StdMutex::new(None),
            live_rx: StdMutex::new(None),
            sent: StdMutex::new(Vec::new()),
            attachments: StdMutex::new(Vec::new()),
            edits: StdMutex::new(Vec::new()),
            deleted: StdMutex::new(Vec::new()),
            deleted_workspaces: StdMutex::new(Vec::new()),
            acks: StdMutex::new(Vec::new()),
            created_workspaces: StdMutex::new(Vec::new()),
            renames: StdMutex::new(Vec::new()),
            command_query_answers: StdMutex::new(Vec::new()),
            downloads: StdMutex::new(HashMap::new()),
            missing_workspaces: StdMutex::new(Vec::new()),
            create_workspace_delay: Duration::ZERO,
            next_message_id: AtomicUsize::new(1),
            next_workspace_id: AtomicUsize::new(1),
        }
    }

    fn with_events_and_gap_starting_workspace_id(
        events: Vec<ChannelEvent>,
        event_gap: Duration,
        next_workspace_id: usize,
    ) -> Self {
        let channel = Self::with_events_and_gap(events, event_gap);
        channel
            .next_workspace_id
            .store(next_workspace_id, Ordering::SeqCst);
        channel
    }

    fn streaming() -> Self {
        Self::streaming_with_create_workspace_delay(Duration::ZERO)
    }

    fn streaming_with_create_workspace_delay(create_workspace_delay: Duration) -> Self {
        let (tx, rx) = mpsc::unbounded_channel();
        Self {
            events: StdMutex::new(None),
            event_gap: Duration::ZERO,
            live_tx: StdMutex::new(Some(tx)),
            live_rx: StdMutex::new(Some(rx)),
            sent: StdMutex::new(Vec::new()),
            attachments: StdMutex::new(Vec::new()),
            edits: StdMutex::new(Vec::new()),
            deleted: StdMutex::new(Vec::new()),
            deleted_workspaces: StdMutex::new(Vec::new()),
            acks: StdMutex::new(Vec::new()),
            created_workspaces: StdMutex::new(Vec::new()),
            renames: StdMutex::new(Vec::new()),
            command_query_answers: StdMutex::new(Vec::new()),
            downloads: StdMutex::new(HashMap::new()),
            missing_workspaces: StdMutex::new(Vec::new()),
            create_workspace_delay,
            next_message_id: AtomicUsize::new(1),
            next_workspace_id: AtomicUsize::new(1),
        }
    }

    fn mark_workspace_missing(&self, workspace: &str) {
        self.missing_workspaces
            .lock()
            .unwrap()
            .push(workspace.to_string());
    }

    fn push_event(&self, event: ChannelEvent) {
        self.live_tx
            .lock()
            .unwrap()
            .as_ref()
            .expect("streaming channel sender")
            .send(event)
            .expect("streaming channel receiver");
    }

    fn close_events(&self) {
        self.live_tx.lock().unwrap().take();
    }

    fn sent_messages(&self) -> Vec<SentMessage> {
        self.sent.lock().unwrap().clone()
    }

    fn sent_attachments(&self) -> Vec<SentAttachment> {
        self.attachments.lock().unwrap().clone()
    }

    fn edited_messages(&self) -> Vec<SentMessage> {
        self.edits.lock().unwrap().clone()
    }

    fn deleted_messages(&self) -> Vec<String> {
        self.deleted.lock().unwrap().clone()
    }

    fn deleted_workspaces(&self) -> Vec<String> {
        self.deleted_workspaces.lock().unwrap().clone()
    }

    fn acknowledged_topics(&self) -> Vec<String> {
        self.acks
            .lock()
            .unwrap()
            .iter()
            .map(|(_, topic)| topic.clone())
            .collect()
    }

    fn acknowledged_handles(&self) -> Vec<(String, String)> {
        self.acks.lock().unwrap().clone()
    }

    fn created_workspaces(&self) -> Vec<(String, String, String)> {
        self.created_workspaces.lock().unwrap().clone()
    }

    fn renames(&self) -> Vec<(String, String)> {
        self.renames.lock().unwrap().clone()
    }

    fn command_query_answers(&self) -> Vec<(String, Vec<CommandQueryResult>)> {
        self.command_query_answers.lock().unwrap().clone()
    }

    fn is_subscribed(&self) -> bool {
        self.live_rx.lock().unwrap().is_none()
    }
}

#[async_trait]
impl Channel for RecordingChannel {
    fn name(&self) -> &'static str {
        "recording"
    }

    fn message_char_limit(&self) -> usize {
        4096
    }

    async fn send(&self, target: &WorkspaceHandle, msg: OutgoingMessage) -> Result<MessageId> {
        if self
            .missing_workspaces
            .lock()
            .unwrap()
            .iter()
            .any(|ws| ws == target.workspace.as_str())
        {
            return Err(ChannelError::WorkspaceNotFound("TOPIC_ID_INVALID".into()));
        }
        let id = self.next_message_id.fetch_add(1, Ordering::SeqCst);
        let message_id = MessageId::new(format!("sent-{id}"));
        self.sent.lock().unwrap().push(SentMessage {
            id: message_id.as_str().to_string(),
            chat: target.chat.as_str().to_string(),
            topic: target.workspace.as_str().to_string(),
            message: msg,
        });
        Ok(message_id)
    }

    async fn send_attachment(
        &self,
        target: &WorkspaceHandle,
        attachment: ChannelAttachment,
    ) -> Result<MessageId> {
        let id = self.next_message_id.fetch_add(1, Ordering::SeqCst);
        let message_id = MessageId::new(format!("attachment-{id}"));
        self.attachments.lock().unwrap().push(SentAttachment {
            id: message_id.as_str().to_string(),
            chat: target.chat.as_str().to_string(),
            topic: target.workspace.as_str().to_string(),
            attachment,
        });
        Ok(message_id)
    }

    async fn edit(
        &self,
        target: &WorkspaceHandle,
        _id: &MessageId,
        msg: OutgoingMessage,
    ) -> Result<()> {
        self.edits.lock().unwrap().push(SentMessage {
            id: _id.as_str().to_string(),
            chat: target.chat.as_str().to_string(),
            topic: target.workspace.as_str().to_string(),
            message: msg,
        });
        Ok(())
    }

    async fn delete(&self, _target: &WorkspaceHandle, id: &MessageId) -> Result<()> {
        self.deleted.lock().unwrap().push(id.as_str().to_string());
        Ok(())
    }

    async fn create_workspace(&self, parent: &ChatId, title: &str) -> Result<WorkspaceHandle> {
        if !self.create_workspace_delay.is_zero() {
            sleep(self.create_workspace_delay).await;
        }
        let id = self.next_workspace_id.fetch_add(1, Ordering::SeqCst);
        self.created_workspaces.lock().unwrap().push((
            parent.as_str().to_string(),
            title.to_string(),
            id.to_string(),
        ));
        Ok(WorkspaceHandle::new(
            parent.clone(),
            WorkspaceId::new(id.to_string()),
        ))
    }

    async fn probe_workspace(&self, handle: &WorkspaceHandle) -> Result<()> {
        if self
            .missing_workspaces
            .lock()
            .unwrap()
            .iter()
            .any(|ws| ws == handle.workspace.as_str())
        {
            return Err(ChannelError::WorkspaceNotFound(
                "message thread not found".into(),
            ));
        }
        Ok(())
    }

    async fn rename_workspace(&self, handle: &WorkspaceHandle, title: &str) -> Result<()> {
        self.renames
            .lock()
            .unwrap()
            .push((handle.workspace.as_str().to_string(), title.to_string()));
        Ok(())
    }

    async fn delete_workspace(&self, handle: &WorkspaceHandle) -> Result<()> {
        self.deleted_workspaces
            .lock()
            .unwrap()
            .push(handle.workspace.as_str().to_string());
        Ok(())
    }

    fn subscribe(&self) -> BoxStream<'static, ChannelEvent> {
        if let Some(rx) = self.live_rx.lock().unwrap().take() {
            return stream::unfold(rx, |mut rx| async move {
                rx.recv().await.map(|event| (event, rx))
            })
            .boxed();
        }
        let events = self.events.lock().unwrap().take().unwrap_or_default();
        let event_gap = self.event_gap;
        stream::iter(events)
            .then(move |event| {
                let event_gap = event_gap;
                async move {
                    if !event_gap.is_zero() {
                        sleep(event_gap).await;
                    }
                    event
                }
            })
            .boxed()
    }

    async fn download_attachment(&self, att: &IncomingAttachment) -> Result<Vec<u8>> {
        Ok(self
            .downloads
            .lock()
            .unwrap()
            .get(&att.file_ref)
            .cloned()
            .unwrap_or_default())
    }

    async fn acknowledge(&self, target: &WorkspaceHandle) -> Result<()> {
        self.acks.lock().unwrap().push((
            target.chat.as_str().to_string(),
            target.workspace.as_str().to_string(),
        ));
        Ok(())
    }

    async fn send_file(&self, _target: &WorkspaceHandle, _file: FileUpload) -> Result<MessageId> {
        Err(ChannelError::Unsupported("send_file".into()))
    }

    async fn answer_command_query(
        &self,
        query: &CommandQuery,
        results: Vec<CommandQueryResult>,
    ) -> Result<()> {
        self.command_query_answers
            .lock()
            .unwrap()
            .push((query.id.clone(), results));
        Ok(())
    }
}

fn watch_notification_live_replay_cassette_path() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("tests/fixtures/watch_notification_live_replay_codex.json")
}

fn live_watch_recording_provider(provider: &str) -> Option<()> {
    let _ = dotenvy::dotenv();
    if std::env::var("LUCARNE_TELEGRAM_RECORD_WATCH_CASSETTE")
        .ok()
        .as_deref()
        != Some("1")
    {
        return None;
    }
    if let Ok(allowed) = std::env::var("LUCARNE_LIVE_PROVIDERS") {
        let allowed = allowed
            .split(',')
            .map(str::trim)
            .filter(|name| !name.is_empty())
            .collect::<Vec<_>>();
        if !allowed.is_empty() && !allowed.iter().any(|name| *name == provider) {
            return None;
        }
    }
    Some(())
}

fn live_watch_recording_timeout() -> Duration {
    std::env::var("LUCARNE_TELEGRAM_LIVE_TIMEOUT")
        .or_else(|_| std::env::var("LUCARNE_LIVE_TIMEOUT"))
        .ok()
        .and_then(|raw| raw.parse::<u64>().ok())
        .map(Duration::from_secs)
        .unwrap_or_else(|| Duration::from_secs(180))
}

#[derive(Clone, serde::Deserialize, serde::Serialize)]
struct LiveReplayCassette {
    provider_id: String,
    session_ref: String,
    #[serde(default)]
    background_prompt: String,
    #[serde(default)]
    reply_prompt: String,
    background_delay_ms: u64,
    background_events: Vec<Event>,
    submit_events: Vec<Event>,
}

impl LiveReplayCassette {
    fn watch_notification() -> Self {
        let path = watch_notification_live_replay_cassette_path();
        let raw = fs::read_to_string(&path).unwrap_or_else(|err| {
            panic!(
                "missing watch notification live replay cassette {}: {err}",
                path.display()
            )
        });
        serde_json::from_str(&raw).expect("watch notification live replay cassette")
    }

    fn background_delay(&self) -> Duration {
        Duration::from_millis(self.background_delay_ms)
    }
}

struct LiveCassetteRecorder {
    provider_id: &'static str,
    session_ref: StdMutex<Option<String>>,
    submit_prompts: StdMutex<Vec<String>>,
    background_events: StdMutex<Vec<Event>>,
    reply_events: StdMutex<Vec<Event>>,
    current_submit: AtomicUsize,
    completed_submits: AtomicUsize,
}

impl LiveCassetteRecorder {
    fn new(provider_id: &'static str) -> Self {
        Self {
            provider_id,
            session_ref: StdMutex::new(None),
            submit_prompts: StdMutex::new(Vec::new()),
            background_events: StdMutex::new(Vec::new()),
            reply_events: StdMutex::new(Vec::new()),
            current_submit: AtomicUsize::new(0),
            completed_submits: AtomicUsize::new(0),
        }
    }

    fn set_session_ref(&self, session_ref: impl Into<String>) {
        *self.session_ref.lock().unwrap() = Some(session_ref.into());
    }

    fn begin_submit(&self, input: &AgentInput) {
        let mut prompts = self.submit_prompts.lock().unwrap();
        prompts.push(input.text.to_string());
        self.current_submit.store(prompts.len(), Ordering::SeqCst);
    }

    fn record_event(&self, event: &Event) {
        let submit = self.current_submit.load(Ordering::SeqCst);
        match submit {
            1 => self.background_events.lock().unwrap().push(event.clone()),
            2 => self.reply_events.lock().unwrap().push(event.clone()),
            _ => {}
        }
        if submit > 0 && matches!(event, Event::TurnCompleted(_) | Event::TurnFailed(_)) {
            self.completed_submits.store(submit, Ordering::SeqCst);
            self.current_submit.store(0, Ordering::SeqCst);
        }
    }

    fn completed_submits(&self) -> usize {
        self.completed_submits.load(Ordering::SeqCst)
    }

    fn cassette(&self) -> LiveReplayCassette {
        let prompts = self.submit_prompts.lock().unwrap();
        let session_ref = self
            .session_ref
            .lock()
            .unwrap()
            .clone()
            .expect("live recording must capture a provider session ref");
        LiveReplayCassette {
            provider_id: self.provider_id.to_string(),
            session_ref,
            background_prompt: prompts
                .first()
                .cloned()
                .expect("live recording must capture the background prompt"),
            reply_prompt: prompts
                .get(1)
                .cloned()
                .expect("live recording must capture the notification reply prompt"),
            background_delay_ms: 25,
            background_events: self.background_events.lock().unwrap().clone(),
            submit_events: self.reply_events.lock().unwrap().clone(),
        }
    }
}

struct LiveRecordingProvider {
    provider_id: &'static str,
    inner: Arc<dyn AgentProvider>,
    recorder: Arc<LiveCassetteRecorder>,
}

impl LiveRecordingProvider {
    fn new(provider_id: &'static str, inner: Arc<dyn AgentProvider>) -> Self {
        assert_eq!(inner.id().as_str(), provider_id);
        Self {
            provider_id,
            inner,
            recorder: Arc::new(LiveCassetteRecorder::new(provider_id)),
        }
    }

    async fn wait_for_submit_completed(&self, submit: usize) {
        assert!(
            eventually_for(live_watch_recording_timeout(), || {
                self.recorder.completed_submits() >= submit
            })
            .await,
            "live recording submit {submit} did not complete before timeout"
        );
    }

    fn write_watch_notification_cassette(&self, path: &Path) {
        let cassette = self.recorder.cassette();
        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent).expect("create live replay cassette dir");
        }
        let raw = serde_json::to_string_pretty(&cassette).expect("serialize live replay cassette");
        fs::write(path, format!("{raw}\n")).expect("write live replay cassette");
    }
}

#[async_trait]
impl AgentProvider for LiveRecordingProvider {
    fn id(&self) -> ProviderId {
        ProviderId::from_static(self.provider_id)
    }

    fn capabilities(&self) -> AgentCapabilities {
        self.inner.capabilities()
    }

    async fn probe(&self) -> std::result::Result<ProbeResult, AgentError> {
        self.inner.probe().await
    }

    async fn open(
        &self,
        req: OpenSession,
    ) -> std::result::Result<Box<dyn AgentSession>, AgentError> {
        let session = self.inner.open(req).await?;
        if let Some(session_ref) = session.provider_session_id().await {
            self.recorder.set_session_ref(session_ref.0.to_string());
        } else {
            self.recorder.set_session_ref(session.id().0.to_string());
        }
        Ok(Box::new(LiveRecordingSession {
            inner: session,
            recorder: Arc::clone(&self.recorder),
        }))
    }

    async fn resume(
        &self,
        req: ResumeSession,
    ) -> std::result::Result<Box<dyn AgentSession>, AgentError> {
        self.recorder.set_session_ref(req.session_ref.0.to_string());
        let session = self.inner.resume(req).await?;
        Ok(Box::new(LiveRecordingSession {
            inner: session,
            recorder: Arc::clone(&self.recorder),
        }))
    }
}

struct LiveRecordingSession {
    inner: Box<dyn AgentSession>,
    recorder: Arc<LiveCassetteRecorder>,
}

#[async_trait]
impl AgentSession for LiveRecordingSession {
    fn id(&self) -> &SessionId {
        self.inner.id()
    }

    fn instance_id(&self) -> &InstanceId {
        self.inner.instance_id()
    }

    fn provider_id(&self) -> ProviderId {
        self.inner.provider_id()
    }

    async fn provider_session_id(&self) -> Option<SessionId> {
        self.inner.provider_session_id().await
    }

    async fn submit(&self, input: AgentInput) -> std::result::Result<(), AgentError> {
        self.recorder.begin_submit(&input);
        let result = self.inner.submit(input).await;
        if let Some(session_ref) = self.inner.provider_session_id().await {
            self.recorder.set_session_ref(session_ref.0.to_string());
        }
        if result.is_err() {
            self.recorder.current_submit.store(0, Ordering::SeqCst);
        }
        result
    }

    async fn interrupt(&self) -> std::result::Result<(), AgentError> {
        self.inner.interrupt().await
    }

    async fn resolve(
        &self,
        req_id: &str,
        response: InterventionResponse,
    ) -> std::result::Result<(), AgentError> {
        self.inner.resolve(req_id, response).await
    }

    async fn take_events(&self) -> std::result::Result<AgentEventStream, AgentError> {
        let mut inner = self.inner.take_events().await?;
        let recorder = Arc::clone(&self.recorder);
        let (tx, rx) = mpsc::channel(32);
        tokio::spawn(async move {
            while let Some(event) = inner.recv().await {
                recorder.record_event(&event);
                if tx.send(event).await.is_err() {
                    break;
                }
            }
        });
        Ok(rx)
    }

    async fn close(&self) -> std::result::Result<(), AgentError> {
        self.inner.close().await
    }
}

struct LiveReplayProvider {
    cassette: LiveReplayCassette,
    submit_calls: Arc<StdMutex<Vec<(String, String)>>>,
}

impl LiveReplayProvider {
    fn from_cassette(cassette: LiveReplayCassette) -> Self {
        Self {
            cassette,
            submit_calls: Arc::new(StdMutex::new(Vec::new())),
        }
    }

    fn submit_calls(&self) -> Vec<(String, String)> {
        self.submit_calls.lock().unwrap().clone()
    }

    fn session(&self) -> LiveReplaySession {
        LiveReplaySession::new(self.cassette.clone(), Arc::clone(&self.submit_calls))
    }
}

#[async_trait]
impl AgentProvider for LiveReplayProvider {
    fn id(&self) -> ProviderId {
        assert_eq!(self.cassette.provider_id, "codex");
        ProviderId::from_static("codex")
    }

    fn capabilities(&self) -> AgentCapabilities {
        AgentCapabilities::default()
    }

    async fn probe(&self) -> std::result::Result<ProbeResult, AgentError> {
        Ok(ProbeResult {
            provider_id: self.id(),
            provider_version: Some("live-replay".into()),
            capabilities: self.capabilities(),
        })
    }

    async fn open(
        &self,
        _req: OpenSession,
    ) -> std::result::Result<Box<dyn AgentSession>, AgentError> {
        Ok(Box::new(self.session()))
    }

    async fn resume(
        &self,
        req: ResumeSession,
    ) -> std::result::Result<Box<dyn AgentSession>, AgentError> {
        assert_eq!(
            req.session_ref.0.as_str(),
            self.cassette.session_ref.as_str()
        );
        Ok(Box::new(self.session()))
    }
}

struct LiveReplaySession {
    cassette: LiveReplayCassette,
    id: SessionId,
    instance_id: InstanceId,
    submit_calls: Arc<StdMutex<Vec<(String, String)>>>,
    tx: mpsc::Sender<Event>,
    rx: TokioMutex<Option<AgentEventStream>>,
}

impl LiveReplaySession {
    fn new(
        cassette: LiveReplayCassette,
        submit_calls: Arc<StdMutex<Vec<(String, String)>>>,
    ) -> Self {
        let (tx, rx) = mpsc::channel(16);
        Self {
            id: SessionId(cassette.session_ref.clone().into()),
            instance_id: InstanceId(format!("instance-{}", cassette.session_ref).into()),
            cassette,
            submit_calls,
            tx,
            rx: TokioMutex::new(Some(rx)),
        }
    }

    async fn send_event(&self, event: Event) -> std::result::Result<(), AgentError> {
        self.tx.send(event).await.map_err(|_| AgentError {
            kind: AgentErrorKind::InvalidState,
            message: "event stream closed".into(),
        })
    }
}

#[async_trait]
impl AgentSession for LiveReplaySession {
    fn id(&self) -> &SessionId {
        &self.id
    }

    fn instance_id(&self) -> &InstanceId {
        &self.instance_id
    }

    fn provider_id(&self) -> ProviderId {
        assert_eq!(self.cassette.provider_id, "codex");
        ProviderId::from_static("codex")
    }

    async fn submit(&self, input: AgentInput) -> std::result::Result<(), AgentError> {
        self.submit_calls
            .lock()
            .unwrap()
            .push((self.id.0.to_string(), input.text.to_string()));
        for event in self.cassette.submit_events.clone() {
            self.send_event(event).await?;
        }
        Ok(())
    }

    async fn interrupt(&self) -> std::result::Result<(), AgentError> {
        Ok(())
    }

    async fn resolve(
        &self,
        _req_id: &str,
        _response: InterventionResponse,
    ) -> std::result::Result<(), AgentError> {
        Ok(())
    }

    async fn take_events(&self) -> std::result::Result<AgentEventStream, AgentError> {
        let rx = self.rx.lock().await.take().ok_or_else(|| AgentError {
            kind: AgentErrorKind::InvalidState,
            message: "events already taken".into(),
        })?;
        let tx = self.tx.clone();
        let delay = self.cassette.background_delay();
        let events = self.cassette.background_events.clone();
        tokio::spawn(async move {
            if !delay.is_zero() {
                sleep(delay).await;
            }
            for event in events {
                let _ = tx.send(event).await;
            }
        });
        Ok(rx)
    }

    async fn close(&self) -> std::result::Result<(), AgentError> {
        Ok(())
    }
}

struct ProviderProbe {
    inputs: Arc<StdMutex<Vec<AgentInput>>>,
    submit_calls: Arc<StdMutex<Vec<(String, String)>>>,
    session_txs: Arc<StdMutex<HashMap<String, mpsc::Sender<Event>>>>,
    opens: Arc<AtomicUsize>,
    open_args: Arc<StdMutex<Vec<serde_json::Value>>>,
    resumes: Arc<StdMutex<Vec<String>>>,
    resume_args: Arc<StdMutex<Vec<serde_json::Value>>>,
    resume_error: Option<String>,
    lifecycle_calls: Arc<StdMutex<Vec<(String, String)>>>,
    command_catalog: AgentCommandCatalog,
    model_catalog: Option<AgentModelCatalog>,
    model_state: Arc<StdMutex<(String, Option<String>)>>,
    model_selections: Arc<StdMutex<Vec<AgentModelSelection>>>,
    permission_catalog: Option<AgentPermissionCatalog>,
    permission_state: Arc<StdMutex<String>>,
    permission_selections: Arc<StdMutex<Vec<AgentPermissionSelection>>>,
    skills: AgentSkillCatalog,
    status: AgentStatus,
    status_calls: Arc<AtomicUsize>,
    process_id: Option<i32>,
    fork_targets: Vec<AgentForkTarget>,
    fork_selections: Arc<StdMutex<Vec<String>>>,
    fork_calls: Arc<StdMutex<Vec<(String, String)>>>,
    submit_events: Vec<Event>,
    auto_complete_turn: bool,
    resolved_interventions: Arc<StdMutex<Vec<(String, InterventionResponse)>>>,
}

impl Default for ProviderProbe {
    fn default() -> Self {
        Self {
            inputs: Arc::new(StdMutex::new(Vec::new())),
            submit_calls: Arc::new(StdMutex::new(Vec::new())),
            session_txs: Arc::new(StdMutex::new(HashMap::new())),
            opens: Arc::new(AtomicUsize::new(0)),
            open_args: Arc::new(StdMutex::new(Vec::new())),
            resumes: Arc::new(StdMutex::new(Vec::new())),
            resume_args: Arc::new(StdMutex::new(Vec::new())),
            resume_error: None,
            lifecycle_calls: Arc::new(StdMutex::new(Vec::new())),
            command_catalog: AgentCommandCatalog::default(),
            model_catalog: None,
            model_state: Arc::new(StdMutex::new((String::new(), None))),
            model_selections: Arc::new(StdMutex::new(Vec::new())),
            permission_catalog: None,
            permission_state: Arc::new(StdMutex::new(String::new())),
            permission_selections: Arc::new(StdMutex::new(Vec::new())),
            skills: AgentSkillCatalog::default(),
            status: AgentStatus::default(),
            status_calls: Arc::new(AtomicUsize::new(0)),
            process_id: None,
            fork_targets: Vec::new(),
            fork_selections: Arc::new(StdMutex::new(Vec::new())),
            fork_calls: Arc::new(StdMutex::new(Vec::new())),
            submit_events: Vec::new(),
            auto_complete_turn: true,
            resolved_interventions: Arc::new(StdMutex::new(Vec::new())),
        }
    }
}

impl ProviderProbe {
    fn with_command_catalog(command_catalog: AgentCommandCatalog) -> Self {
        Self {
            command_catalog,
            ..Self::default()
        }
    }

    fn with_skills(skills: AgentSkillCatalog) -> Self {
        Self {
            skills,
            ..Self::default()
        }
    }

    fn with_status(status: AgentStatus) -> Self {
        Self {
            status,
            ..Self::default()
        }
    }

    fn with_process_id(process_id: i32) -> Self {
        Self {
            process_id: Some(process_id),
            ..Self::default()
        }
    }

    fn with_models(model_catalog: AgentModelCatalog) -> Self {
        let current_model = model_catalog
            .current_model
            .as_deref()
            .unwrap_or_default()
            .to_string();
        let current_reasoning = model_catalog
            .current_reasoning
            .as_deref()
            .map(str::to_string);
        Self {
            model_catalog: Some(model_catalog),
            model_state: Arc::new(StdMutex::new((current_model, current_reasoning))),
            ..Self::default()
        }
    }

    fn with_permissions(permission_catalog: AgentPermissionCatalog) -> Self {
        let current_mode = permission_catalog
            .current_mode
            .as_deref()
            .unwrap_or_default()
            .to_string();
        Self {
            permission_catalog: Some(permission_catalog),
            permission_state: Arc::new(StdMutex::new(current_mode)),
            ..Self::default()
        }
    }

    fn with_fork_targets(fork_targets: Vec<AgentForkTarget>) -> Self {
        Self {
            fork_targets,
            ..Self::default()
        }
    }

    fn with_submit_events(submit_events: Vec<Event>) -> Self {
        Self {
            submit_events,
            ..Self::default()
        }
    }

    fn with_pending_submit_events(submit_events: Vec<Event>) -> Self {
        Self {
            submit_events,
            auto_complete_turn: false,
            ..Self::default()
        }
    }

    fn with_resume_error(error: &str) -> Self {
        Self {
            resume_error: Some(error.to_string()),
            ..Self::default()
        }
    }

    fn inputs(&self) -> Vec<AgentInput> {
        self.inputs.lock().unwrap().clone()
    }

    fn submit_calls(&self) -> Vec<(String, String)> {
        self.submit_calls.lock().unwrap().clone()
    }

    async fn emit_to_session(&self, session_id: &str, event: Event) {
        let tx = self
            .session_txs
            .lock()
            .unwrap()
            .get(session_id)
            .cloned()
            .unwrap_or_else(|| panic!("missing recording session {session_id}"));
        tx.send(event)
            .await
            .expect("recording session event stream");
    }

    fn resumes(&self) -> Vec<String> {
        self.resumes.lock().unwrap().clone()
    }

    fn opens(&self) -> usize {
        self.opens.load(Ordering::SeqCst)
    }

    fn open_args(&self) -> Vec<serde_json::Value> {
        self.open_args.lock().unwrap().clone()
    }

    fn resume_args(&self) -> Vec<serde_json::Value> {
        self.resume_args.lock().unwrap().clone()
    }

    fn lifecycle_calls(&self) -> Vec<(String, String)> {
        self.lifecycle_calls.lock().unwrap().clone()
    }

    fn fork_selections(&self) -> Vec<String> {
        self.fork_selections.lock().unwrap().clone()
    }

    fn fork_calls(&self) -> Vec<(String, String)> {
        self.fork_calls.lock().unwrap().clone()
    }

    fn model_selections(&self) -> Vec<AgentModelSelection> {
        self.model_selections.lock().unwrap().clone()
    }

    fn permission_selections(&self) -> Vec<AgentPermissionSelection> {
        self.permission_selections.lock().unwrap().clone()
    }

    fn resolved_interventions(&self) -> Vec<(String, InterventionResponse)> {
        self.resolved_interventions.lock().unwrap().clone()
    }

    fn status_calls(&self) -> usize {
        self.status_calls.load(Ordering::SeqCst)
    }

    fn recording_session(&self, session_id: &str) -> RecordingSession {
        RecordingSession::new(
            session_id,
            Arc::clone(&self.inputs),
            Arc::clone(&self.submit_calls),
            Arc::clone(&self.session_txs),
            Arc::clone(&self.lifecycle_calls),
            self.command_catalog.clone(),
            self.model_catalog.clone(),
            Arc::clone(&self.model_state),
            Arc::clone(&self.model_selections),
            self.permission_catalog.clone(),
            Arc::clone(&self.permission_state),
            Arc::clone(&self.permission_selections),
            self.skills.clone(),
            self.status.clone(),
            Arc::clone(&self.status_calls),
            self.process_id,
            self.fork_targets.clone(),
            Arc::clone(&self.fork_selections),
            Arc::clone(&self.fork_calls),
            self.submit_events.clone(),
            self.auto_complete_turn,
            Arc::clone(&self.resolved_interventions),
        )
    }
}

#[async_trait]
impl AgentProvider for ProviderProbe {
    fn id(&self) -> ProviderId {
        ProviderId::from_static("codex")
    }

    fn capabilities(&self) -> AgentCapabilities {
        AgentCapabilities::default()
    }

    async fn probe(&self) -> std::result::Result<ProbeResult, AgentError> {
        Ok(ProbeResult {
            provider_id: self.id(),
            provider_version: Some("test".into()),
            capabilities: self.capabilities(),
        })
    }

    async fn open(
        &self,
        req: OpenSession,
    ) -> std::result::Result<Box<dyn AgentSession>, AgentError> {
        self.opens.fetch_add(1, Ordering::SeqCst);
        self.open_args.lock().unwrap().push(req.args);
        Ok(Box::new(self.recording_session("session-test")))
    }

    async fn resume(
        &self,
        req: ResumeSession,
    ) -> std::result::Result<Box<dyn AgentSession>, AgentError> {
        self.resumes
            .lock()
            .unwrap()
            .push(req.session_ref.0.to_string());
        self.resume_args.lock().unwrap().push(req.args);
        if let Some(error) = self.resume_error.as_ref() {
            return Err(AgentError {
                kind: AgentErrorKind::Internal,
                message: error.clone().into(),
            });
        }
        Ok(Box::new(self.recording_session(req.session_ref.0.as_str())))
    }
}

struct RecordingSession {
    id: SessionId,
    instance_id: InstanceId,
    inputs: Arc<StdMutex<Vec<AgentInput>>>,
    submit_calls: Arc<StdMutex<Vec<(String, String)>>>,
    lifecycle_calls: Arc<StdMutex<Vec<(String, String)>>>,
    command_catalog: AgentCommandCatalog,
    model_catalog: Option<AgentModelCatalog>,
    model_state: Arc<StdMutex<(String, Option<String>)>>,
    model_selections: Arc<StdMutex<Vec<AgentModelSelection>>>,
    permission_catalog: Option<AgentPermissionCatalog>,
    permission_state: Arc<StdMutex<String>>,
    permission_selections: Arc<StdMutex<Vec<AgentPermissionSelection>>>,
    skills: AgentSkillCatalog,
    status: AgentStatus,
    status_calls: Arc<AtomicUsize>,
    process_id: Option<i32>,
    fork_targets: Vec<AgentForkTarget>,
    fork_selections: Arc<StdMutex<Vec<String>>>,
    fork_calls: Arc<StdMutex<Vec<(String, String)>>>,
    submit_events: Vec<Event>,
    auto_complete_turn: bool,
    resolved_interventions: Arc<StdMutex<Vec<(String, InterventionResponse)>>>,
    tx: mpsc::Sender<Event>,
    rx: TokioMutex<Option<AgentEventStream>>,
}

impl RecordingSession {
    fn new(
        session_id: &str,
        inputs: Arc<StdMutex<Vec<AgentInput>>>,
        submit_calls: Arc<StdMutex<Vec<(String, String)>>>,
        session_txs: Arc<StdMutex<HashMap<String, mpsc::Sender<Event>>>>,
        lifecycle_calls: Arc<StdMutex<Vec<(String, String)>>>,
        command_catalog: AgentCommandCatalog,
        model_catalog: Option<AgentModelCatalog>,
        model_state: Arc<StdMutex<(String, Option<String>)>>,
        model_selections: Arc<StdMutex<Vec<AgentModelSelection>>>,
        permission_catalog: Option<AgentPermissionCatalog>,
        permission_state: Arc<StdMutex<String>>,
        permission_selections: Arc<StdMutex<Vec<AgentPermissionSelection>>>,
        skills: AgentSkillCatalog,
        status: AgentStatus,
        status_calls: Arc<AtomicUsize>,
        process_id: Option<i32>,
        fork_targets: Vec<AgentForkTarget>,
        fork_selections: Arc<StdMutex<Vec<String>>>,
        fork_calls: Arc<StdMutex<Vec<(String, String)>>>,
        submit_events: Vec<Event>,
        auto_complete_turn: bool,
        resolved_interventions: Arc<StdMutex<Vec<(String, InterventionResponse)>>>,
    ) -> Self {
        let (tx, rx) = mpsc::channel(16);
        session_txs
            .lock()
            .unwrap()
            .insert(session_id.to_string(), tx.clone());
        Self {
            id: SessionId(session_id.into()),
            instance_id: InstanceId(format!("instance-{session_id}").into()),
            inputs,
            submit_calls,
            lifecycle_calls,
            command_catalog,
            model_catalog,
            model_state,
            model_selections,
            permission_catalog,
            permission_state,
            permission_selections,
            skills,
            status,
            status_calls,
            process_id,
            fork_targets,
            fork_selections,
            fork_calls,
            submit_events,
            auto_complete_turn,
            resolved_interventions,
            tx,
            rx: TokioMutex::new(Some(rx)),
        }
    }

    async fn send_event(&self, event: Event) -> std::result::Result<(), AgentError> {
        self.tx.send(event).await.map_err(|_| AgentError {
            kind: AgentErrorKind::InvalidState,
            message: "event stream closed".into(),
        })
    }
}

fn unsupported_probe_command(action: &str) -> AgentError {
    AgentError {
        kind: AgentErrorKind::Unsupported,
        message: format!("codex session does not support {action}").into(),
    }
}

#[async_trait]
impl AgentSession for RecordingSession {
    fn id(&self) -> &SessionId {
        &self.id
    }

    fn instance_id(&self) -> &InstanceId {
        &self.instance_id
    }

    fn provider_id(&self) -> ProviderId {
        ProviderId::from_static("codex")
    }

    fn process_id(&self) -> Option<i32> {
        self.process_id
    }

    async fn submit(&self, input: AgentInput) -> std::result::Result<(), AgentError> {
        self.submit_calls
            .lock()
            .unwrap()
            .push((self.id.0.to_string(), input.text.to_string()));
        self.inputs.lock().unwrap().push(input);
        for event in self.submit_events.clone() {
            self.send_event(event).await?;
        }
        if self.auto_complete_turn {
            self.send_event(Event::Message(MessageEvent {
                role: MessageRole::Assistant,
                text: "ok".into(),
                streaming: false,
            }))
            .await?;
            self.send_event(Event::TurnCompleted(
                lucarne::agent_runtime::events::TurnCompletedEvent {
                    turn_id: "turn-test".into(),
                    usage: None,
                },
            ))
            .await?;
        }
        Ok(())
    }

    async fn list_commands(&self) -> std::result::Result<AgentCommandCatalog, AgentError> {
        Ok(self.command_catalog.clone())
    }

    async fn list_models(&self) -> std::result::Result<AgentModelCatalog, AgentError> {
        let Some(mut catalog) = self.model_catalog.clone() else {
            return Err(unsupported_probe_command("list_models"));
        };
        let (model, reasoning) = self.model_state.lock().unwrap().clone();
        if !model.is_empty() {
            catalog.current_model = Some(model.into());
            catalog.current_reasoning = reasoning.map(Into::into);
        }
        Ok(catalog)
    }

    async fn set_model(
        &self,
        selection: AgentModelSelection,
    ) -> std::result::Result<AgentStatus, AgentError> {
        self.model_selections
            .lock()
            .unwrap()
            .push(selection.clone());
        *self.model_state.lock().unwrap() = (
            selection.model.to_string(),
            selection.reasoning.as_deref().map(str::to_string),
        );
        Ok(AgentStatus {
            model: Some(selection.model),
            reasoning: selection.reasoning,
            ..Default::default()
        })
    }

    async fn list_permissions(&self) -> std::result::Result<AgentPermissionCatalog, AgentError> {
        let Some(mut catalog) = self.permission_catalog.clone() else {
            return Err(unsupported_probe_command("list_permissions"));
        };
        let mode = self.permission_state.lock().unwrap().clone();
        if !mode.is_empty() {
            catalog.current_mode = Some(mode.into());
        }
        Ok(catalog)
    }

    async fn set_permissions(
        &self,
        selection: AgentPermissionSelection,
    ) -> std::result::Result<AgentStatus, AgentError> {
        self.permission_selections
            .lock()
            .unwrap()
            .push(selection.clone());
        *self.permission_state.lock().unwrap() = selection.mode.to_string();
        Ok(AgentStatus {
            permissions: Some(selection.mode.to_string().into()),
            ..Default::default()
        })
    }

    async fn list_skills(&self) -> std::result::Result<AgentSkillCatalog, AgentError> {
        Ok(self.skills.clone())
    }

    async fn new(&self) -> std::result::Result<(), AgentError> {
        self.lifecycle_calls
            .lock()
            .unwrap()
            .push((self.id.0.to_string(), "new".to_string()));
        Ok(())
    }

    async fn quit(&self) -> std::result::Result<(), AgentError> {
        self.lifecycle_calls
            .lock()
            .unwrap()
            .push((self.id.0.to_string(), "quit".to_string()));
        Ok(())
    }

    async fn status(&self) -> std::result::Result<AgentStatus, AgentError> {
        self.status_calls.fetch_add(1, Ordering::SeqCst);
        let mut status = self.status.clone();
        let (model, reasoning) = self.model_state.lock().unwrap().clone();
        if !model.is_empty() {
            status.model = Some(model.into());
            status.reasoning = reasoning.map(Into::into);
        }
        let permissions = self.permission_state.lock().unwrap().clone();
        if !permissions.is_empty() {
            status.permissions = Some(permissions.into());
        }
        Ok(status)
    }

    async fn list_fork_targets(&self) -> std::result::Result<AgentForkTargetCatalog, AgentError> {
        Ok(AgentForkTargetCatalog {
            targets: self.fork_targets.clone(),
        })
    }

    async fn fork(
        &self,
        selection: AgentForkSelection,
    ) -> std::result::Result<AgentForkResult, AgentError> {
        self.fork_selections
            .lock()
            .unwrap()
            .push(selection.target_id.to_string());
        self.fork_calls
            .lock()
            .unwrap()
            .push((self.id.0.to_string(), selection.target_id.to_string()));
        Ok(AgentForkResult {
            session_ref: Some(SessionRef("fork-session".into())),
            source_session_ref: Some(SessionRef("session-test".into())),
        })
    }

    async fn interrupt(&self) -> std::result::Result<(), AgentError> {
        self.lifecycle_calls
            .lock()
            .unwrap()
            .push((self.id.0.to_string(), "interrupt".to_string()));
        Ok(())
    }

    async fn resolve(
        &self,
        req_id: &str,
        response: InterventionResponse,
    ) -> std::result::Result<(), AgentError> {
        self.resolved_interventions
            .lock()
            .unwrap()
            .push((req_id.to_string(), response));
        self.send_event(Event::Message(MessageEvent {
            role: MessageRole::Assistant,
            text: "resolved".into(),
            streaming: false,
        }))
        .await?;
        self.send_event(Event::TurnCompleted(
            lucarne::agent_runtime::events::TurnCompletedEvent {
                turn_id: "turn-resolved".into(),
                usage: None,
            },
        ))
        .await?;
        Ok(())
    }

    async fn take_events(&self) -> std::result::Result<AgentEventStream, AgentError> {
        self.rx.lock().await.take().ok_or_else(|| AgentError {
            kind: AgentErrorKind::InvalidState,
            message: "events already taken".into(),
        })
    }

    async fn close(&self) -> std::result::Result<(), AgentError> {
        Ok(())
    }
}
