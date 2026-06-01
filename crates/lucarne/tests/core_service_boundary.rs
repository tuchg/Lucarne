use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use lucarne::{
    agent_runtime::{
        events::TurnCompletedEvent, AgentError, AgentErrorKind, AgentEventStream, AgentInput,
        AgentProvider, AgentSession, Event, InstanceId, InterventionResponse, MessageEvent,
        MessageRole, OpenSession, ProbeResult, ResumeSession, SessionId,
    },
    control_plane::{ControlPlaneSqliteStore, ScheduledTaskId, TurnSource, WorkspaceId},
    core_service::{
        CoreEvent, DaemonApi, OpenWorkspaceRequest, ResumeWorkspaceRequest,
        RunDueScheduledTasksRequest, UpsertScheduledTaskRequest,
    },
    LucarneCore, ProviderId,
};
use rusqlite::Connection;
use smol_str::SmolStr;
use tempfile::TempDir;
use tokio::time::{timeout, Duration};

#[test]
fn core_service_does_not_write_sqlite_while_holding_state_mutex() {
    let service = std::fs::read_to_string(
        std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("src/core_service/service.rs"),
    )
    .expect("read core service source");
    for forbidden in [
        "replace_non_timeline_entities(state.persistence_entities_without_timeline())",
        "upsert_entities(state.persistence_entities_for_timeline_item(item))",
        "persist_non_timeline_entities(&state)",
        "persist_timeline_item(&state",
    ] {
        assert!(
            !service.contains(forbidden),
            "core service must snapshot persistence entities before SQLite writes: {forbidden}"
        );
    }
    assert!(
        !service.contains("tokio::sync::Mutex<HashMap<WorkspaceId, Arc<dyn AgentSession>>>"),
        "live session registry should not use an async mutex for in-memory map access"
    );
    assert!(
        service.contains("state: Arc<RwLock<ControlPlaneState>>"),
        "core control-plane cache should allow concurrent read-only daemon queries"
    );
    assert!(
        !service.contains("state: Mutex<ControlPlaneState>"),
        "core control-plane cache should not serialize read-only queries behind one Mutex"
    );
}

#[test]
fn core_service_submit_turn_moves_owned_input_without_cloning() {
    let service = std::fs::read_to_string(
        std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("src/core_service/service.rs"),
    )
    .expect("read core service source");

    assert!(
        !service.contains("live.submit(req.input.clone()).await?"),
        "SubmitTurnRequest is owned, so submit_turn should move AgentInput instead of cloning it"
    );
}

#[test]
fn core_service_matches_history_watch_events_at_consumption_site() {
    let service = std::fs::read_to_string(
        std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("src/core_service/service.rs"),
    )
    .expect("read core service source");

    for forbidden in [
        "fn project_history_watch_event",
        "fn project_history_watch_live_completion_event",
        "fn project_history_watch_live_failure_event",
    ] {
        assert!(
            !service.contains(forbidden),
            "history watch should match WatchEvent at the consumption site instead of keeping a projection helper: {forbidden}"
        );
    }
}

#[test]
fn core_history_watch_uses_message_selection_instead_of_full_parse() {
    let service = std::fs::read_to_string(
        std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("src/core_service/service.rs"),
    )
    .expect("read core service source");

    assert!(
        service.contains("fn history_watch_selection() -> ParseSelection"),
        "core service should centralize the lean history watch parse selection"
    );
    assert!(
        service.contains("ParseSelection::empty().with_meta().with_messages()"),
        "history watch only needs metadata and message/lifecycle records for notifications"
    );
    assert!(
        service.contains(".selection(history_watch_selection())"),
        "default history watch must not use WatchConfig::new()'s full parse selection"
    );
}

#[test]
fn core_service_record_live_workspace_moves_owned_open_request() {
    let service = std::fs::read_to_string(
        std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("src/core_service/service.rs"),
    )
    .expect("read core service source");

    assert!(
        !service.contains("record_workspace_with_id(workspace_id, req.clone())"),
        "OpenWorkspaceRequest is owned by record_live_workspace and should be moved into state"
    );
}

#[test]
fn core_service_moves_owned_workspace_project_path_without_cloning() {
    let service = std::fs::read_to_string(
        std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("src/core_service/service.rs"),
    )
    .expect("read core service source");

    assert!(
        !service.contains(".project_path\n            .clone()\n            .unwrap_or_else(default_project_path)"),
        "OpenWorkspaceRequest is owned by core service write paths; project_path should be moved, not cloned"
    );
}

#[test]
fn core_daemon_boundaries_emit_structured_tracing() {
    let manifest = std::path::Path::new(env!("CARGO_MANIFEST_DIR"));
    let service =
        std::fs::read_to_string(manifest.join("src/core_service/service.rs")).expect("service");
    for needle in [
        "#[instrument",
        "lucarne::core_service",
        "open_workspace_with_id_and_events",
        "resume_workspace_with_events",
        "submit_turn",
        "spawn_event_pump",
    ] {
        assert!(
            service.contains(needle),
            "core service tracing must cover daemon boundary: {needle}"
        );
    }

    let store =
        std::fs::read_to_string(manifest.join("src/control_plane/store.rs")).expect("store");
    for needle in [
        "lucarne::control_plane::store",
        "control-plane sqlite opened",
        "control-plane entities upserted",
        "control-plane non-timeline entities replaced",
    ] {
        assert!(
            store.contains(needle),
            "control-plane store tracing must cover persistence boundary: {needle}"
        );
    }

    let history_index =
        std::fs::read_to_string(manifest.join("src/history/index.rs")).expect("history index");
    for needle in [
        "lucarne::history::index",
        "history index cache invalidated",
        "history index page served",
    ] {
        assert!(
            history_index.contains(needle),
            "history index tracing must cover cache boundary: {needle}"
        );
    }

    let events =
        std::fs::read_to_string(manifest.join("src/core_service/events.rs")).expect("events");
    for needle in [
        "lucarne::core_service::events",
        "workspace event stream created",
        "workspace event stream lagged",
    ] {
        assert!(
            events.contains(needle),
            "core event stream tracing must cover daemon event delivery: {needle}"
        );
    }
}

#[test]
fn core_open_sqlite_has_memory_profile_snapshots_for_startup_phases() {
    let manifest = std::path::Path::new(env!("CARGO_MANIFEST_DIR"));
    let service =
        std::fs::read_to_string(manifest.join("src/core_service/service.rs")).expect("service");

    for label in [
        "lucarne.core.open_sqlite.start",
        "lucarne.core.open_sqlite.after_runtime_new",
        "lucarne.core.open_sqlite.after_register_defaults",
        "lucarne.core.open_sqlite.after_store_open",
        "lucarne.core.from_runtime_and_store.start",
        "lucarne.core.from_runtime_and_store.after_load_control_plane",
        "lucarne.core.from_runtime_and_store.after_provider_ids",
        "lucarne.core.start_history_session_watch.start",
        "lucarne.core.start_history_session_watch.after_initial_watcher",
        "lucarne.core.start_history_watcher_once.start",
        "lucarne.core.start_history_watcher_once.after_watcher_start",
    ] {
        assert!(service.contains(label), "missing snapshot {label}");
    }
}

#[test]
fn adapter_supervisor_starts_history_watch_after_adapter_subscriptions() {
    let manifest = std::path::Path::new(env!("CARGO_MANIFEST_DIR"));
    let adapter_lib = std::fs::read_to_string(manifest.join("../lucarne-adapter/src/lib.rs"))
        .expect("adapter supervisor source");
    let adapter_source = &adapter_lib;
    let telegram_bot = std::fs::read_to_string(manifest.join("../lucarne-telegram/src/bot.rs"))
        .expect("telegram bot");
    let telegram_source = telegram_bot
        .split("#[cfg(test)]")
        .next()
        .unwrap_or(&telegram_bot);

    assert!(
        adapter_source.contains("start_history_watch_after_core_subscriber"),
        "adapter supervisor should own global history-watch startup"
    );
    assert!(
        adapter_source.contains("core.has_event_subscribers()"),
        "history watch startup must wait until an adapter has subscribed to core events"
    );
    assert!(
        adapter_source.contains("core.start_history_session_watch()"),
        "supervisor should start the global history watcher after subscription"
    );
    assert!(
        !telegram_source.contains("start_history_session_watch()"),
        "Telegram must not own global history-watch startup"
    );
}

#[test]
fn lucarned_does_not_start_history_watch_before_adapters_subscribe() {
    let manifest = std::path::Path::new(env!("CARGO_MANIFEST_DIR"));
    let lucarned_main =
        std::fs::read_to_string(manifest.join("../lucarned/src/main.rs")).expect("lucarned main");

    assert!(
        !lucarned_main.contains("start_history_session_watch()"),
        "lucarned must not start the history watcher before adapter-owned core event subscribers exist"
    );
}

#[test]
fn core_workspace_event_streams_use_workspace_scoped_broadcasts() {
    let service = std::fs::read_to_string(
        std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("src/core_service/service.rs"),
    )
    .expect("read core service source");

    assert!(
        service.contains("workspace_events: RwLock<HashMap<WorkspaceId, broadcast::Sender<AgentEvent>>>"),
        "workspace event streams should have their own broadcast sender instead of filtering the global daemon stream"
    );
    assert!(
        service.contains("CoreWorkspaceEventStream::from_workspace_events"),
        "core should subscribe workspace streams to the workspace-scoped event sender"
    );
    assert!(
        !service.contains("CoreWorkspaceEventStream::new(workspace_id, self.watch_events())"),
        "workspace streams must not subscribe to the global CoreEvent stream and locally filter every event"
    );
}

#[test]
fn control_plane_state_emits_structured_tracing() {
    let state = std::fs::read_to_string(
        std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("src/control_plane/state.rs"),
    )
    .expect("control-plane state");
    for needle in [
        "lucarne::control_plane::state",
        "workspace upserted",
        "live instance attached",
        "turn started",
        "turn completed",
        "timeline item appended",
    ] {
        assert!(
            state.contains(needle),
            "control-plane state tracing must cover core state machine transition: {needle}"
        );
    }
}

#[test]
fn provider_dialect_modules_emit_structured_tracing() {
    let manifest = std::path::Path::new(env!("CARGO_MANIFEST_DIR"));
    for (file, target) in [
        ("src/dialects/claude.rs", "lucarne::dialects::claude"),
        ("src/dialects/copilot.rs", "lucarne::dialects::copilot"),
        ("src/dialects/pi_rpc.rs", "lucarne::dialects::pi_rpc"),
    ] {
        let source = std::fs::read_to_string(manifest.join(file)).expect("dialect source");
        assert!(
            source.contains(target),
            "{file} must emit structured tracing for provider protocol translation"
        );
    }
}

#[test]
fn daemon_facade_emits_structured_tracing() {
    let daemon = std::fs::read_to_string(
        std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("src/daemon.rs"),
    )
    .expect("daemon facade");
    for needle in [
        "lucarne::daemon",
        "opening sqlite daemon",
        "daemon core attached",
    ] {
        assert!(
            daemon.contains(needle),
            "daemon facade tracing must cover process composition boundary: {needle}"
        );
    }
}

#[test]
fn agent_adapter_modules_emit_structured_tracing() {
    let manifest = std::path::Path::new(env!("CARGO_MANIFEST_DIR"));
    for (file, target) in [
        ("src/adapters/claude.rs", "lucarne::adapters::claude"),
        ("src/adapters/codex.rs", "lucarne::adapters::codex"),
        ("src/adapters/copilot.rs", "lucarne::adapters::copilot"),
        ("src/adapters/gemini.rs", "lucarne::adapters::gemini"),
        ("src/adapters/pi.rs", "lucarne::adapters::pi"),
        (
            "src/adapters/codex_prep.rs",
            "lucarne::adapters::launch_prep",
        ),
    ] {
        let source = std::fs::read_to_string(manifest.join(file)).expect("adapter source");
        assert!(
            source.contains(target),
            "{file} must emit structured tracing for operational adapter boundaries"
        );
    }
}

#[test]
fn pi_adapter_uses_per_start_components_without_shared_mutex() {
    let manifest = std::path::Path::new(env!("CARGO_MANIFEST_DIR"));
    let adapter = std::fs::read_to_string(manifest.join("src/adapter.rs")).expect("adapter");
    let pi = std::fs::read_to_string(manifest.join("src/adapters/pi.rs")).expect("pi adapter");

    assert!(
        adapter.contains("ProtocolSessionParts") && adapter.contains("build_session"),
        "ProtocolAdapter must support per-start launcher/dialect/args composition"
    );
    for forbidden in [
        "pending_ref",
        "Mutex<Option<SessionPathRef>>",
        "pending_for_args",
        "pending_for_dialect",
    ] {
        assert!(
            !pi.contains(forbidden),
            "Pi adapter must not share per-start session state through {forbidden}"
        );
    }
    assert!(
        pi.contains("build_args: Some"),
        "Pi adapter should build args per-start via build_args"
    );
}

#[test]
fn provider_catalog_ids_are_exposed_as_borrowed_read_only_view() {
    let manifest = std::path::Path::new(env!("CARGO_MANIFEST_DIR"));
    let core = std::fs::read_to_string(manifest.join("src/core_service/service.rs")).expect("core");
    assert!(
        core.contains("pub fn provider_ids(&self) -> &[&'static str]"),
        "LucarneCore provider_ids should return a borrowed catalog view, not clone a Vec"
    );
    assert!(
        !core.contains("pub fn provider_ids(&self) -> Vec<&'static str>"),
        "LucarneCore provider_ids should not allocate for read-only provider catalog queries"
    );

    let daemon = std::fs::read_to_string(manifest.join("src/daemon.rs")).expect("daemon");
    assert!(
        daemon.contains("pub fn provider_ids(&self) -> &[&'static str]"),
        "LucarneDaemon provider_ids should preserve the borrowed catalog view"
    );
}

#[test]
fn agent_runtime_register_defaults_uses_adapter_registry_boundary() {
    let manifest = std::path::Path::new(env!("CARGO_MANIFEST_DIR"));
    let runtime = std::fs::read_to_string(manifest.join("src/agent_runtime/runtime.rs"))
        .expect("read runtime source");

    for forbidden in [
        "use crate::adapters::{claude, codex, gemini}",
        "claude::new(",
        "codex::new(",
        "gemini::new(",
    ] {
        assert!(
            !runtime.contains(forbidden),
            "agent runtime should consume adapter registry output instead of constructing concrete providers: {forbidden}"
        );
    }
    assert!(
        runtime.contains("crate::adapters::default_adapters()"),
        "default adapter construction belongs in the adapters module"
    );
}

#[test]
fn agent_runtime_has_no_legacy_adapter_trait_bridge() {
    let manifest = std::path::Path::new(env!("CARGO_MANIFEST_DIR"));
    let adapter =
        std::fs::read_to_string(manifest.join("src/adapter.rs")).expect("read adapter source");
    let provider = std::fs::read_to_string(manifest.join("src/agent_runtime/provider.rs"))
        .expect("read provider source");
    let runtime_mod = std::fs::read_to_string(manifest.join("src/agent_runtime/mod.rs"))
        .expect("read runtime mod source");

    assert!(
        !adapter.contains("pub trait Adapter"),
        "the old public Adapter trait should be retired; ProtocolAdapter should expose concrete start/probe methods"
    );
    assert!(
        !adapter.contains("pub struct Registry"),
        "the old Adapter registry should be retired in favor of AgentRuntime providers"
    );
    assert!(
        !provider.contains("AdapterBackedProvider"),
        "AgentProvider should no longer be bridged through AdapterBackedProvider"
    );
    assert!(
        !runtime_mod.contains("AdapterBackedProvider"),
        "the legacy adapter bridge must not remain in the public agent_runtime exports"
    );
}

#[test]
fn reconcile_outcome_is_copy_and_not_cloned_in_reconcile_paths() {
    let manifest = std::path::Path::new(env!("CARGO_MANIFEST_DIR"));
    let types = std::fs::read_to_string(manifest.join("src/control_plane/types.rs"))
        .expect("control-plane types");
    assert!(
        types.contains("#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]\n#[serde(rename_all = \"snake_case\")]\npub enum ReconcileOutcome"),
        "ReconcileOutcome is a closed value enum and should be Copy"
    );

    let state = std::fs::read_to_string(manifest.join("src/control_plane/state.rs"))
        .expect("control-plane state");
    assert!(
        !state.contains("outcome.clone()"),
        "control-plane reconcile outcome writes should copy the enum, not clone it"
    );

    let telegram_bot = std::fs::read_to_string(
        manifest
            .parent()
            .expect("crates dir")
            .join("lucarne-telegram/src/bot.rs"),
    )
    .expect("telegram bot");
    assert!(
        !telegram_bot.contains("reconcile_outcome.clone()"),
        "Telegram reconcile path should copy ReconcileOutcome, not clone it"
    );
}

#[test]
fn workspace_binding_upsert_borrows_native_resume_ref() {
    let manifest = std::path::Path::new(env!("CARGO_MANIFEST_DIR"));
    let service =
        std::fs::read_to_string(manifest.join("src/core_service/service.rs")).expect("service");
    assert!(
        service.contains("native_resume_ref: Option<&str>"),
        "upsert_workspace_binding should borrow native resume refs instead of forcing UI callers to clone String"
    );

    let telegram_state = std::fs::read_to_string(
        manifest
            .parent()
            .expect("crates dir")
            .join("lucarne-telegram/src/state.rs"),
    )
    .expect("telegram state");
    assert!(
        !telegram_state.contains(
            "upsert_workspace_binding(workspace_id.clone(), request, native_ref.clone())"
        ),
        "Telegram state should pass native_ref.as_deref() into core instead of cloning it"
    );

    let telegram_bot = std::fs::read_to_string(
        manifest
            .parent()
            .expect("crates dir")
            .join("lucarne-telegram/src/bot.rs"),
    )
    .expect("telegram bot");
    assert!(
        !telegram_bot.contains("Some(reference.to_string())"),
        "Telegram bot should borrow provider resume refs when upserting workspace binding"
    );
}

#[test]
fn core_internal_resume_refs_stay_smolstr_until_ui_boundary() {
    let manifest = std::path::Path::new(env!("CARGO_MANIFEST_DIR"));
    let service =
        std::fs::read_to_string(manifest.join("src/core_service/service.rs")).expect("service");
    let allocations = service.matches("native_resume_ref.to_string()").count();
    assert!(
        allocations <= 1,
        "core internal resume/fork paths should not allocate String just to rebuild SessionRef; found {allocations}"
    );
}

#[tokio::test]
async fn core_exposes_daemon_api_without_adapter_types() {
    let tmp = TempDir::new().unwrap();
    let db = tmp.path().join("state.sqlite3");

    let core = LucarneCore::open_sqlite(&db).expect("open core");
    let api: &dyn DaemonApi = core.as_ref();
    let provider_ids = api
        .providers()
        .into_iter()
        .map(|provider| provider.id.as_str())
        .collect::<Vec<_>>();

    assert_eq!(provider_ids, core.provider_ids());
    assert!(api.list_sessions().is_empty());
}

#[tokio::test]
async fn core_provider_catalog_is_runtime_owned_without_default_fallback() {
    let tmp = TempDir::new().unwrap();
    let db = tmp.path().join("state.sqlite3");
    let runtime = Arc::new(lucarne::agent_runtime::AgentRuntime::new());
    let core = LucarneCore::from_runtime_and_store(
        Arc::clone(&runtime),
        ControlPlaneSqliteStore::open(&db).expect("store"),
    )
    .expect("core");

    assert!(core.provider_ids().is_empty());
    assert!(core.provider_catalog().is_empty());

    runtime.register(Arc::new(CustomProvider));
    let core = LucarneCore::from_runtime_and_store(
        runtime,
        ControlPlaneSqliteStore::open(&db).expect("store"),
    )
    .expect("core");
    assert_eq!(core.provider_ids(), &["qwen"]);
    let catalog = core.provider_catalog();
    assert_eq!(catalog.len(), 1);
    assert_eq!(catalog[0].provider_id, "qwen");
    assert_eq!(catalog[0].display_name, "Qwen");
    assert_eq!(catalog[0].binary, "qwen-cli");
    assert!(catalog[0].available);
}

#[tokio::test]
async fn core_pumps_provider_events_to_daemon_event_stream() {
    let runtime = Arc::new(lucarne::agent_runtime::AgentRuntime::new());
    runtime.register(Arc::new(BoundaryProvider));
    let core = LucarneCore::from_runtime_and_store(
        runtime,
        ControlPlaneSqliteStore::open_in_memory().expect("store"),
    )
    .expect("core");
    let workspace_id = WorkspaceId::new("source-workspace");
    let opened = core
        .open_workspace_binding_with_events(
            workspace_id.clone(),
            OpenWorkspaceRequest {
                provider_id: "codex",
                project_path: None,
                title: "source".into(),
            },
        )
        .await
        .expect("open source");
    let mut workspace_events = opened.events;
    let mut daemon_events = core.watch_events();

    core.submit_turn(lucarne::core_service::SubmitTurnRequest {
        workspace_id: workspace_id.clone(),
        source: TurnSource::UserMessage,
        input: AgentInput {
            text: "hello".into(),
            images: vec![],
        },
        reply_to_channel_message_id: None,
    })
    .await
    .expect("submit");

    let workspace_event = timeout(Duration::from_secs(1), workspace_events.recv())
        .await
        .expect("workspace event timeout")
        .expect("workspace event");
    assert!(matches!(
        workspace_event,
        Event::Message(MessageEvent {
            role: MessageRole::Assistant,
            ..
        })
    ));

    let mut saw_timeline = false;
    let mut saw_completed = false;
    for _ in 0..4 {
        match timeout(Duration::from_secs(1), daemon_events.recv())
            .await
            .expect("daemon event timeout")
            .expect("daemon event")
        {
            CoreEvent::TimelineEvent {
                workspace_id: id,
                event: Event::Message(_),
                ..
            } if id == workspace_id => saw_timeline = true,
            CoreEvent::TurnCompleted { workspace_id: id } if id == workspace_id => {
                saw_completed = true;
            }
            _ => {}
        }
        if saw_timeline && saw_completed {
            break;
        }
    }
    assert!(
        saw_timeline,
        "daemon event stream must include timeline events"
    );
    assert!(
        saw_completed,
        "daemon event stream must include turn lifecycle events"
    );
}

#[tokio::test]
async fn journey_34_dual_channel_core_event_delivers_to_both_telegram_and_wechat() {
    let runtime = Arc::new(lucarne::agent_runtime::AgentRuntime::new());
    runtime.register(Arc::new(BoundaryProvider));
    let core = LucarneCore::from_runtime_and_store(
        runtime,
        ControlPlaneSqliteStore::open_in_memory().expect("store"),
    )
    .expect("core");
    let workspace_id = WorkspaceId::new("cross-channel-workspace");
    core.open_workspace_binding_with_events(
        workspace_id.clone(),
        OpenWorkspaceRequest {
            provider_id: "codex",
            project_path: Some("/tmp/cross-channel".into()),
            title: "cross channel".into(),
        },
    )
    .await
    .expect("open workspace");
    let mut telegram_events = core.watch_events();
    let mut wechat_events = core.watch_events();

    core.submit_turn(lucarne::core_service::SubmitTurnRequest {
        workspace_id: workspace_id.clone(),
        source: TurnSource::UserMessage,
        input: AgentInput {
            text: "notify both".into(),
            images: vec![],
        },
        reply_to_channel_message_id: None,
    })
    .await
    .expect("submit turn");

    assert!(
        receiver_saw_assistant_event(&mut telegram_events, &workspace_id, "daemon event").await,
        "Telegram adapter subscriber must receive the core event"
    );
    assert!(
        receiver_saw_assistant_event(&mut wechat_events, &workspace_id, "daemon event").await,
        "WeChat adapter subscriber must receive the same core event"
    );
}

#[tokio::test]
async fn journey_38_dual_channel_active_suppression_shared_across_channels() {
    let core = LucarneCore::from_runtime_and_store(
        Arc::new(lucarne::agent_runtime::AgentRuntime::new()),
        ControlPlaneSqliteStore::open_in_memory().expect("store"),
    )
    .expect("core");
    let workspace_id = WorkspaceId::new("shared-suppression-workspace");

    assert!(!core.direct_notification_suppressed(&workspace_id));
    core.begin_direct_notification_suppression(&workspace_id);
    assert!(
        core.direct_notification_suppressed(&workspace_id),
        "suppression set by one adapter must be visible to every channel using the shared core"
    );
    core.end_direct_notification_suppression(&workspace_id);
    assert!(!core.direct_notification_suppressed(&workspace_id));
}

#[tokio::test]
async fn journey_49_shared_core_live_session_reused_across_telegram_and_wechat() {
    let runtime = Arc::new(lucarne::agent_runtime::AgentRuntime::new());
    runtime.register(Arc::new(BoundaryProvider));
    let core = LucarneCore::from_runtime_and_store(
        runtime,
        ControlPlaneSqliteStore::open_in_memory().expect("store"),
    )
    .expect("core");
    let workspace_id = WorkspaceId::new("shared-live-workspace");
    let opened = core
        .open_workspace_binding_with_events(
            workspace_id.clone(),
            OpenWorkspaceRequest {
                provider_id: "codex",
                project_path: Some("/tmp/shared-live".into()),
                title: "shared live".into(),
            },
        )
        .await
        .expect("telegram opens workspace");

    let telegram_resume = core
        .resume_workspace_with_events(ResumeWorkspaceRequest {
            workspace_id: workspace_id.clone(),
            force_bypass_permissions: false,
        })
        .await
        .expect("telegram resume reuses live session");
    let wechat_resume = core
        .resume_workspace_with_events(ResumeWorkspaceRequest {
            workspace_id: workspace_id.clone(),
            force_bypass_permissions: false,
        })
        .await
        .expect("wechat resume reuses live session");

    assert_eq!(
        telegram_resume.session.instance_id(),
        opened.session.instance_id(),
        "Telegram should reuse the existing live provider session"
    );
    assert_eq!(
        wechat_resume.session.instance_id(),
        opened.session.instance_id(),
        "WeChat should reuse the same live provider session through shared core"
    );
}

#[tokio::test]
async fn journey_67_scheduled_task_resumes_submits_and_notifies() {
    let provider = Arc::new(ScheduledProvider::default());
    let runtime = Arc::new(lucarne::agent_runtime::AgentRuntime::new());
    runtime.register(provider.clone());
    let temp = TempDir::new().expect("temp dir");
    let db = temp.path().join("state.sqlite3");
    let workspace_id = WorkspaceId::new("scheduled-workspace");
    let task_id = ScheduledTaskId::new("daily-summary");

    let core = LucarneCore::from_runtime_and_store(
        Arc::clone(&runtime),
        ControlPlaneSqliteStore::open(&db).expect("store"),
    )
    .expect("core");
    core.upsert_workspace_binding(
        workspace_id.clone(),
        OpenWorkspaceRequest {
            provider_id: "codex",
            project_path: Some("/tmp/scheduled-project".into()),
            title: "scheduled project".into(),
        },
        Some("scheduled-native-session"),
    )
    .expect("seed resumable workspace");
    core.upsert_scheduled_task(UpsertScheduledTaskRequest {
        task_id: task_id.clone(),
        workspace_id: workspace_id.clone(),
        provider_id: "codex",
        project_path: "/tmp/scheduled-project".into(),
        title: "scheduled project".into(),
        prompt: "run scheduled summary".into(),
        next_run_unix_ms: 1_000,
        enabled: true,
    })
    .expect("persist scheduled task");

    let reloaded = LucarneCore::from_runtime_and_store(
        Arc::clone(&runtime),
        ControlPlaneSqliteStore::open(&db).expect("reopen store"),
    )
    .expect("reload core");
    assert!(
        reloaded.scheduled_task(&task_id).is_some(),
        "scheduled task must reload from the control-plane store"
    );
    let mut events = reloaded.watch_events();

    let report = reloaded
        .run_due_scheduled_tasks(RunDueScheduledTasksRequest { now_unix_ms: 1_001 })
        .await
        .expect("run due scheduled tasks");

    assert!(
        report.failed.is_empty(),
        "scheduled task failed: {report:?}"
    );
    assert_eq!(report.triggered.len(), 1);
    assert_eq!(report.triggered[0].task_id, task_id);
    assert_eq!(
        provider.resumes.lock().expect("resumes lock").as_slice(),
        ["scheduled-native-session"]
    );
    assert_eq!(
        provider
            .submissions
            .lock()
            .expect("submissions lock")
            .as_slice(),
        ["run scheduled summary"]
    );
    assert!(
        receiver_saw_assistant_event(&mut events, &workspace_id, "scheduled done").await,
        "scheduled task completion must notify core event subscribers"
    );
    let task = reloaded
        .scheduled_task(&ScheduledTaskId::new("daily-summary"))
        .expect("scheduled task after run");
    assert!(!task.enabled);
    assert_eq!(task.last_run_unix_ms, Some(1_001));

    let reloaded_again = LucarneCore::from_runtime_and_store(
        runtime,
        ControlPlaneSqliteStore::open(&db).expect("reopen store after run"),
    )
    .expect("reload core after run");
    let persisted = reloaded_again
        .scheduled_task(&ScheduledTaskId::new("daily-summary"))
        .expect("persisted scheduled task after run");
    assert!(!persisted.enabled);
    assert_eq!(persisted.last_run_unix_ms, Some(1_001));
}

async fn receiver_saw_assistant_event(
    receiver: &mut lucarne::core_service::CoreEventReceiver,
    workspace_id: &WorkspaceId,
    expected: &str,
) -> bool {
    for _ in 0..6 {
        let Ok(Ok(event)) = timeout(Duration::from_secs(1), receiver.recv()).await else {
            return false;
        };
        if let CoreEvent::TimelineEvent {
            workspace_id: id,
            event:
                Event::Message(MessageEvent {
                    role: MessageRole::Assistant,
                    text,
                    ..
                }),
            ..
        } = event
        {
            if &id == workspace_id && text.as_str() == expected {
                return true;
            }
        }
    }
    false
}

#[tokio::test]
async fn core_turn_lifecycle_does_not_rewrite_unrelated_workspace_rows() {
    let runtime = Arc::new(lucarne::agent_runtime::AgentRuntime::new());
    runtime.register(Arc::new(BoundaryProvider));
    let tmp = TempDir::new().expect("temp dir");
    let db = tmp.path().join("state.sqlite3");
    let core = LucarneCore::from_runtime_and_store(
        runtime,
        ControlPlaneSqliteStore::open(&db).expect("store"),
    )
    .expect("core");
    let workspace_a = WorkspaceId::new("workspace-a");
    let workspace_b = WorkspaceId::new("workspace-b");
    let opened = core
        .open_workspace_binding_with_events(
            workspace_a.clone(),
            OpenWorkspaceRequest {
                provider_id: "codex",
                project_path: None,
                title: "workspace a".into(),
            },
        )
        .await
        .expect("open workspace a");
    core.upsert_workspace_binding(
        workspace_b.clone(),
        OpenWorkspaceRequest {
            provider_id: "codex",
            project_path: None,
            title: "workspace b".into(),
        },
        Some("workspace-b-session"),
    )
    .expect("seed workspace b");
    install_entity_write_log(&db);

    let turn = core
        .start_turn(
            workspace_a.clone(),
            core.active_provider_session_id(&workspace_a)
                .expect("provider session"),
            lucarne::control_plane::LiveInstanceId::new(opened.session.instance_id().0.clone()),
            lucarne::control_plane::TurnSource::UserMessage,
            "hello",
            None,
        )
        .expect("start turn");
    core.complete_turn_with_usage_value(turn.turn_id, None)
        .expect("complete turn");

    assert!(
        entity_write_log(&db, "workspace", workspace_b.as_str()).is_empty(),
        "turn lifecycle persistence must not rewrite unrelated workspace rows"
    );
}

fn install_entity_write_log(db: &std::path::Path) {
    let conn = Connection::open(db).unwrap();
    conn.execute_batch(
        "CREATE TABLE entity_write_log (
            action TEXT NOT NULL,
            kind TEXT NOT NULL,
            entity_id TEXT NOT NULL
        );
        CREATE TRIGGER log_control_plane_delete
        AFTER DELETE ON control_plane_entities
        BEGIN
            INSERT INTO entity_write_log(action, kind, entity_id)
            VALUES ('delete', OLD.kind, OLD.entity_id);
        END;
        CREATE TRIGGER log_control_plane_insert
        AFTER INSERT ON control_plane_entities
        BEGIN
            INSERT INTO entity_write_log(action, kind, entity_id)
            VALUES ('insert', NEW.kind, NEW.entity_id);
        END;
        CREATE TRIGGER log_control_plane_update
        AFTER UPDATE ON control_plane_entities
        BEGIN
            INSERT INTO entity_write_log(action, kind, entity_id)
            VALUES ('update', NEW.kind, NEW.entity_id);
        END;",
    )
    .unwrap();
}

fn entity_write_log(db: &std::path::Path, kind: &str, entity_id: &str) -> Vec<String> {
    let conn = Connection::open(db).unwrap();
    let mut stmt = conn
        .prepare(
            "SELECT action FROM entity_write_log
             WHERE kind = ?1 AND entity_id = ?2
             ORDER BY rowid",
        )
        .unwrap();
    stmt.query_map((kind, entity_id), |row| row.get(0))
        .unwrap()
        .collect::<Result<Vec<_>, _>>()
        .unwrap()
}

struct BoundaryProvider;

struct CustomProvider;

#[async_trait]
impl AgentProvider for CustomProvider {
    fn id(&self) -> ProviderId {
        ProviderId::from_static("qwen")
    }

    fn label(&self) -> &str {
        "Qwen"
    }

    fn binary(&self) -> &str {
        "qwen-cli"
    }

    async fn probe(&self) -> Result<ProbeResult, AgentError> {
        Ok(ProbeResult {
            provider_id: ProviderId::from_static("qwen"),
            provider_version: None,
            capabilities: Default::default(),
        })
    }

    async fn open(&self, _req: OpenSession) -> Result<Box<dyn AgentSession>, AgentError> {
        Err(AgentError {
            kind: AgentErrorKind::Unsupported,
            message: "open not used".into(),
        })
    }

    async fn resume(&self, _req: ResumeSession) -> Result<Box<dyn AgentSession>, AgentError> {
        Err(AgentError {
            kind: AgentErrorKind::Unsupported,
            message: "resume not used".into(),
        })
    }
}

#[async_trait]
impl AgentProvider for BoundaryProvider {
    fn id(&self) -> ProviderId {
        ProviderId::from_static("codex")
    }

    async fn probe(&self) -> Result<ProbeResult, AgentError> {
        Ok(ProbeResult {
            provider_id: ProviderId::from_static("codex"),
            provider_version: Some("test".into()),
            capabilities: Default::default(),
        })
    }

    async fn open(&self, _req: OpenSession) -> Result<Box<dyn AgentSession>, AgentError> {
        Ok(Box::new(TestSession::new(
            "source-session",
            "source-instance",
        )))
    }

    async fn resume(&self, _req: ResumeSession) -> Result<Box<dyn AgentSession>, AgentError> {
        Err(AgentError {
            kind: AgentErrorKind::Unsupported,
            message: "resume not used".into(),
        })
    }
}

struct TestSession {
    id: SessionId,
    instance_id: InstanceId,
    events_tx: tokio::sync::mpsc::Sender<Event>,
    events_rx: tokio::sync::Mutex<Option<AgentEventStream>>,
}

impl TestSession {
    fn new(id: &str, instance_id: &str) -> Self {
        let (events_tx, events_rx) = tokio::sync::mpsc::channel(4);
        Self {
            id: SessionId(SmolStr::new(id)),
            instance_id: InstanceId(SmolStr::new(instance_id)),
            events_tx,
            events_rx: tokio::sync::Mutex::new(Some(events_rx)),
        }
    }
}

#[async_trait]
impl AgentSession for TestSession {
    fn id(&self) -> &SessionId {
        &self.id
    }

    fn instance_id(&self) -> &InstanceId {
        &self.instance_id
    }

    fn provider_id(&self) -> ProviderId {
        ProviderId::from_static("codex")
    }

    async fn submit(&self, _input: AgentInput) -> Result<(), AgentError> {
        self.events_tx
            .send(Event::Message(MessageEvent {
                role: MessageRole::Assistant,
                text: "daemon event".into(),
                streaming: false,
            }))
            .await
            .unwrap();
        self.events_tx
            .send(Event::TurnCompleted(TurnCompletedEvent {
                turn_id: "turn-1".into(),
                usage: None,
            }))
            .await
            .unwrap();
        Ok(())
    }

    async fn interrupt(&self) -> Result<(), AgentError> {
        Ok(())
    }

    async fn resolve(
        &self,
        _req_id: &str,
        _response: InterventionResponse,
    ) -> Result<(), AgentError> {
        Ok(())
    }

    async fn take_events(&self) -> Result<AgentEventStream, AgentError> {
        self.events_rx
            .lock()
            .await
            .take()
            .ok_or_else(|| AgentError {
                kind: AgentErrorKind::InvalidState,
                message: "events already taken".into(),
            })
    }

    async fn close(&self) -> Result<(), AgentError> {
        Ok(())
    }
}

#[derive(Default)]
struct ScheduledProvider {
    resumes: Arc<Mutex<Vec<String>>>,
    submissions: Arc<Mutex<Vec<String>>>,
}

#[async_trait]
impl AgentProvider for ScheduledProvider {
    fn id(&self) -> ProviderId {
        ProviderId::from_static("codex")
    }

    async fn probe(&self) -> Result<ProbeResult, AgentError> {
        Ok(ProbeResult {
            provider_id: ProviderId::from_static("codex"),
            provider_version: Some("test".into()),
            capabilities: Default::default(),
        })
    }

    async fn open(&self, _req: OpenSession) -> Result<Box<dyn AgentSession>, AgentError> {
        Ok(Box::new(ScheduledSession::new(
            "scheduled-opened-session",
            "scheduled-opened-instance",
            Arc::clone(&self.submissions),
        )))
    }

    async fn resume(&self, req: ResumeSession) -> Result<Box<dyn AgentSession>, AgentError> {
        self.resumes
            .lock()
            .expect("resumes lock")
            .push(req.session_ref.0.to_string());
        Ok(Box::new(ScheduledSession::new(
            "scheduled-resumed-session",
            "scheduled-resumed-instance",
            Arc::clone(&self.submissions),
        )))
    }
}

struct ScheduledSession {
    id: SessionId,
    instance_id: InstanceId,
    submissions: Arc<Mutex<Vec<String>>>,
    events_tx: tokio::sync::mpsc::Sender<Event>,
    events_rx: tokio::sync::Mutex<Option<AgentEventStream>>,
}

impl ScheduledSession {
    fn new(id: &str, instance_id: &str, submissions: Arc<Mutex<Vec<String>>>) -> Self {
        let (events_tx, events_rx) = tokio::sync::mpsc::channel(4);
        Self {
            id: SessionId(SmolStr::new(id)),
            instance_id: InstanceId(SmolStr::new(instance_id)),
            submissions,
            events_tx,
            events_rx: tokio::sync::Mutex::new(Some(events_rx)),
        }
    }
}

#[async_trait]
impl AgentSession for ScheduledSession {
    fn id(&self) -> &SessionId {
        &self.id
    }

    fn instance_id(&self) -> &InstanceId {
        &self.instance_id
    }

    fn provider_id(&self) -> ProviderId {
        ProviderId::from_static("codex")
    }

    async fn submit(&self, input: AgentInput) -> Result<(), AgentError> {
        self.submissions
            .lock()
            .expect("submissions lock")
            .push(input.text.to_string());
        self.events_tx
            .send(Event::Message(MessageEvent {
                role: MessageRole::Assistant,
                text: "scheduled done".into(),
                streaming: false,
            }))
            .await
            .unwrap();
        self.events_tx
            .send(Event::TurnCompleted(TurnCompletedEvent {
                turn_id: "scheduled-turn".into(),
                usage: None,
            }))
            .await
            .unwrap();
        Ok(())
    }

    async fn interrupt(&self) -> Result<(), AgentError> {
        Ok(())
    }

    async fn resolve(
        &self,
        _req_id: &str,
        _response: InterventionResponse,
    ) -> Result<(), AgentError> {
        Ok(())
    }

    async fn take_events(&self) -> Result<AgentEventStream, AgentError> {
        self.events_rx
            .lock()
            .await
            .take()
            .ok_or_else(|| AgentError {
                kind: AgentErrorKind::InvalidState,
                message: "events already taken".into(),
            })
    }

    async fn close(&self) -> Result<(), AgentError> {
        Ok(())
    }
}
