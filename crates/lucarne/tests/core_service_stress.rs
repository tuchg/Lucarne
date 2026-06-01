use std::{
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc,
    },
    time::{Duration, Instant},
};

use async_trait::async_trait;
use lucarne::{
    agent_runtime::{
        events::TurnCompletedEvent, AgentError, AgentErrorKind, AgentEventStream, AgentInput,
        AgentProvider, AgentSession, Event, InstanceId, InterventionResponse, MessageEvent,
        MessageRole, OpenSession, ProbeResult, ResumeSession, SessionId,
    },
    control_plane::{
        ControlPlaneSqliteStore, TimelineItem, TimelineItemKind, TurnSource, WorkspaceId,
    },
    core_service::{OpenWorkspaceRequest, SubmitTurnRequest},
    LucarneCore, ProviderId,
};
use smol_str::SmolStr;
use tempfile::TempDir;
use tokio::{
    sync::{mpsc, Mutex as AsyncMutex},
    time::timeout,
};

const WORKSPACES: usize = 16;
const TURNS_PER_WORKSPACE: usize = 4;

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn core_service_stress_open_submit_record_and_complete_many_workspaces() {
    let workspaces = stress_usize("LUCARNE_STRESS_WORKSPACES", WORKSPACES);
    let turns_per_workspace =
        stress_usize("LUCARNE_STRESS_TURNS_PER_WORKSPACE", TURNS_PER_WORKSPACE);
    let worker_timeout = Duration::from_secs(stress_u64("LUCARNE_STRESS_WORKER_TIMEOUT_SECS", 10));
    let max_elapsed_ms = stress_optional_u128("LUCARNE_STRESS_MAX_ELAPSED_MS");
    let runtime = Arc::new(lucarne::agent_runtime::AgentRuntime::new());
    runtime.register(Arc::new(StressProvider::default()));
    let tmp = TempDir::new().expect("temp dir");
    let core = LucarneCore::from_runtime_and_store(
        runtime,
        ControlPlaneSqliteStore::open(tmp.path().join("control-plane.sqlite3")).expect("store"),
    )
    .expect("core");

    let started = Instant::now();
    let mut tasks = Vec::new();
    for workspace_idx in 0..workspaces {
        let core = Arc::clone(&core);
        tasks.push(tokio::spawn(async move {
            exercise_workspace(core, workspace_idx, turns_per_workspace)
                .await
                .map_err(|err| format!("workspace {workspace_idx}: {err}"))
        }));
    }

    for task in tasks {
        timeout(worker_timeout, task)
            .await
            .expect("stress worker timed out")
            .expect("stress worker panicked")
            .expect("stress worker failed");
    }

    let elapsed = started.elapsed();
    let sessions = core.list_sessions();
    assert_eq!(sessions.len(), workspaces);
    if let Some(max_elapsed_ms) = max_elapsed_ms {
        assert!(
            elapsed.as_millis() <= max_elapsed_ms,
            "core service stress exceeded max elapsed: actual={}ms max={}ms",
            elapsed.as_millis(),
            max_elapsed_ms
        );
    }

    let store = core.control_plane_store();
    let reloaded = store
        .load_control_plane()
        .expect("load persisted control-plane")
        .expect("persisted control-plane");
    assert_eq!(reloaded.workspace_bindings().len(), 0);
    assert_eq!(store.workspace_bindings().unwrap().len(), workspaces);
}

async fn exercise_workspace(
    core: Arc<LucarneCore>,
    workspace_idx: usize,
    turns_per_workspace: usize,
) -> Result<(), String> {
    let workspace_id = WorkspaceId::new(format!("stress-workspace-{workspace_idx:03}"));
    let opened = core
        .open_workspace_binding_with_events(
            workspace_id.clone(),
            OpenWorkspaceRequest {
                provider_id: "stress",
                project_path: None,
                title: format!("stress {workspace_idx}"),
            },
        )
        .await
        .map_err(|err| err.to_string())?;
    let mut events = opened.events;

    for turn_idx in 0..turns_per_workspace {
        let input = format!("stress input {workspace_idx}:{turn_idx}");
        let submitted = core
            .submit_turn(SubmitTurnRequest {
                workspace_id: workspace_id.clone(),
                source: TurnSource::UserMessage,
                input: AgentInput {
                    text: input.into(),
                    images: Vec::new(),
                },
                reply_to_channel_message_id: None,
            })
            .await
            .map_err(|err| err.to_string())?;

        let mut saw_assistant = false;
        let mut saw_completed = false;
        for _ in 0..2 {
            match timeout(Duration::from_secs(2), events.recv())
                .await
                .map_err(|_| "workspace event timeout".to_string())?
                .map_err(|err| format!("workspace event stream error: {err:?}"))?
            {
                Event::Message(message) if message.role == MessageRole::Assistant => {
                    core.append_timeline(TimelineItem::new(
                        workspace_id.clone(),
                        submitted.turn_id.clone(),
                        TimelineItemKind::Assistant,
                        serde_json::json!({ "text": message.text }),
                    ))
                    .map_err(|err| err.to_string())?;
                    saw_assistant = true;
                }
                Event::TurnCompleted(_) => {
                    saw_completed = true;
                }
                other => return Err(format!("unexpected workspace event: {other:?}")),
            }
        }
        if !saw_assistant || !saw_completed {
            return Err(format!(
                "missing events for turn {turn_idx}: assistant={saw_assistant} completed={saw_completed}"
            ));
        }
    }
    Ok(())
}

fn stress_usize(name: &str, default: usize) -> usize {
    std::env::var(name)
        .ok()
        .and_then(|value| value.parse().ok())
        .filter(|value| *value > 0)
        .unwrap_or(default)
}

fn stress_u64(name: &str, default: u64) -> u64 {
    std::env::var(name)
        .ok()
        .and_then(|value| value.parse().ok())
        .filter(|value| *value > 0)
        .unwrap_or(default)
}

fn stress_optional_u128(name: &str) -> Option<u128> {
    std::env::var(name)
        .ok()
        .and_then(|value| value.parse().ok())
        .filter(|value| *value > 0)
}

#[derive(Default)]
struct StressProvider {
    next: AtomicUsize,
}

#[async_trait]
impl AgentProvider for StressProvider {
    fn id(&self) -> ProviderId {
        ProviderId::from_static("stress")
    }

    async fn probe(&self) -> Result<ProbeResult, AgentError> {
        Ok(ProbeResult {
            provider_id: ProviderId::from_static("stress"),
            provider_version: Some("stress".into()),
            capabilities: Default::default(),
        })
    }

    async fn open(&self, _req: OpenSession) -> Result<Box<dyn AgentSession>, AgentError> {
        let idx = self.next.fetch_add(1, Ordering::Relaxed);
        Ok(Box::new(StressSession::new(idx)))
    }

    async fn resume(&self, _req: ResumeSession) -> Result<Box<dyn AgentSession>, AgentError> {
        Err(unsupported("resume"))
    }
}

struct StressSession {
    id: SessionId,
    instance_id: InstanceId,
    events_tx: mpsc::Sender<Event>,
    events_rx: AsyncMutex<Option<AgentEventStream>>,
    submitted: AtomicUsize,
}

impl StressSession {
    fn new(idx: usize) -> Self {
        let (events_tx, events_rx) = mpsc::channel(64);
        Self {
            id: SessionId(SmolStr::new(format!("stress-session-{idx:03}"))),
            instance_id: InstanceId(SmolStr::new(format!("stress-instance-{idx:03}"))),
            events_tx,
            events_rx: AsyncMutex::new(Some(events_rx)),
            submitted: AtomicUsize::new(0),
        }
    }
}

#[async_trait]
impl AgentSession for StressSession {
    fn id(&self) -> &SessionId {
        &self.id
    }

    fn instance_id(&self) -> &InstanceId {
        &self.instance_id
    }

    fn provider_id(&self) -> ProviderId {
        ProviderId::from_static("stress")
    }

    async fn submit(&self, input: AgentInput) -> Result<(), AgentError> {
        let turn_idx = self.submitted.fetch_add(1, Ordering::Relaxed);
        self.events_tx
            .send(Event::Message(MessageEvent {
                role: MessageRole::Assistant,
                text: format!("reply {turn_idx}: {}", input.text).into(),
                streaming: false,
            }))
            .await
            .map_err(|_| invalid_state("stress event receiver closed"))?;
        self.events_tx
            .send(Event::TurnCompleted(TurnCompletedEvent {
                turn_id: format!("stress-turn-{turn_idx}").into(),
                usage: None,
            }))
            .await
            .map_err(|_| invalid_state("stress event receiver closed"))?;
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
            .ok_or_else(|| invalid_state("events already taken"))
    }

    async fn close(&self) -> Result<(), AgentError> {
        Ok(())
    }
}

fn unsupported(action: &str) -> AgentError {
    AgentError {
        kind: AgentErrorKind::Unsupported,
        message: format!("stress provider does not support {action}").into(),
    }
}

fn invalid_state(message: impl Into<SmolStr>) -> AgentError {
    AgentError {
        kind: AgentErrorKind::InvalidState,
        message: message.into(),
    }
}
