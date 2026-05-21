use std::path::PathBuf;

use async_trait::async_trait;
use smol_str::SmolStr;

use crate::{
    agent_runtime::{
        AgentCommandCatalog, AgentCommandInvocation, AgentCommandResult, AgentError,
        AgentEventStream, AgentForkResult, AgentForkSelection, AgentForkTargetCatalog, AgentInput,
        AgentModelCatalog, AgentModelSelection, AgentPermissionCatalog, AgentPermissionSelection,
        AgentSession, AgentSkillCatalog, AgentStatus, Event as AgentEvent, InstanceId,
        InterventionResponse, ProviderId, SessionId,
    },
    control_plane::{
        InterventionCallbackToken, LiveInstanceId, ProviderSessionId, Revision, ScheduledTaskId,
        TurnId, WorkspaceId,
    },
    history::{HistoryEntry, HistoryWorkspace},
};

pub use crate::control_plane::SystemSettings;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WorkspaceSummary {
    pub workspace_id: WorkspaceId,
    pub provider_id: &'static str,
    pub title: String,
    pub project_path: Option<PathBuf>,
    pub revision: Revision,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ProviderCatalogEntry {
    pub provider_id: &'static str,
    pub display_name: SmolStr,
    pub runtime_label: SmolStr,
    pub binary: SmolStr,
    pub available: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct HistoryProviderCatalogEntry {
    pub provider_id: &'static str,
    pub display_name: &'static str,
}

#[derive(Debug, Clone)]
pub struct HistoryPage {
    pub entries: Vec<HistoryEntry>,
    pub total: usize,
}

#[derive(Debug, Clone)]
pub struct HistoryWorkspacePage {
    pub entries: Vec<HistoryWorkspace>,
    pub total: usize,
}

#[derive(Debug, Clone)]
pub struct OpenWorkspaceRequest {
    pub provider_id: &'static str,
    pub project_path: Option<PathBuf>,
    pub title: String,
}

#[derive(Debug, Clone)]
pub struct ResumeWorkspaceRequest {
    pub workspace_id: WorkspaceId,
    pub force_bypass_permissions: bool,
}

#[derive(Debug, Clone)]
pub struct SubmitTurnRequest {
    pub workspace_id: WorkspaceId,
    pub input: AgentInput,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SubmittedTurn {
    pub turn_id: TurnId,
}

#[derive(Debug, Clone)]
pub struct UpsertScheduledTaskRequest {
    pub task_id: ScheduledTaskId,
    pub workspace_id: WorkspaceId,
    pub provider_id: &'static str,
    pub project_path: PathBuf,
    pub title: String,
    pub prompt: String,
    pub next_run_unix_ms: u64,
    pub enabled: bool,
}

#[derive(Debug, Clone, Copy)]
pub struct RunDueScheduledTasksRequest {
    pub now_unix_ms: u64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ScheduledTaskRun {
    pub task_id: ScheduledTaskId,
    pub workspace_id: WorkspaceId,
    pub turn_id: TurnId,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ScheduledTaskRunError {
    pub task_id: ScheduledTaskId,
    pub workspace_id: WorkspaceId,
    pub error: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct RunScheduledTasksReport {
    pub triggered: Vec<ScheduledTaskRun>,
    pub failed: Vec<ScheduledTaskRunError>,
}

#[derive(Debug, Clone)]
pub struct InterruptTurnRequest {
    pub workspace_id: WorkspaceId,
}

#[derive(Debug, Clone)]
pub struct ResolvePermissionRequest {
    pub workspace_id: WorkspaceId,
    pub token: InterventionCallbackToken,
    pub response: InterventionResponse,
}

#[derive(Debug, Clone)]
pub struct InvokeCommandRequest {
    pub workspace_id: WorkspaceId,
    pub command: AgentCommandInvocation,
}

#[derive(Debug, Clone)]
pub struct CloseWorkspaceRequest {
    pub workspace_id: WorkspaceId,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum AgentResourceScope {
    All,
    Workspace(WorkspaceId),
}

#[derive(Debug, Clone, PartialEq)]
pub struct AgentResourceSnapshot {
    pub managed_agent_count: usize,
    pub process_count: usize,
    pub total_cpu_percent: f32,
    pub total_memory_bytes: u64,
    pub agents: Vec<AgentResourceEntry>,
    pub observed_sessions: Vec<ObservedAgentSession>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct AgentResourceEntry {
    pub workspace_id: WorkspaceId,
    pub title: SmolStr,
    pub provider_id: &'static str,
    pub provider_session_id: ProviderSessionId,
    pub native_resume_ref: SmolStr,
    pub live_instance_id: LiveInstanceId,
    pub pid: Option<i32>,
    pub identity: Option<String>,
    pub process_count: usize,
    pub cpu_percent: f32,
    pub memory_bytes: u64,
    pub last_active_unix: i64,
    pub last_active_display: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ObservedAgentSession {
    pub workspace_id: WorkspaceId,
    pub provider_id: &'static str,
    pub provider_session_id: ProviderSessionId,
    pub native_resume_ref: SmolStr,
    pub title: SmolStr,
    pub cwd: Option<PathBuf>,
    pub session_path: PathBuf,
    pub last_active_unix: i64,
    pub last_active_display: String,
    pub observed_pid: Option<i32>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct KillAgentRequest {
    pub scope: AgentResourceScope,
    pub target: KillAgentTarget,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum KillAgentTarget {
    All,
    Identity(String),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct KillAgentReport {
    pub killed: Vec<KilledAgent>,
    pub not_found: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct KilledAgent {
    pub workspace_id: WorkspaceId,
    pub provider_session_id: ProviderSessionId,
    pub native_resume_ref: SmolStr,
    pub live_instance_id: LiveInstanceId,
    pub pid: Option<i32>,
    pub identity: Option<String>,
}

pub fn render_agent_resource_snapshot(snapshot: &AgentResourceSnapshot) -> String {
    let mut body = format!(
        "agent resources\nmanaged agents: `{}`\nobserved recent: `{}`\nactual processes: `{}`\ncpu: `{}`\nmemory: `{}`",
        snapshot.managed_agent_count,
        snapshot.observed_sessions.len(),
        snapshot.process_count,
        format_cpu(snapshot.total_cpu_percent),
        format_bytes(snapshot.total_memory_bytes),
    );
    for agent in &snapshot.agents {
        body.push_str("\n\n");
        body.push_str(agent.identity.as_deref().unwrap_or("unidentified"));
        body.push('\n');
        let last_active_display = agent_last_active_display(snapshot, agent);
        body.push_str(&format!(
            "workspace: `{}`\nprovider: `{}`\nsession: `{}`\nlast active: `{}`\npid: `{}`\nprocesses: `{}`\ncpu: `{}`\nmemory: `{}`",
            agent.workspace_id.as_str(),
            agent.provider_id,
            agent.native_resume_ref,
            last_active_display,
            agent
                .pid
                .map(|pid| pid.to_string())
                .unwrap_or_else(|| "unknown".to_string()),
            agent.process_count,
            format_cpu(agent.cpu_percent),
            format_bytes(agent.memory_bytes),
        ));
    }
    if !snapshot.observed_sessions.is_empty() {
        body.push_str("\n\nobserved recent");
        for session in &snapshot.observed_sessions {
            body.push_str("\n\n");
            body.push_str(session.title.as_ref());
            body.push('\n');
            body.push_str(&format!(
                "workspace: `{}`\nprovider: `{}`\nsession: `{}`\nlast active: `{}`",
                session.workspace_id.as_str(),
                session.provider_id,
                session.native_resume_ref,
                session.last_active_display,
            ));
            if let Some(cwd) = session.cwd.as_ref() {
                body.push_str(&format!("\ncwd: `{}`", cwd.display()));
            }
        }
    }
    body
}

pub fn render_kill_agent_report(report: &KillAgentReport) -> String {
    let mut body = format!("kill agent\nkilled: `{}`", report.killed.len());
    for killed in &report.killed {
        body.push_str("\n\n");
        body.push_str(killed.identity.as_deref().unwrap_or("unidentified"));
        body.push('\n');
        body.push_str(&format!(
            "workspace: `{}`\nsession: `{}`\npid: `{}`",
            killed.workspace_id.as_str(),
            killed.native_resume_ref,
            killed
                .pid
                .map(|pid| pid.to_string())
                .unwrap_or_else(|| "unknown".to_string()),
        ));
    }
    if let Some(identity) = report.not_found.as_deref() {
        body.push_str(&format!("\n\nnot found: `{identity}`"));
    }
    body
}

fn agent_last_active_display<'a>(
    snapshot: &'a AgentResourceSnapshot,
    agent: &'a AgentResourceEntry,
) -> &'a str {
    if !agent.last_active_display.trim().is_empty() {
        return agent.last_active_display.as_str();
    }
    snapshot
        .observed_sessions
        .iter()
        .find(|session| session.provider_session_id == agent.provider_session_id)
        .map(|session| session.last_active_display.as_str())
        .filter(|display| !display.trim().is_empty())
        .unwrap_or("unknown")
}

fn format_cpu(value: f32) -> String {
    format!("{value:.1}%")
}

fn format_bytes(bytes: u64) -> String {
    const KIB: f64 = 1024.0;
    const MIB: f64 = KIB * 1024.0;
    const GIB: f64 = MIB * 1024.0;
    let bytes = bytes as f64;
    if bytes >= GIB {
        format!("{:.1} GiB", bytes / GIB)
    } else if bytes >= MIB {
        format!("{:.1} MiB", bytes / MIB)
    } else if bytes >= KIB {
        format!("{:.1} KiB", bytes / KIB)
    } else {
        format!("{bytes:.0} B")
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn resource_snapshot_renderer_shows_managed_last_active_from_observed_watch() {
        let snapshot = AgentResourceSnapshot {
            managed_agent_count: 1,
            process_count: 2,
            total_cpu_percent: 0.6,
            total_memory_bytes: 221_800_000,
            agents: vec![AgentResourceEntry {
                workspace_id: WorkspaceId::new("codex:resume:observed"),
                title: "fix checkout bug".into(),
                provider_id: "codex",
                provider_session_id: ProviderSessionId::new("codex:observed-thread"),
                native_resume_ref: "observed-thread".into(),
                live_instance_id: LiveInstanceId::new("live-observed"),
                pid: Some(98_844),
                identity: Some("observed-thread:98844".into()),
                process_count: 2,
                cpu_percent: 0.6,
                memory_bytes: 221_800_000,
                last_active_unix: 1_776_960_000,
                last_active_display: "05-01 00:00:00".into(),
            }],
            observed_sessions: vec![
                ObservedAgentSession {
                    workspace_id: WorkspaceId::new("codex:resume:observed"),
                    provider_id: "codex",
                    provider_session_id: ProviderSessionId::new("codex:observed-thread"),
                    native_resume_ref: "observed-thread".into(),
                    title: "fix checkout bug".into(),
                    cwd: Some(PathBuf::from("/tmp/lucarnex")),
                    session_path: PathBuf::from("/tmp/rollout-observed-thread.jsonl"),
                    last_active_unix: 1_776_960_000,
                    last_active_display: "05-01 00:00:00".into(),
                    observed_pid: None,
                },
                ObservedAgentSession {
                    workspace_id: WorkspaceId::new("codex:resume:observed-two"),
                    provider_id: "codex",
                    provider_session_id: ProviderSessionId::new("codex:observed-thread-two"),
                    native_resume_ref: "observed-thread-two".into(),
                    title: "review spacing".into(),
                    cwd: None,
                    session_path: PathBuf::from("/tmp/rollout-observed-thread-two.jsonl"),
                    last_active_unix: 1_776_960_001,
                    last_active_display: "05-01 00:00:01".into(),
                    observed_pid: None,
                },
            ],
        };

        let rendered = render_agent_resource_snapshot(&snapshot);
        let managed_block = rendered
            .split("\n\nobserved recent")
            .next()
            .expect("managed block");
        let observed_block = rendered
            .split("\n\nobserved recent")
            .nth(1)
            .expect("observed block");

        assert!(rendered.contains("managed agents: `1`"));
        assert!(rendered.contains("observed recent: `2`"));
        assert!(managed_block.contains("observed-thread:98844"));
        assert!(managed_block.contains("last active: `05-01 00:00:00`"));
        assert!(observed_block.contains("fix checkout bug"));
        assert!(
            observed_block.contains("cwd: `/tmp/lucarnex`\n\nreview spacing"),
            "observed session items should be separated by a blank line"
        );
        assert!(observed_block.contains("provider: `codex`"));
        assert!(observed_block.contains("session: `observed-thread`"));
        assert!(observed_block.contains("last active: `05-01 00:00:00`"));
        assert!(observed_block.contains("cwd: `/tmp/lucarnex`"));
        assert!(!observed_block.contains("pid:"));
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LiveWorkspace {
    pub workspace_id: WorkspaceId,
    pub provider_id: &'static str,
    pub session_id: SessionId,
}

#[derive(Debug, Clone)]
pub struct CoreTimelineEvent {
    pub workspace_id: WorkspaceId,
    pub turn_id: Option<TurnId>,
    pub event: AgentEvent,
}

pub type ProviderSessionProjection = crate::control_plane::ProviderSessionRecord;
pub type LiveInstanceProjection = crate::control_plane::LiveInstanceRecord;
pub type TimelineProjectionItem = crate::control_plane::TimelineItem;
pub type TimelineKindProjection = crate::control_plane::TimelineItemKind;

#[async_trait]
pub trait DaemonSession: AgentSession {
    async fn submit_turn(&self, input: AgentInput) -> Result<(), AgentError> {
        self.submit(input).await
    }

    async fn run_command(
        &self,
        command: AgentCommandInvocation,
    ) -> Result<AgentCommandResult, AgentError> {
        self.invoke_command(command).await
    }

    async fn interrupt_turn(&self) -> Result<(), AgentError> {
        self.interrupt().await
    }
}

pub struct AgentSessionHandle {
    inner: std::sync::Arc<dyn AgentSession>,
}

impl AgentSessionHandle {
    pub fn new(inner: std::sync::Arc<dyn AgentSession>) -> Self {
        Self { inner }
    }
}

#[async_trait]
impl AgentSession for AgentSessionHandle {
    fn id(&self) -> &SessionId {
        self.inner.id()
    }

    fn instance_id(&self) -> &InstanceId {
        self.inner.instance_id()
    }

    fn provider_id(&self) -> ProviderId {
        self.inner.provider_id()
    }

    fn process_id(&self) -> Option<i32> {
        self.inner.process_id()
    }

    async fn submit(&self, input: AgentInput) -> Result<(), AgentError> {
        self.inner.submit(input).await
    }

    async fn list_commands(&self) -> Result<AgentCommandCatalog, AgentError> {
        self.inner.list_commands().await
    }

    async fn invoke_command(
        &self,
        command: AgentCommandInvocation,
    ) -> Result<AgentCommandResult, AgentError> {
        self.inner.invoke_command(command).await
    }

    async fn list_models(&self) -> Result<AgentModelCatalog, AgentError> {
        self.inner.list_models().await
    }

    async fn set_model(&self, selection: AgentModelSelection) -> Result<AgentStatus, AgentError> {
        self.inner.set_model(selection).await
    }

    async fn list_permissions(&self) -> Result<AgentPermissionCatalog, AgentError> {
        self.inner.list_permissions().await
    }

    async fn set_permissions(
        &self,
        selection: AgentPermissionSelection,
    ) -> Result<AgentStatus, AgentError> {
        self.inner.set_permissions(selection).await
    }

    async fn list_skills(&self) -> Result<AgentSkillCatalog, AgentError> {
        self.inner.list_skills().await
    }

    async fn list_fork_targets(&self) -> Result<AgentForkTargetCatalog, AgentError> {
        self.inner.list_fork_targets().await
    }

    async fn fork(&self, selection: AgentForkSelection) -> Result<AgentForkResult, AgentError> {
        self.inner.fork(selection).await
    }

    async fn status(&self) -> Result<AgentStatus, AgentError> {
        self.inner.status().await
    }

    async fn new(&self) -> Result<(), AgentError> {
        self.inner.new().await
    }

    async fn quit(&self) -> Result<(), AgentError> {
        self.inner.quit().await
    }

    async fn interrupt(&self) -> Result<(), AgentError> {
        self.inner.interrupt().await
    }

    async fn resolve(
        &self,
        req_id: &str,
        response: InterventionResponse,
    ) -> Result<(), AgentError> {
        self.inner.resolve(req_id, response).await
    }

    async fn take_events(&self) -> Result<AgentEventStream, AgentError> {
        self.inner.take_events().await
    }

    async fn take_activity_events(
        &self,
    ) -> Result<Option<crate::agent_runtime::AgentActivityStream>, AgentError> {
        self.inner.take_activity_events().await
    }

    async fn close(&self) -> Result<(), AgentError> {
        self.inner.close().await
    }

    async fn observed_close_reason(&self) -> Option<SmolStr> {
        self.inner.observed_close_reason().await
    }

    async fn provider_session_id(&self) -> Option<SessionId> {
        self.inner.provider_session_id().await
    }
}

#[async_trait]
impl<T> DaemonSession for T where T: AgentSession + ?Sized {}

pub struct OpenedCoreSession {
    pub workspace: LiveWorkspace,
    pub session: std::sync::Arc<dyn DaemonSession>,
    pub events: super::CoreWorkspaceEventStream,
}
