use std::{path::PathBuf, time::Duration};

/// Global timeout configuration shared across all channels.
#[derive(Debug, Clone)]
pub struct CoreOptions {
    /// How long a submitted turn can be completely silent (no events)
    /// before the core watchdog fails it.
    pub turn_inactivity: Duration,
    /// Maximum time a submitted turn may remain incomplete after last activity.
    pub turn_deadline: Duration,
    /// Default idle timeout for agent sessions (auto-close when idle).
    /// Overridable per-session via `idle_timeout_ms`.
    pub session_idle_timeout: Duration,
}

impl Default for CoreOptions {
    fn default() -> Self {
        Self {
            turn_inactivity: Duration::from_secs(1800),
            turn_deadline: Duration::from_secs(3600),
            session_idle_timeout: Duration::from_secs(7200),
        }
    }
}

mod api;
mod events;
mod service;
mod types;

pub use api::DaemonApi;
pub use events::{
    CoreEvent, CoreEventReceiver, CoreWorkspaceEventRecvError, CoreWorkspaceEventStream,
    CoreWorkspaceEventTryRecvError,
};
pub use service::{CoreError, HistoryWatchState, HistoryWatchStatus, LucarneCore};
pub use types::{
    render_agent_resource_snapshot, render_kill_agent_report, AgentResourceEntry,
    AgentResourceScope, AgentResourceSnapshot, AgentSessionHandle, CloseWorkspaceRequest,
    CoreTimelineEvent, DaemonSession, HistoryPage, HistoryProviderCatalogEntry,
    HistoryWorkspacePage, InterruptTurnRequest, InvokeCommandRequest, KillAgentReport,
    KillAgentRequest, KillAgentTarget, KilledAgent, LiveInstanceProjection, LiveWorkspace,
    ObservedAgentSession, OpenWorkspaceRequest, OpenedCoreSession, ProviderCatalogEntry,
    ProviderSessionProjection, ResolvePermissionRequest, ResumeWorkspaceRequest,
    RunDueScheduledTasksRequest, RunScheduledTasksReport, ScheduledTaskRun, ScheduledTaskRunError,
    SubmitTurnRequest, SubmittedTurn, SystemSettings, TimelineKindProjection,
    TimelineProjectionItem, UpsertScheduledTaskRequest, WorkspaceSummary,
};

pub fn default_lucarned_home_dir() -> Option<PathBuf> {
    std::env::var_os("HOME")
        .filter(|value| !value.is_empty())
        .map(|home| PathBuf::from(home).join(".lucarned"))
}

pub fn default_state_db_path() -> Option<PathBuf> {
    default_lucarned_home_dir().map(|home| home.join("state.sqlite3"))
}

#[cfg(test)]
mod tests {
    use super::default_state_db_path;

    #[test]
    fn default_state_db_path_is_under_lucarned_home() {
        let path = default_state_db_path().expect("state db path");

        assert!(path.ends_with(".lucarned/state.sqlite3"));
        assert!(!path.starts_with("./data"));
    }
}
