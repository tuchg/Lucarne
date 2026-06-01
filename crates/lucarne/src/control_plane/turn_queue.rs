use super::types::WorkspaceId;
use std::{
    collections::{HashMap, VecDeque},
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc, Mutex as StdMutex, RwLock as StdRwLock,
    },
};
use tokio::sync::oneshot;
use tracing::debug;

#[derive(Debug)]
pub enum TurnAdmission {
    Ready(TurnPermit),
    Queued(QueuedTurn),
}

impl TurnAdmission {
    pub fn queued_position(&self) -> Option<usize> {
        match self {
            Self::Ready(permit) => permit.queued_position(),
            Self::Queued(queued) => Some(queued.position),
        }
    }

    pub async fn wait(self) -> TurnPermit {
        match self {
            Self::Ready(permit) => permit,
            Self::Queued(queued) => queued.wait().await,
        }
    }
}

#[derive(Debug)]
pub struct QueuedTurn {
    workspace_id: WorkspaceId,
    position: usize,
    scheduler: TurnScheduler,
    waiter: Option<Arc<QueuedWaiter>>,
    receiver: Option<oneshot::Receiver<()>>,
    counted: bool,
}

impl QueuedTurn {
    pub fn position(&self) -> usize {
        self.position
    }

    pub async fn wait(mut self) -> TurnPermit {
        self.receiver
            .take()
            .expect("queued turn receiver")
            .await
            .expect("queued turn grant");
        self.scheduler.note_handoff_started(&self.workspace_id);
        self.counted = false;
        self.waiter = None;
        TurnPermit {
            queued_position: Some(self.position),
            workspace_id: self.workspace_id.clone(),
            scheduler: self.scheduler.clone(),
        }
    }
}

impl Drop for QueuedTurn {
    fn drop(&mut self) {
        if self.counted {
            if let Some(waiter) = self.waiter.take() {
                waiter.cancel();
                match self.scheduler.cancel_queued(&self.workspace_id, waiter.id) {
                    CancelOutcome::Removed => {}
                    CancelOutcome::PendingHandoff => {
                        self.scheduler.release_next(&self.workspace_id);
                    }
                    CancelOutcome::Missing => {}
                }
            }
        }
    }
}

#[derive(Debug)]
pub struct TurnPermit {
    queued_position: Option<usize>,
    workspace_id: WorkspaceId,
    scheduler: TurnScheduler,
}

impl TurnPermit {
    pub fn queued_position(&self) -> Option<usize> {
        self.queued_position
    }
}

impl Drop for TurnPermit {
    fn drop(&mut self) {
        self.scheduler.release_next(&self.workspace_id);
    }
}

#[derive(Debug, Clone, Default)]
pub struct TurnScheduler {
    queues: Arc<StdRwLock<HashMap<WorkspaceId, Arc<StdMutex<WorkspaceQueue>>>>>,
    next_waiter_id: Arc<AtomicU64>,
}

impl TurnScheduler {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn admit(&self, workspace_id: &WorkspaceId) -> TurnAdmission {
        let queue = self.workspace_queue(workspace_id);
        let mut state = queue.lock().expect("turn scheduler workspace queue");
        if !state.active && !state.handoff_pending && state.waiters.is_empty() {
            state.active = true;
            debug!(
                target: "lucarne::control_plane::turn_queue",
                workspace_id = %workspace_id.as_str(),
                "turn admitted"
            );
            return TurnAdmission::Ready(TurnPermit {
                queued_position: None,
                workspace_id: workspace_id.clone(),
                scheduler: self.clone(),
            });
        }

        let position = state.waiters.len() + usize::from(state.handoff_pending) + 1;
        let (tx, rx) = oneshot::channel();
        let waiter = Arc::new(QueuedWaiter {
            id: self.next_waiter_id.fetch_add(1, Ordering::Relaxed),
            grant: StdMutex::new(Some(tx)),
        });
        state.waiters.push_back(Arc::clone(&waiter));
        debug!(
            target: "lucarne::control_plane::turn_queue",
            workspace_id = %workspace_id.as_str(),
            position,
            "turn queued"
        );
        TurnAdmission::Queued(QueuedTurn {
            workspace_id: workspace_id.clone(),
            position,
            scheduler: self.clone(),
            waiter: Some(waiter),
            receiver: Some(rx),
            counted: true,
        })
    }

    pub async fn acquire(&self, workspace_id: &WorkspaceId) -> TurnPermit {
        self.admit(workspace_id).wait().await
    }

    fn workspace_queue(&self, workspace_id: &WorkspaceId) -> Arc<StdMutex<WorkspaceQueue>> {
        if let Some(queue) = self
            .queues
            .read()
            .expect("turn scheduler lock registry")
            .get(workspace_id)
            .cloned()
        {
            return queue;
        }

        self.queues
            .write()
            .expect("turn scheduler lock registry")
            .entry(workspace_id.clone())
            .or_insert_with(|| Arc::new(StdMutex::new(WorkspaceQueue::default())))
            .clone()
    }

    fn release_next(&self, workspace_id: &WorkspaceId) {
        let Some(queue) = self.existing_workspace_queue(workspace_id) else {
            return;
        };
        loop {
            let next = {
                let mut state = queue.lock().expect("turn scheduler workspace queue");
                state.handoff_pending = false;
                match state.waiters.pop_front() {
                    Some(waiter) => {
                        state.active = true;
                        state.handoff_pending = true;
                        Some(waiter)
                    }
                    None => {
                        state.active = false;
                        None
                    }
                }
            };
            let Some(waiter) = next else {
                return;
            };
            if waiter.grant() {
                return;
            }
        }
    }

    fn note_handoff_started(&self, workspace_id: &WorkspaceId) {
        if let Some(queue) = self.existing_workspace_queue(workspace_id) {
            queue
                .lock()
                .expect("turn scheduler workspace queue")
                .handoff_pending = false;
        }
    }

    fn cancel_queued(&self, workspace_id: &WorkspaceId, waiter_id: u64) -> CancelOutcome {
        let Some(queue) = self.existing_workspace_queue(workspace_id) else {
            return CancelOutcome::Missing;
        };
        let mut state = queue.lock().expect("turn scheduler workspace queue");
        if let Some(index) = state
            .waiters
            .iter()
            .position(|waiter| waiter.id == waiter_id)
        {
            state.waiters.remove(index);
            return CancelOutcome::Removed;
        }
        if state.handoff_pending {
            state.handoff_pending = false;
            return CancelOutcome::PendingHandoff;
        }
        CancelOutcome::Missing
    }

    fn existing_workspace_queue(
        &self,
        workspace_id: &WorkspaceId,
    ) -> Option<Arc<StdMutex<WorkspaceQueue>>> {
        self.queues
            .read()
            .expect("turn scheduler lock registry")
            .get(workspace_id)
            .cloned()
    }
}

#[derive(Debug, Default)]
struct WorkspaceQueue {
    active: bool,
    handoff_pending: bool,
    waiters: VecDeque<Arc<QueuedWaiter>>,
}

#[derive(Debug)]
struct QueuedWaiter {
    id: u64,
    grant: StdMutex<Option<oneshot::Sender<()>>>,
}

impl QueuedWaiter {
    fn grant(&self) -> bool {
        self.grant
            .lock()
            .expect("turn scheduler queued waiter")
            .take()
            .is_some_and(|grant| grant.send(()).is_ok())
    }

    fn cancel(&self) {
        self.grant
            .lock()
            .expect("turn scheduler queued waiter")
            .take();
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum CancelOutcome {
    Removed,
    PendingHandoff,
    Missing,
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;
    use tokio::time::{sleep, timeout};

    #[tokio::test]
    async fn same_workspace_turns_are_admitted_serially_with_position() {
        let scheduler = TurnScheduler::new();
        let workspace = WorkspaceId::new("ws");
        let first = scheduler.acquire(&workspace).await;
        assert_eq!(first.queued_position(), None);

        let second_scheduler = scheduler.clone();
        let second_workspace = workspace.clone();
        let second_admission = second_scheduler.admit(&second_workspace);
        assert_eq!(second_admission.queued_position(), Some(1));
        let second = tokio::spawn(async move { second_admission.wait().await });
        sleep(Duration::from_millis(20)).await;
        assert!(
            !second.is_finished(),
            "second turn must wait while the first permit is alive"
        );

        drop(first);
        let second = timeout(Duration::from_secs(1), second)
            .await
            .expect("second turn should acquire after first completes")
            .expect("second turn task");
        assert_eq!(second.queued_position(), Some(1));
    }

    #[test]
    fn turn_scheduler_emits_structured_tracing() {
        let source = std::fs::read_to_string(
            std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
                .join("src/control_plane/turn_queue.rs"),
        )
        .expect("read turn queue source");
        let production = source
            .split("#[cfg(test)]")
            .next()
            .expect("production source");

        for needle in [
            "lucarne::control_plane::turn_queue",
            "turn admitted",
            "turn queued",
        ] {
            assert!(
                production.contains(needle),
                "turn scheduler tracing must cover admission boundary: {needle}"
            );
        }
    }

    #[test]
    fn turn_scheduler_uses_read_optimized_workspace_queue_registry() {
        let source = std::fs::read_to_string(
            std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
                .join("src/control_plane/turn_queue.rs"),
        )
        .expect("read turn queue source");
        let production = source
            .split("#[cfg(test)]")
            .next()
            .expect("production source");

        assert!(
            production.contains(
                "queues: Arc<StdRwLock<HashMap<WorkspaceId, Arc<StdMutex<WorkspaceQueue>>>>>"
            ),
            "workspace queue registry should allow read-only lookups without taking an exclusive map lock"
        );
        assert!(
            !production.contains("queues: Arc<StdMutex<HashMap<WorkspaceId, WorkspaceQueue>>>"),
            "workspace queue registry should not use a global mutex for every turn admission"
        );
    }

    #[tokio::test]
    async fn dropped_queued_turn_releases_waiter_position() {
        let scheduler = TurnScheduler::new();
        let workspace = WorkspaceId::new("ws");
        let _first = scheduler.acquire(&workspace).await;

        let dropped = scheduler.admit(&workspace);
        assert_eq!(dropped.queued_position(), Some(1));
        drop(dropped);

        let next = scheduler.admit(&workspace);
        assert_eq!(next.queued_position(), Some(1));
    }

    #[tokio::test]
    async fn queued_turn_reserves_fifo_position_before_wait_is_called() {
        let scheduler = TurnScheduler::new();
        let workspace = WorkspaceId::new("ws");
        let first = scheduler.acquire(&workspace).await;

        let queued = scheduler.admit(&workspace);
        assert_eq!(queued.queued_position(), Some(1));
        drop(first);

        sleep(Duration::from_millis(20)).await;
        let later = scheduler.admit(&workspace);
        assert_eq!(
            later.queued_position(),
            Some(2),
            "a later turn must not overtake a queued turn while the caller sends its queue notice"
        );

        drop(queued);
        timeout(Duration::from_secs(1), later.wait())
            .await
            .expect("later turn should acquire after queued turn is cancelled");
    }

    #[tokio::test]
    async fn later_turn_cannot_overtake_unpolled_queued_turn_after_release() {
        let scheduler = TurnScheduler::new();
        let workspace = WorkspaceId::new("ws");
        let first = scheduler.acquire(&workspace).await;

        let queued = scheduler.admit(&workspace);
        assert_eq!(queued.queued_position(), Some(1));
        drop(first);

        let later = scheduler.admit(&workspace);
        assert_eq!(
            later.queued_position(),
            Some(2),
            "a later turn must queue behind the older turn even before the older caller awaits its permit"
        );

        let later = tokio::spawn(async move { later.wait().await });
        sleep(Duration::from_millis(20)).await;
        assert!(
            !later.is_finished(),
            "later turn must not acquire before the older queued turn is awaited and released"
        );

        let queued = timeout(Duration::from_secs(1), queued.wait())
            .await
            .expect("older queued turn should acquire");
        drop(queued);
        let later = timeout(Duration::from_secs(1), later)
            .await
            .expect("later turn should acquire after older queued turn releases")
            .expect("later turn task");
        assert_eq!(later.queued_position(), Some(2));
    }

    #[tokio::test]
    async fn different_workspaces_are_admitted_independently() {
        let scheduler = TurnScheduler::new();
        let first = scheduler.acquire(&WorkspaceId::new("ws-1")).await;
        let second = scheduler.acquire(&WorkspaceId::new("ws-2")).await;

        assert_eq!(first.queued_position(), None);
        assert_eq!(second.queued_position(), None);
    }
}
