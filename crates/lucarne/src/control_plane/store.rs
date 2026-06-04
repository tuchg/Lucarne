use std::{
    path::Path,
    sync::{Arc, Mutex},
};

use rusqlite::{params, types::ValueRef, Connection, OptionalExtension};
use serde::{de::DeserializeOwned, Serialize};

use super::{
    message_session_binding_id, ChannelBinding, ChannelBindingId, CommandCallbackRecord,
    CommandCallbackToken, CommandId, CommandWorkflow, ControlPlanePersistenceEntity,
    ControlPlaneState, HistoryOlderCallbackRecord, HistoryOlderCallbackToken, HistoryReplayRecord,
    InterventionCallbackRecord, InterventionCallbackToken, LiveInstanceId, LiveInstanceRecord,
    MessageSessionBinding, PanelRenderId, PanelRenderRecord, ProviderSessionId,
    ProviderSessionRecord, ReconcileOutcome, Revision, ScheduledTaskId, ScheduledTaskRecord,
    SubAgentCallbackRecord, SubAgentCallbackToken, SubAgentLinkId, SubAgentLinkRecord,
    TimelineItem, TimelineSeq, TurnId, TurnRecord, WorkspaceBinding, WorkspaceId,
};
use tracing::{debug, info};

#[derive(Debug, thiserror::Error)]
pub enum ControlPlaneStoreError {
    #[error("control-plane sqlite: {0}")]
    Sqlite(#[from] rusqlite::Error),
    #[error("control-plane json: {0}")]
    Json(#[from] serde_json::Error),
    #[error("control-plane io: {0}")]
    Io(#[from] std::io::Error),
}

struct SerializedEntity {
    kind: String,
    entity_id: String,
    workspace_id: Option<String>,
    state_json: String,
}

const LOOKUP_INDEX_VERSION_KEY: &str = "lookup_index_version";
const LOOKUP_INDEX_VERSION: &str = "1";

fn is_hot_snapshot_kind(kind: &str) -> bool {
    matches!(kind, "meta" | "system_settings")
}

#[derive(Clone)]
pub struct ControlPlaneSqliteStore {
    conn: Arc<Mutex<Connection>>,
}

impl ControlPlaneSqliteStore {
    pub fn open(path: impl AsRef<Path>) -> Result<Self, ControlPlaneStoreError> {
        if let Some(parent) = path.as_ref().parent() {
            if !parent.as_os_str().is_empty() {
                std::fs::create_dir_all(parent)?;
            }
        }
        info!(
            target: "lucarne::control_plane::store",
            path = ?path.as_ref(),
            "control-plane sqlite opened"
        );
        let conn = Connection::open(path)?;
        Self::from_connection(conn)
    }

    pub fn open_in_memory() -> Result<Self, ControlPlaneStoreError> {
        Self::from_connection(Connection::open_in_memory()?)
    }

    fn from_connection(mut conn: Connection) -> Result<Self, ControlPlaneStoreError> {
        conn.execute_batch("PRAGMA mmap_size = 268435456; PRAGMA cache_size = -100;")?;
        conn.execute_batch(
            "DROP TABLE IF EXISTS work_sessions;
            CREATE TABLE IF NOT EXISTS control_plane_entities (
                kind TEXT NOT NULL,
                entity_id TEXT NOT NULL,
                workspace_id TEXT,
                state_json TEXT NOT NULL,
                PRIMARY KEY(kind, entity_id)
            );
            CREATE INDEX IF NOT EXISTS idx_control_plane_entities_workspace
                ON control_plane_entities(workspace_id, kind);
            CREATE INDEX IF NOT EXISTS idx_control_plane_entities_kind
                ON control_plane_entities(kind);
            CREATE TABLE IF NOT EXISTS control_plane_store_meta (
                key TEXT NOT NULL PRIMARY KEY,
                value TEXT NOT NULL
            ) WITHOUT ROWID;
            CREATE TABLE IF NOT EXISTS control_plane_workspace_provider_session_index (
                workspace_id TEXT NOT NULL PRIMARY KEY,
                provider_session_id TEXT NOT NULL
            ) WITHOUT ROWID;
            CREATE INDEX IF NOT EXISTS idx_control_plane_workspace_provider_session_provider
                ON control_plane_workspace_provider_session_index(provider_session_id, workspace_id);
            CREATE TABLE IF NOT EXISTS control_plane_message_provider_session_index (
                binding_id TEXT NOT NULL PRIMARY KEY,
                provider_session_id TEXT NOT NULL
            ) WITHOUT ROWID;
            CREATE INDEX IF NOT EXISTS idx_control_plane_message_provider_session_provider
                ON control_plane_message_provider_session_index(provider_session_id, binding_id);
            CREATE TABLE IF NOT EXISTS control_plane_subagent_parent_turn_index (
                link_id TEXT NOT NULL PRIMARY KEY,
                turn_id TEXT NOT NULL
            ) WITHOUT ROWID;
            CREATE INDEX IF NOT EXISTS idx_control_plane_subagent_parent_turn_turn
                ON control_plane_subagent_parent_turn_index(turn_id, link_id);",
        )?;
        ensure_lookup_indexes_current(&mut conn)?;
        Ok(Self {
            conn: Arc::new(Mutex::new(conn)),
        })
    }

    pub fn clone_connection(&self) -> Arc<Mutex<Connection>> {
        Arc::clone(&self.conn)
    }

    pub fn has_any_record(&self) -> Result<bool, ControlPlaneStoreError> {
        let conn = self.conn.lock().unwrap();
        let count: i64 =
            conn.query_row("SELECT COUNT(*) FROM control_plane_entities", [], |row| {
                row.get(0)
            })?;
        Ok(count > 0)
    }

    pub fn entity_state<T: DeserializeOwned>(
        &self,
        kind: &str,
        entity_id: &str,
    ) -> Result<Option<T>, ControlPlaneStoreError> {
        let conn = self.conn.lock().unwrap();
        let state_json = conn
            .query_row(
                "SELECT state_json
                 FROM control_plane_entities
                 WHERE kind = ?1 AND entity_id = ?2",
                params![kind, entity_id],
                |row| row.get::<_, String>(0),
            )
            .optional()?;
        state_json
            .map(|state_json| serde_json::from_str(&state_json).map_err(Into::into))
            .transpose()
    }

    pub fn entities_by_kind<T: DeserializeOwned>(
        &self,
        kind: &str,
    ) -> Result<Vec<T>, ControlPlaneStoreError> {
        let conn = self.conn.lock().unwrap();
        let mut stmt = conn.prepare(
            "SELECT state_json
             FROM control_plane_entities
             WHERE kind = ?1
             ORDER BY entity_id",
        )?;
        let mut rows = stmt.query(params![kind])?;
        let mut records = Vec::new();
        while let Some(row) = rows.next()? {
            let state_json: String = row.get(0)?;
            records.push(serde_json::from_str(&state_json)?);
        }
        Ok(records)
    }

    fn entities_by_workspace<T: DeserializeOwned>(
        &self,
        kind: &str,
        workspace_id: &WorkspaceId,
    ) -> Result<Vec<T>, ControlPlaneStoreError> {
        let conn = self.conn.lock().unwrap();
        let mut stmt = conn.prepare(
            "SELECT state_json
             FROM control_plane_entities
             WHERE kind = ?1 AND workspace_id = ?2
             ORDER BY entity_id",
        )?;
        let mut rows = stmt.query(params![kind, workspace_id.as_str()])?;
        let mut records = Vec::new();
        while let Some(row) = rows.next()? {
            let state_json: String = row.get(0)?;
            records.push(serde_json::from_str(&state_json)?);
        }
        Ok(records)
    }

    pub fn load_control_plane(&self) -> Result<Option<ControlPlaneState>, ControlPlaneStoreError> {
        let conn = self.conn.lock().unwrap();
        let mut stmt = conn.prepare(
            "SELECT kind, entity_id, workspace_id, state_json
             FROM control_plane_entities
             WHERE kind IN (
                'meta',
                'system_settings'
             )",
        )?;
        let mut rows = stmt.query([])?;
        let mut state = ControlPlaneState::default();
        let mut entity_count = 0usize;
        while let Some(row) = rows.next()? {
            let kind: String = row.get(0)?;
            let workspace_id: Option<String> = row.get(2)?;
            let state_json = state_json_bytes(row.get_ref(3)?)?;
            state.apply_persistence_entity_json(
                kind.as_str(),
                workspace_id.as_deref(),
                state_json,
            )?;
            entity_count += 1;
        }
        if entity_count == 0 {
            debug!(
                target: "lucarne::control_plane::store",
                "control-plane store empty"
            );
            return Ok(None);
        }
        debug!(
            target: "lucarne::control_plane::store",
            entity_count,
            "control-plane hot entities loaded (cold records deferred)"
        );
        state.set_timeline_store(Arc::clone(&self.conn));
        Ok(Some(state))
    }

    pub fn provider_session(
        &self,
        provider_session_id: &ProviderSessionId,
    ) -> Result<Option<ProviderSessionRecord>, ControlPlaneStoreError> {
        self.entity_state("provider_session", provider_session_id.as_str())
    }

    pub fn workspace(
        &self,
        workspace_id: &WorkspaceId,
    ) -> Result<Option<WorkspaceBinding>, ControlPlaneStoreError> {
        self.entity_state("workspace", workspace_id.as_str())
    }

    pub fn workspace_bindings(&self) -> Result<Vec<WorkspaceBinding>, ControlPlaneStoreError> {
        self.entities_by_kind("workspace")
    }

    pub fn workspace_id_for_live_instance(
        &self,
        live_instance_id: &LiveInstanceId,
    ) -> Result<Option<WorkspaceId>, ControlPlaneStoreError> {
        let conn = self.conn.lock().unwrap();
        let workspace_id = conn
            .query_row(
                "SELECT workspace_id
                 FROM control_plane_entities
                 WHERE kind = 'live_instance' AND entity_id = ?1",
                params![live_instance_id.as_str()],
                |row| row.get::<_, Option<String>>(0),
            )
            .optional()?
            .flatten();
        Ok(workspace_id.map(WorkspaceId::new))
    }

    pub fn live_instance(
        &self,
        live_instance_id: &LiveInstanceId,
    ) -> Result<Option<LiveInstanceRecord>, ControlPlaneStoreError> {
        self.entity_state("live_instance", live_instance_id.as_str())
    }

    pub fn live_instances_for_restart_cleanup(
        &self,
    ) -> Result<Vec<(Option<WorkspaceId>, LiveInstanceRecord)>, ControlPlaneStoreError> {
        let conn = self.conn.lock().unwrap();
        let mut stmt = conn.prepare(
            "SELECT workspace_id, state_json
             FROM control_plane_entities
             WHERE kind = 'live_instance'
             ORDER BY entity_id",
        )?;
        let mut rows = stmt.query([])?;
        let mut records = Vec::new();
        while let Some(row) = rows.next()? {
            let workspace_id: Option<String> = row.get(0)?;
            let state_json: String = row.get(1)?;
            let live = serde_json::from_str::<LiveInstanceRecord>(&state_json)?;
            records.push((workspace_id.map(WorkspaceId::new), live));
        }
        Ok(records)
    }

    pub fn turn(&self, turn_id: &TurnId) -> Result<Option<TurnRecord>, ControlPlaneStoreError> {
        self.entity_state("turn", turn_id.as_str())
    }

    pub fn turns_for_workspace(
        &self,
        workspace_id: &WorkspaceId,
    ) -> Result<Vec<TurnRecord>, ControlPlaneStoreError> {
        self.entities_by_workspace("turn", workspace_id)
    }

    pub fn command(
        &self,
        command_id: &CommandId,
    ) -> Result<Option<CommandWorkflow>, ControlPlaneStoreError> {
        self.entity_state("command", command_id.as_str())
    }

    pub fn command_workflows_for_workspace(
        &self,
        workspace_id: &WorkspaceId,
    ) -> Result<Vec<CommandWorkflow>, ControlPlaneStoreError> {
        self.entities_by_workspace("command", workspace_id)
    }

    pub fn command_callback(
        &self,
        token: &CommandCallbackToken,
    ) -> Result<Option<CommandCallbackRecord>, ControlPlaneStoreError> {
        self.entity_state("command_callback", token.as_str())
    }

    pub fn intervention_callback(
        &self,
        token: &InterventionCallbackToken,
    ) -> Result<Option<InterventionCallbackRecord>, ControlPlaneStoreError> {
        self.entity_state("intervention_callback", token.as_str())
    }

    pub fn delete_intervention_callbacks_for_live_instances(
        &self,
        live_instance_ids: &[LiveInstanceId],
    ) -> Result<(), ControlPlaneStoreError> {
        if live_instance_ids.is_empty() {
            return Ok(());
        }
        let live_instance_ids = live_instance_ids
            .iter()
            .cloned()
            .collect::<std::collections::HashSet<_>>();
        self.delete_intervention_callbacks_matching(|callback| {
            live_instance_ids.contains(&callback.live_instance_id)
        })?;
        Ok(())
    }

    pub fn delete_intervention_callbacks_for_request(
        &self,
        live_instance_id: &LiveInstanceId,
        req_id: &str,
    ) -> Result<(), ControlPlaneStoreError> {
        self.delete_intervention_callbacks_matching(|callback| {
            callback.live_instance_id == *live_instance_id && callback.req_id.as_str() == req_id
        })?;
        Ok(())
    }

    fn delete_intervention_callbacks_matching(
        &self,
        matches: impl Fn(&InterventionCallbackRecord) -> bool,
    ) -> Result<usize, ControlPlaneStoreError> {
        let mut conn = self.conn.lock().unwrap();
        let mut stmt = conn.prepare(
            "SELECT entity_id, state_json
             FROM control_plane_entities
             WHERE kind = 'intervention_callback'
             ORDER BY entity_id",
        )?;
        let mut rows = stmt.query([])?;
        let mut entity_ids = Vec::new();
        while let Some(row) = rows.next()? {
            let entity_id: String = row.get(0)?;
            let state_json: String = row.get(1)?;
            let callback = serde_json::from_str::<InterventionCallbackRecord>(&state_json)?;
            if matches(&callback) {
                entity_ids.push(entity_id);
            }
        }
        drop(rows);
        drop(stmt);
        if entity_ids.is_empty() {
            return Ok(0);
        }
        let removed = entity_ids.len();
        let tx = conn.transaction()?;
        {
            let mut delete = tx.prepare(
                "DELETE FROM control_plane_entities
                 WHERE kind = 'intervention_callback' AND entity_id = ?1",
            )?;
            for entity_id in entity_ids {
                delete.execute(params![entity_id])?;
            }
        }
        tx.commit()?;
        Ok(removed)
    }

    pub fn history_replay(
        &self,
        workspace_id: &WorkspaceId,
    ) -> Result<Option<HistoryReplayRecord>, ControlPlaneStoreError> {
        self.entity_state("history_replay", workspace_id.as_str())
    }

    pub fn history_older_callback(
        &self,
        token: &HistoryOlderCallbackToken,
    ) -> Result<Option<HistoryOlderCallbackRecord>, ControlPlaneStoreError> {
        self.entity_state("history_older_callback", token.as_str())
    }

    pub fn channel_binding(
        &self,
        binding_id: &ChannelBindingId,
    ) -> Result<Option<ChannelBinding>, ControlPlaneStoreError> {
        self.entity_state("channel_binding", binding_id.as_str())
    }

    pub fn channel_bindings_for_workspace(
        &self,
        workspace_id: &WorkspaceId,
    ) -> Result<Vec<ChannelBinding>, ControlPlaneStoreError> {
        self.entities_by_workspace("channel_binding", workspace_id)
    }

    pub fn reconcile_outcome(
        &self,
        workspace_id: &WorkspaceId,
    ) -> Result<Option<ReconcileOutcome>, ControlPlaneStoreError> {
        let conn = self.conn.lock().unwrap();
        let state_json = conn
            .query_row(
                "SELECT state_json
                 FROM control_plane_entities
                 WHERE kind = 'reconcile_outcome' AND entity_id = ?1",
                params![workspace_id.as_str()],
                |row| row.get::<_, String>(0),
            )
            .optional()?;
        state_json
            .map(|state_json| serde_json::from_str(&state_json).map_err(Into::into))
            .transpose()
    }

    pub fn panel_render(
        &self,
        panel_id: &PanelRenderId,
    ) -> Result<Option<PanelRenderRecord>, ControlPlaneStoreError> {
        self.entity_state("panel_render", panel_id.as_str())
    }

    pub fn panel_renders(&self) -> Result<Vec<PanelRenderRecord>, ControlPlaneStoreError> {
        self.entities_by_kind("panel_render")
    }

    pub fn max_panel_render_revision(&self) -> Result<Option<Revision>, ControlPlaneStoreError> {
        Ok(self
            .entities_by_kind::<PanelRenderRecord>("panel_render")?
            .into_iter()
            .map(|panel| panel.last_rendered_revision)
            .max())
    }

    pub fn scheduled_task(
        &self,
        task_id: &ScheduledTaskId,
    ) -> Result<Option<ScheduledTaskRecord>, ControlPlaneStoreError> {
        self.entity_state("scheduled_task", task_id.as_str())
    }

    pub fn due_scheduled_tasks(
        &self,
        now_unix_ms: u64,
    ) -> Result<Vec<ScheduledTaskRecord>, ControlPlaneStoreError> {
        Ok(self
            .entities_by_kind::<ScheduledTaskRecord>("scheduled_task")?
            .into_iter()
            .filter(|task| task.enabled && task.next_run_unix_ms <= now_unix_ms)
            .collect())
    }

    pub fn subagent_links_for_turn(
        &self,
        turn_id: &TurnId,
    ) -> Result<Vec<SubAgentLinkRecord>, ControlPlaneStoreError> {
        let conn = self.conn.lock().unwrap();
        let mut stmt = conn.prepare(
            "SELECT entities.state_json
             FROM control_plane_subagent_parent_turn_index idx
             JOIN control_plane_entities entities
               ON entities.kind = 'subagent_link'
              AND entities.entity_id = idx.link_id
             WHERE idx.turn_id = ?1
             ORDER BY idx.link_id",
        )?;
        let mut rows = stmt.query(params![turn_id.as_str()])?;
        let mut links = Vec::new();
        while let Some(row) = rows.next()? {
            let state_json: String = row.get(0)?;
            links.push(serde_json::from_str(&state_json)?);
        }
        Ok(links)
    }

    pub fn subagent_links_for_workspace(
        &self,
        workspace_id: &WorkspaceId,
    ) -> Result<Vec<SubAgentLinkRecord>, ControlPlaneStoreError> {
        self.entities_by_workspace("subagent_link", workspace_id)
    }

    pub fn subagent_link(
        &self,
        link_id: &SubAgentLinkId,
    ) -> Result<Option<SubAgentLinkRecord>, ControlPlaneStoreError> {
        self.entity_state("subagent_link", link_id.as_str())
    }

    pub fn subagent_callback(
        &self,
        token: &SubAgentCallbackToken,
    ) -> Result<Option<SubAgentCallbackRecord>, ControlPlaneStoreError> {
        self.entity_state("subagent_callback", token.as_str())
    }

    pub fn workspace_for_provider_session(
        &self,
        provider_session_id: &ProviderSessionId,
    ) -> Result<Option<WorkspaceBinding>, ControlPlaneStoreError> {
        let conn = self.conn.lock().unwrap();
        let state_json = conn
            .query_row(
                "SELECT entities.state_json
                 FROM control_plane_workspace_provider_session_index idx
                 JOIN control_plane_entities entities
                   ON entities.kind = 'workspace'
                  AND entities.entity_id = idx.workspace_id
                 WHERE idx.provider_session_id = ?1
                 ORDER BY idx.workspace_id
                 LIMIT 1",
                params![provider_session_id.as_str()],
                |row| row.get::<_, String>(0),
            )
            .optional()?;
        state_json
            .map(|state_json| serde_json::from_str(&state_json).map_err(Into::into))
            .transpose()
    }

    pub fn provider_session_referenced_by_other_workspace(
        &self,
        provider_session_id: &ProviderSessionId,
        excluded_workspace_id: &WorkspaceId,
    ) -> Result<bool, ControlPlaneStoreError> {
        let conn = self.conn.lock().unwrap();
        let referenced = conn
            .query_row(
                "SELECT 1
                 FROM control_plane_workspace_provider_session_index
                 WHERE provider_session_id = ?1
                   AND workspace_id != ?2
                 LIMIT 1",
                params![provider_session_id.as_str(), excluded_workspace_id.as_str()],
                |_| Ok(()),
            )
            .optional()?
            .is_some();
        Ok(referenced)
    }

    pub fn message_session_bindings_for_provider_session(
        &self,
        provider_session_id: &ProviderSessionId,
    ) -> Result<Vec<MessageSessionBinding>, ControlPlaneStoreError> {
        let conn = self.conn.lock().unwrap();
        let mut stmt = conn.prepare(
            "SELECT entities.state_json
             FROM control_plane_message_provider_session_index idx
             JOIN control_plane_entities entities
               ON entities.kind = 'message_session_binding'
              AND entities.entity_id = idx.binding_id
             WHERE idx.provider_session_id = ?1
             ORDER BY idx.binding_id",
        )?;
        let mut rows = stmt.query(params![provider_session_id.as_str()])?;
        let mut bindings = Vec::new();
        while let Some(row) = rows.next()? {
            let state_json: String = row.get(0)?;
            bindings.push(serde_json::from_str(&state_json)?);
        }
        Ok(bindings)
    }

    pub fn delete_message_session_bindings_for_provider_session(
        &self,
        provider_session_id: &ProviderSessionId,
    ) -> Result<(), ControlPlaneStoreError> {
        for binding in self.message_session_bindings_for_provider_session(provider_session_id)? {
            self.delete_entity("message_session_binding", binding.binding_id.as_str())?;
        }
        Ok(())
    }

    pub fn message_session_binding(
        &self,
        channel: &str,
        chat_id: &str,
        message_id: &str,
    ) -> Result<Option<MessageSessionBinding>, ControlPlaneStoreError> {
        let conn = self.conn.lock().unwrap();
        let binding_id = message_session_binding_id(channel, chat_id, message_id);
        let state_json = conn
            .query_row(
                "SELECT state_json
                 FROM control_plane_entities
                 WHERE kind = 'message_session_binding' AND entity_id = ?1",
                params![binding_id.as_str()],
                |row| row.get::<_, String>(0),
            )
            .optional()?;
        state_json
            .map(|state_json| serde_json::from_str(&state_json).map_err(Into::into))
            .transpose()
    }

    pub fn timeline_item(
        &self,
        workspace_id: &WorkspaceId,
        seq: TimelineSeq,
    ) -> Result<Option<TimelineItem>, ControlPlaneStoreError> {
        let conn = self.conn.lock().unwrap();
        let entity_id = format!("{}:{}", workspace_id.as_str(), seq.get());
        let state_json = conn
            .query_row(
                "SELECT state_json
                 FROM control_plane_entities
                 WHERE kind = 'timeline' AND entity_id = ?1",
                params![entity_id],
                |row| row.get::<_, String>(0),
            )
            .optional()?;
        state_json
            .map(|state_json| serde_json::from_str(&state_json).map_err(Into::into))
            .transpose()
    }

    pub fn upsert_entity_state(
        &self,
        kind: &str,
        entity_id: &str,
        workspace_id: Option<&str>,
        state: &(impl Serialize + ?Sized),
    ) -> Result<(), ControlPlaneStoreError> {
        let state_json = serde_json::to_string(state)?;
        let entity = SerializedEntity {
            kind: kind.to_string(),
            entity_id: entity_id.to_string(),
            workspace_id: workspace_id.map(str::to_string),
            state_json,
        };
        let mut conn = self.conn.lock().unwrap();
        let tx = conn.transaction()?;
        upsert_serialized_entity(&tx, &entity)?;
        tx.commit()?;
        debug!(
            target: "lucarne::control_plane::store",
            kind,
            entity_id,
            "control-plane entity upserted"
        );
        Ok(())
    }

    pub fn delete_entity(
        &self,
        kind: &str,
        entity_id: &str,
    ) -> Result<bool, ControlPlaneStoreError> {
        let mut conn = self.conn.lock().unwrap();
        let tx = conn.transaction()?;
        delete_lookup_index(&tx, kind, entity_id)?;
        let removed = tx.execute(
            "DELETE FROM control_plane_entities WHERE kind = ?1 AND entity_id = ?2",
            params![kind, entity_id],
        )?;
        tx.commit()?;
        debug!(
            target: "lucarne::control_plane::store",
            kind,
            entity_id,
            removed,
            "control-plane entity deleted"
        );
        Ok(removed > 0)
    }

    pub fn delete_entities_by_kind(&self, kind: &str) -> Result<(), ControlPlaneStoreError> {
        let mut conn = self.conn.lock().unwrap();
        let tx = conn.transaction()?;
        delete_lookup_index_by_kind(&tx, kind)?;
        let removed = tx.execute(
            "DELETE FROM control_plane_entities WHERE kind = ?1",
            params![kind],
        )?;
        tx.commit()?;
        debug!(
            target: "lucarne::control_plane::store",
            kind,
            removed,
            "control-plane entities deleted by kind"
        );
        Ok(())
    }

    pub fn upsert_entities(
        &self,
        entities: Vec<ControlPlanePersistenceEntity>,
    ) -> Result<(), ControlPlaneStoreError> {
        let entities = serialize_entities(entities)?;
        let entity_count = entities.len();
        let mut conn = self.conn.lock().unwrap();
        let tx = conn.transaction()?;
        for entity in &entities {
            upsert_serialized_entity(&tx, entity)?;
        }
        tx.commit()?;
        debug!(
            target: "lucarne::control_plane::store",
            entity_count,
            "control-plane entities upserted"
        );
        Ok(())
    }

    /// Replace hot snapshot-owned entities in one transaction while preserving
    /// rows whose serialized state did not change. Cold rows are maintained by
    /// scoped upsert/delete APIs and are not owned by hot snapshots.
    pub fn replace_non_timeline_entities(
        &self,
        entities: Vec<ControlPlanePersistenceEntity>,
    ) -> Result<(), ControlPlaneStoreError> {
        let entities = entities
            .into_iter()
            .filter(|entity| is_hot_snapshot_kind(entity.kind.as_str()))
            .collect::<Vec<_>>();
        let entities = serialize_entities(entities)?;
        let entity_count = entities.len();
        if entity_count == 0 {
            debug!(
                target: "lucarne::control_plane::store",
                "control-plane hot snapshot replacement skipped with no hot entities"
            );
            return Ok(());
        }
        let mut conn = self.conn.lock().unwrap();
        let tx = conn.transaction()?;
        {
            tx.execute(
                "CREATE TEMP TABLE IF NOT EXISTS control_plane_replace_keys (
                    kind TEXT NOT NULL,
                    entity_id TEXT NOT NULL,
                    PRIMARY KEY(kind, entity_id)
                 ) WITHOUT ROWID",
                [],
            )?;
            tx.execute("DELETE FROM control_plane_replace_keys", [])?;
            {
                let mut key_stmt = tx.prepare(
                    "INSERT INTO control_plane_replace_keys (kind, entity_id)
                     VALUES (?1, ?2)",
                )?;
                for entity in &entities {
                    debug_assert_ne!(entity.kind, "timeline");
                    debug_assert!(is_hot_snapshot_kind(entity.kind.as_str()));
                    key_stmt.execute(params![entity.kind, entity.entity_id])?;
                }
            }
            tx.execute(
                "DELETE FROM control_plane_entities
                 WHERE kind IN ('meta', 'system_settings')
                   AND NOT EXISTS (
                       SELECT 1
                       FROM control_plane_replace_keys keys
                       WHERE keys.kind = control_plane_entities.kind
                         AND keys.entity_id = control_plane_entities.entity_id
                   )",
                [],
            )?;
            for entity in &entities {
                upsert_serialized_entity_if_changed(&tx, entity)?;
            }
            tx.execute("DELETE FROM control_plane_replace_keys", [])?;
        }
        tx.commit()?;
        debug!(
            target: "lucarne::control_plane::store",
            entity_count,
            "control-plane non-timeline entities replaced"
        );
        Ok(())
    }

    pub fn delete_workspace_entities(
        &self,
        workspace_id: &str,
    ) -> Result<(), ControlPlaneStoreError> {
        let mut conn = self.conn.lock().unwrap();
        let tx = conn.transaction()?;
        tx.execute(
            "DELETE FROM control_plane_workspace_provider_session_index
             WHERE workspace_id = ?1",
            params![workspace_id],
        )?;
        tx.execute(
            "DELETE FROM control_plane_message_provider_session_index
             WHERE binding_id IN (
                 SELECT entity_id
                 FROM control_plane_entities
                 WHERE workspace_id = ?1 AND kind = 'message_session_binding'
             )",
            params![workspace_id],
        )?;
        tx.execute(
            "DELETE FROM control_plane_subagent_parent_turn_index
             WHERE link_id IN (
                 SELECT entity_id
                 FROM control_plane_entities
                 WHERE workspace_id = ?1 AND kind = 'subagent_link'
             )",
            params![workspace_id],
        )?;
        tx.execute(
            "DELETE FROM control_plane_entities WHERE workspace_id = ?1",
            params![workspace_id],
        )?;
        tx.commit()?;
        debug!(
            target: "lucarne::control_plane::store",
            workspace_id,
            "control-plane workspace entities deleted"
        );
        Ok(())
    }

    pub fn replace_entities(
        &self,
        entities: Vec<ControlPlanePersistenceEntity>,
    ) -> Result<(), ControlPlaneStoreError> {
        let entities = serialize_entities(entities)?;
        let entity_count = entities.len();
        let mut conn = self.conn.lock().unwrap();
        let tx = conn.transaction()?;
        tx.execute("DELETE FROM control_plane_entities", [])?;
        clear_lookup_indexes(&tx)?;
        for entity in &entities {
            upsert_serialized_entity(&tx, entity)?;
        }
        tx.commit()?;
        debug!(
            target: "lucarne::control_plane::store",
            entity_count,
            "control-plane entities replaced"
        );
        Ok(())
    }
}

fn ensure_lookup_indexes_current(conn: &mut Connection) -> Result<(), ControlPlaneStoreError> {
    let current_version = conn
        .query_row(
            "SELECT value FROM control_plane_store_meta WHERE key = ?1",
            params![LOOKUP_INDEX_VERSION_KEY],
            |row| row.get::<_, String>(0),
        )
        .optional()?;
    if current_version.as_deref() == Some(LOOKUP_INDEX_VERSION) {
        return Ok(());
    }

    let tx = conn.transaction()?;
    rebuild_lookup_indexes(&tx)?;
    tx.execute(
        "INSERT INTO control_plane_store_meta (key, value)
         VALUES (?1, ?2)
         ON CONFLICT(key) DO UPDATE SET value = excluded.value",
        params![LOOKUP_INDEX_VERSION_KEY, LOOKUP_INDEX_VERSION],
    )?;
    tx.commit()?;
    Ok(())
}

fn rebuild_lookup_indexes(conn: &Connection) -> Result<(), ControlPlaneStoreError> {
    clear_lookup_indexes(conn)?;
    let mut stmt = conn.prepare(
        "SELECT kind, entity_id, workspace_id, state_json
         FROM control_plane_entities
         WHERE kind IN ('workspace', 'message_session_binding', 'subagent_link')
         ORDER BY kind, entity_id",
    )?;
    let mut rows = stmt.query([])?;
    let mut entities = Vec::new();
    while let Some(row) = rows.next()? {
        entities.push(SerializedEntity {
            kind: row.get(0)?,
            entity_id: row.get(1)?,
            workspace_id: row.get(2)?,
            state_json: row.get(3)?,
        });
    }
    drop(rows);
    drop(stmt);
    for entity in &entities {
        sync_lookup_index(conn, entity)?;
    }
    Ok(())
}

fn upsert_serialized_entity(
    conn: &Connection,
    entity: &SerializedEntity,
) -> Result<(), ControlPlaneStoreError> {
    conn.execute(
        "INSERT INTO control_plane_entities
            (kind, entity_id, workspace_id, state_json)
         VALUES (?1, ?2, ?3, ?4)
         ON CONFLICT(kind, entity_id) DO UPDATE SET
            workspace_id = excluded.workspace_id,
            state_json = excluded.state_json",
        params![
            entity.kind,
            entity.entity_id,
            entity.workspace_id,
            entity.state_json
        ],
    )?;
    sync_lookup_index(conn, entity)
}

fn upsert_serialized_entity_if_changed(
    conn: &Connection,
    entity: &SerializedEntity,
) -> Result<(), ControlPlaneStoreError> {
    conn.execute(
        "INSERT INTO control_plane_entities
            (kind, entity_id, workspace_id, state_json)
         VALUES (?1, ?2, ?3, ?4)
         ON CONFLICT(kind, entity_id) DO UPDATE SET
            workspace_id = excluded.workspace_id,
            state_json = excluded.state_json
         WHERE control_plane_entities.workspace_id IS NOT excluded.workspace_id
            OR control_plane_entities.state_json IS NOT excluded.state_json",
        params![
            entity.kind,
            entity.entity_id,
            entity.workspace_id,
            entity.state_json
        ],
    )?;
    sync_lookup_index(conn, entity)
}

fn sync_lookup_index(
    conn: &Connection,
    entity: &SerializedEntity,
) -> Result<(), ControlPlaneStoreError> {
    delete_lookup_index(conn, entity.kind.as_str(), entity.entity_id.as_str())?;
    match entity.kind.as_str() {
        "workspace" => {
            let workspace = serde_json::from_str::<WorkspaceBinding>(&entity.state_json)?;
            if let Some(provider_session_id) = workspace.active_provider_session_id {
                conn.execute(
                    "INSERT OR REPLACE INTO control_plane_workspace_provider_session_index
                        (workspace_id, provider_session_id)
                     VALUES (?1, ?2)",
                    params![
                        workspace.workspace_id.as_str(),
                        provider_session_id.as_str()
                    ],
                )?;
            }
        }
        "message_session_binding" => {
            let binding = serde_json::from_str::<MessageSessionBinding>(&entity.state_json)?;
            conn.execute(
                "INSERT OR REPLACE INTO control_plane_message_provider_session_index
                    (binding_id, provider_session_id)
                 VALUES (?1, ?2)",
                params![
                    binding.binding_id.as_str(),
                    binding.provider_session_id.as_str()
                ],
            )?;
        }
        "subagent_link" => {
            let link = serde_json::from_str::<SubAgentLinkRecord>(&entity.state_json)?;
            conn.execute(
                "INSERT OR REPLACE INTO control_plane_subagent_parent_turn_index
                    (link_id, turn_id)
                 VALUES (?1, ?2)",
                params![link.link_id.as_str(), link.parent_turn_id.as_str()],
            )?;
        }
        _ => {}
    }
    Ok(())
}

fn delete_lookup_index(
    conn: &Connection,
    kind: &str,
    entity_id: &str,
) -> Result<(), ControlPlaneStoreError> {
    match kind {
        "workspace" => {
            conn.execute(
                "DELETE FROM control_plane_workspace_provider_session_index
                 WHERE workspace_id = ?1",
                params![entity_id],
            )?;
        }
        "message_session_binding" => {
            conn.execute(
                "DELETE FROM control_plane_message_provider_session_index
                 WHERE binding_id = ?1",
                params![entity_id],
            )?;
        }
        "subagent_link" => {
            conn.execute(
                "DELETE FROM control_plane_subagent_parent_turn_index
                 WHERE link_id = ?1",
                params![entity_id],
            )?;
        }
        _ => {}
    }
    Ok(())
}

fn delete_lookup_index_by_kind(
    conn: &Connection,
    kind: &str,
) -> Result<(), ControlPlaneStoreError> {
    match kind {
        "workspace" => {
            conn.execute(
                "DELETE FROM control_plane_workspace_provider_session_index",
                [],
            )?;
        }
        "message_session_binding" => {
            conn.execute(
                "DELETE FROM control_plane_message_provider_session_index",
                [],
            )?;
        }
        "subagent_link" => {
            conn.execute("DELETE FROM control_plane_subagent_parent_turn_index", [])?;
        }
        _ => {}
    }
    Ok(())
}

fn clear_lookup_indexes(conn: &Connection) -> Result<(), ControlPlaneStoreError> {
    conn.execute(
        "DELETE FROM control_plane_workspace_provider_session_index",
        [],
    )?;
    conn.execute(
        "DELETE FROM control_plane_message_provider_session_index",
        [],
    )?;
    conn.execute("DELETE FROM control_plane_subagent_parent_turn_index", [])?;
    Ok(())
}

fn state_json_bytes(state_json: ValueRef<'_>) -> Result<&[u8], rusqlite::Error> {
    match state_json {
        ValueRef::Text(bytes) => Ok(bytes),
        other => Err(rusqlite::Error::InvalidColumnType(
            3,
            "state_json".into(),
            other.data_type(),
        )),
    }
}

fn serialize_entities(
    entities: Vec<ControlPlanePersistenceEntity>,
) -> Result<Vec<SerializedEntity>, ControlPlaneStoreError> {
    entities
        .into_iter()
        .map(|entity| {
            Ok(SerializedEntity {
                kind: entity.kind,
                entity_id: entity.entity_id,
                workspace_id: entity.workspace_id,
                state_json: serde_json::to_string(&entity.state)?,
            })
        })
        .collect()
}
