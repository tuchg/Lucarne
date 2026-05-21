use async_trait::async_trait;
use base64::Engine;
use smol_str::SmolStr;
use std::collections::{BTreeMap, VecDeque};
use std::sync::{
    atomic::{AtomicBool, Ordering},
    Arc, Mutex as StdMutex, RwLock as StdRwLock,
};
use tokio::{
    sync::{mpsc, oneshot, watch, Mutex as AsyncMutex},
    task::JoinHandle,
    time::{timeout, Duration},
};
use tracing::{debug, info, trace, warn};

use crate::{
    agent_runtime::project::Projector,
    dialect::{ImageRef, Input},
    event::{Decision, PermissionAnswer, PermissionResponse},
    runtime, ProviderId,
};

use super::{
    AgentActivityStream, AgentCapabilities, AgentCommandCatalog, AgentCommandCompletion,
    AgentCommandInvocation, AgentCommandResult, AgentCommandResultData, AgentCommandSource,
    AgentError, AgentErrorKind, AgentEventStream, AgentForkResult, AgentForkSelection,
    AgentForkTargetCatalog, AgentInput, AgentModelCatalog, AgentModelSelection,
    AgentPermissionCatalog, AgentPermissionSelection, AgentSkillCatalog, AgentStatus,
    ApprovalDecision, InstanceId, InterventionResponse, RuntimeBusFilter, SessionId,
};

const CLOSE_DRAIN_TIMEOUT: Duration = Duration::from_millis(250);
const COMMAND_RESULT_TIMEOUT: Duration = Duration::from_secs(120);
const COMMAND_READY_TIMEOUT: Duration = Duration::from_secs(30);
const KNOWN_SESSION_ID_GRACE: Duration = Duration::from_secs(5);
const FRESH_SESSION_ID_GRACE: Duration = Duration::from_secs(2);
const DEFAULT_IDLE_TIMEOUT: Duration = Duration::from_secs(2 * 60 * 60);
const IDLE_CLOSE_REASON: &str = "idle timeout";
const DIRECT_ATTACH_CAPABILITIES: AgentCapabilities = AgentCapabilities {
    reasoning_stream: false,
    tool_stream: false,
    usage_reporting: false,
    structured_intervention: true,
    command_catalog: false,
};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct AgentSessionOptions {
    pub idle_timeout: Option<Duration>,
}

impl AgentSessionOptions {
    pub fn with_idle_timeout(idle_timeout: Option<Duration>) -> Self {
        Self { idle_timeout }
    }

    pub(crate) fn from_idle_timeout_ms(idle_timeout_ms: Option<u64>) -> Self {
        match idle_timeout_ms {
            Some(0) => Self { idle_timeout: None },
            Some(ms) => Self {
                idle_timeout: Some(Duration::from_millis(ms)),
            },
            None => Self::default(),
        }
    }
}

impl Default for AgentSessionOptions {
    fn default() -> Self {
        Self {
            idle_timeout: Some(DEFAULT_IDLE_TIMEOUT),
        }
    }
}

struct ClosingState {
    active: AtomicBool,
    tx: watch::Sender<bool>,
}

impl ClosingState {
    fn new() -> Self {
        let (tx, _rx) = watch::channel(false);
        Self {
            active: AtomicBool::new(false),
            tx,
        }
    }

    fn start(&self) {
        self.active.store(true, Ordering::Relaxed);
        let _ = self.tx.send(true);
    }

    fn is_active(&self) -> bool {
        self.active.load(Ordering::Relaxed)
    }

    fn subscribe(&self) -> watch::Receiver<bool> {
        self.tx.subscribe()
    }
}

enum DrainControl {
    BeginTypedCommand {
        command_name: SmolStr,
        registered_tx: oneshot::Sender<Result<(), AgentError>>,
        result_tx: oneshot::Sender<Result<AgentCommandResultData, AgentError>>,
    },
    CancelTypedCommand {
        command_name: SmolStr,
        error: AgentError,
    },
    UpdateFilter {
        filter: RuntimeBusFilter,
        applied_tx: oneshot::Sender<()>,
    },
    ForceReady {
        session_id: SessionId,
    },
}

struct TypedCommandWaiter {
    command_name: SmolStr,
    result: Option<AgentCommandResultData>,
    tx: oneshot::Sender<Result<AgentCommandResultData, AgentError>>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum IdleSignal {
    Busy,
    Idle,
    Activity,
    Stop,
}

#[async_trait]
pub trait AgentSession: Send + Sync {
    fn id(&self) -> &SessionId;
    fn instance_id(&self) -> &InstanceId;
    fn provider_id(&self) -> ProviderId;
    fn process_id(&self) -> Option<i32> {
        None
    }

    async fn submit(&self, input: AgentInput) -> Result<(), AgentError>;
    async fn list_commands(&self) -> Result<AgentCommandCatalog, AgentError> {
        Ok(AgentCommandCatalog::default())
    }
    async fn invoke_command(
        &self,
        command: AgentCommandInvocation,
    ) -> Result<AgentCommandResult, AgentError> {
        let action = format!("command {:?}", command.name);
        Err(unsupported_session_command(self.provider_id(), &action))
    }
    async fn list_models(&self) -> Result<AgentModelCatalog, AgentError> {
        Err(unsupported_session_command(
            self.provider_id(),
            "list_models",
        ))
    }
    async fn set_model(&self, _selection: AgentModelSelection) -> Result<AgentStatus, AgentError> {
        Err(unsupported_session_command(self.provider_id(), "set_model"))
    }
    async fn list_permissions(&self) -> Result<AgentPermissionCatalog, AgentError> {
        Err(unsupported_session_command(
            self.provider_id(),
            "list_permissions",
        ))
    }
    async fn set_permissions(
        &self,
        _selection: AgentPermissionSelection,
    ) -> Result<AgentStatus, AgentError> {
        Err(unsupported_session_command(
            self.provider_id(),
            "set_permissions",
        ))
    }
    async fn list_skills(&self) -> Result<AgentSkillCatalog, AgentError> {
        Err(unsupported_session_command(
            self.provider_id(),
            "list_skills",
        ))
    }
    async fn new(&self) -> Result<(), AgentError> {
        Err(unsupported_session_command(self.provider_id(), "new"))
    }
    async fn quit(&self) -> Result<(), AgentError> {
        Err(unsupported_session_command(self.provider_id(), "quit"))
    }
    async fn list_fork_targets(&self) -> Result<AgentForkTargetCatalog, AgentError> {
        Err(unsupported_session_command(
            self.provider_id(),
            "list_fork_targets",
        ))
    }
    async fn fork(&self, _selection: AgentForkSelection) -> Result<AgentForkResult, AgentError> {
        Err(unsupported_session_command(self.provider_id(), "fork"))
    }
    async fn status(&self) -> Result<AgentStatus, AgentError> {
        Err(unsupported_session_command(self.provider_id(), "status"))
    }
    async fn interrupt(&self) -> Result<(), AgentError>;
    async fn resolve(&self, req_id: &str, response: InterventionResponse)
        -> Result<(), AgentError>;
    async fn take_events(&self) -> Result<AgentEventStream, AgentError>;
    async fn take_activity_events(&self) -> Result<Option<AgentActivityStream>, AgentError> {
        Ok(None)
    }
    async fn close(&self) -> Result<(), AgentError>;

    #[doc(hidden)]
    async fn observed_close_reason(&self) -> Option<SmolStr> {
        None
    }

    #[doc(hidden)]
    async fn provider_session_id(&self) -> Option<SessionId> {
        Some(self.id().clone())
    }

    #[doc(hidden)]
    async fn update_runtime_filter(&self, _filter: RuntimeBusFilter) -> Result<bool, AgentError> {
        Ok(false)
    }
}

fn unsupported_session_command(provider_id: ProviderId, action: &str) -> AgentError {
    AgentError {
        kind: AgentErrorKind::Unsupported,
        message: format!("{provider_id} session does not support {action}").into(),
    }
}

fn command_source_for_catalog(
    catalog: &AgentCommandCatalog,
    command: &AgentCommandInvocation,
) -> AgentCommandSource {
    let name = command.name.as_str();
    catalog
        .commands
        .iter()
        .find(|command| {
            command.name.as_str() == name || command.aliases.iter().any(|alias| alias == name)
        })
        .map(|command| command.source)
        .unwrap_or(command.source)
}

fn command_completion_for_catalog(
    catalog: &AgentCommandCatalog,
    name: &str,
) -> AgentCommandCompletion {
    catalog
        .commands
        .iter()
        .find(|command| {
            command.name.as_str() == name || command.aliases.iter().any(|alias| alias == name)
        })
        .map(|command| command.completion)
        .unwrap_or_default()
}

fn agent_command_result_data_from_event(
    data: crate::event::CommandResultData,
) -> AgentCommandResultData {
    match data {
        crate::event::CommandResultData::Models(catalog) => AgentCommandResultData::Models(catalog),
        crate::event::CommandResultData::ModelChanged(selection) => {
            AgentCommandResultData::Status(AgentStatus {
                model: Some(selection.model),
                reasoning: selection.reasoning,
                ..Default::default()
            })
        }
        crate::event::CommandResultData::Permissions(catalog) => {
            AgentCommandResultData::Permissions(catalog)
        }
        crate::event::CommandResultData::PermissionsChanged(selection) => {
            AgentCommandResultData::Status(AgentStatus {
                permissions: Some(selection.mode.to_string().into()),
                ..Default::default()
            })
        }
        crate::event::CommandResultData::Status(status) => AgentCommandResultData::Status(status),
        crate::event::CommandResultData::Skills(catalog) => AgentCommandResultData::Skills(catalog),
        crate::event::CommandResultData::Forked(result) => AgentCommandResultData::Fork(result),
        crate::event::CommandResultData::ForkTargets(catalog) => {
            AgentCommandResultData::ForkTargets(catalog)
        }
        crate::event::CommandResultData::Commands(catalog) => {
            AgentCommandResultData::Commands(catalog)
        }
        crate::event::CommandResultData::Text { text } => AgentCommandResultData::Text { text },
    }
}

fn unexpected_command_result(
    command: &str,
    expected: &str,
    actual: &AgentCommandResultData,
) -> AgentError {
    invalid_state(format!(
        "command {command:?} returned {}, expected {expected}",
        agent_command_result_data_name(actual)
    ))
}

fn agent_command_result_data_name(data: &AgentCommandResultData) -> &'static str {
    match data {
        AgentCommandResultData::Models(_) => "models",
        AgentCommandResultData::Permissions(_) => "permissions",
        AgentCommandResultData::Skills(_) => "skills",
        AgentCommandResultData::Commands(_) => "commands",
        AgentCommandResultData::ForkTargets(_) => "fork_targets",
        AgentCommandResultData::Fork(_) => "fork",
        AgentCommandResultData::Status(_) => "status",
        AgentCommandResultData::Text { .. } => "text",
        AgentCommandResultData::Json(_) => "json",
        AgentCommandResultData::Empty => "empty",
    }
}

pub struct AgentSessionFacade {
    provider_id: ProviderId,
    instance_id: InstanceId,
    session_id: SessionId,
    process_id: Option<i32>,
    provider_session_id: Arc<StdRwLock<Option<SessionId>>>,
    provider_ready_rx: AsyncMutex<watch::Receiver<bool>>,
    session: Arc<AsyncMutex<runtime::Session>>,
    events_rx: StdMutex<Option<AgentEventStream>>,
    activity_rx: StdMutex<Option<AgentActivityStream>>,
    close_reason: Arc<StdRwLock<Option<SmolStr>>>,
    closing: Arc<ClosingState>,
    question_keys: Arc<StdRwLock<BTreeMap<String, Vec<Vec<String>>>>>,
    control_tx: mpsc::UnboundedSender<DrainControl>,
    idle_tx: Option<mpsc::UnboundedSender<IdleSignal>>,
    idle_task: StdMutex<Option<JoinHandle<()>>>,
    drain_task: StdMutex<Option<JoinHandle<()>>>,
}

impl AgentSessionFacade {
    pub async fn attach(
        provider_id: ProviderId,
        instance_id: InstanceId,
        session: runtime::Session,
        initial_input: Option<AgentInput>,
    ) -> Result<Self, AgentError> {
        Self::attach_with_options_and_capabilities(
            provider_id,
            instance_id,
            session,
            initial_input,
            None,
            AgentSessionOptions::default(),
            DIRECT_ATTACH_CAPABILITIES,
        )
        .await
    }

    pub async fn attach_with_known_session_id(
        provider_id: ProviderId,
        instance_id: InstanceId,
        session: runtime::Session,
        initial_input: Option<AgentInput>,
        known_session_id: Option<SessionId>,
    ) -> Result<Self, AgentError> {
        Self::attach_with_options_and_capabilities(
            provider_id,
            instance_id,
            session,
            initial_input,
            known_session_id,
            AgentSessionOptions::default(),
            DIRECT_ATTACH_CAPABILITIES,
        )
        .await
    }

    pub async fn attach_with_options(
        provider_id: ProviderId,
        instance_id: InstanceId,
        session: runtime::Session,
        initial_input: Option<AgentInput>,
        known_session_id: Option<SessionId>,
        options: AgentSessionOptions,
    ) -> Result<Self, AgentError> {
        Self::attach_with_options_and_capabilities(
            provider_id,
            instance_id,
            session,
            initial_input,
            known_session_id,
            options,
            DIRECT_ATTACH_CAPABILITIES,
        )
        .await
    }

    pub async fn attach_with_options_and_capabilities(
        provider_id: ProviderId,
        instance_id: InstanceId,
        session: runtime::Session,
        initial_input: Option<AgentInput>,
        known_session_id: Option<SessionId>,
        options: AgentSessionOptions,
        capabilities: AgentCapabilities,
    ) -> Result<Self, AgentError> {
        info!(
            target: "lucarne::agent_runtime::session",
            provider_id = %provider_id,
            instance_id = %instance_id.0,
            runtime_session_id = session.id(),
            epoch = session.epoch(),
            has_initial_input = initial_input.is_some(),
            has_known_session_id = known_session_id.is_some(),
            idle_timeout_ms = options.idle_timeout.map(|duration| duration.as_millis() as u64),
            "attaching agent session facade"
        );
        let starts_busy = initial_input.is_some();
        let process_id = session.process_id();
        let runtime_session_id = SessionId(session.id().to_string().into());
        let internal_events = session
            .events()
            .await
            .ok_or_else(|| invalid_state("internal session event stream already taken"))?;
        let session = Arc::new(AsyncMutex::new(session));
        let (public_tx, public_rx) = mpsc::channel(1024);
        let (activity_tx, activity_rx) = mpsc::channel(16);
        let (ready_tx, ready_rx) = oneshot::channel();
        let (provider_ready_tx, provider_ready_rx) = watch::channel(false);
        let (control_tx, control_rx) = mpsc::unbounded_channel();
        let (idle_tx, idle_rx) = mpsc::unbounded_channel();
        let close_reason = Arc::new(StdRwLock::new(None));
        let closing = Arc::new(ClosingState::new());
        let question_keys = Arc::new(StdRwLock::new(BTreeMap::new()));
        let provider_session_id = Arc::new(StdRwLock::new(None));
        let known_session_id_hint = known_session_id.clone();
        let drain_task = tokio::spawn(drain_events(
            provider_id,
            capabilities,
            internal_events,
            public_tx,
            ready_tx,
            provider_ready_tx,
            Arc::clone(&provider_session_id),
            Arc::clone(&close_reason),
            Arc::clone(&closing),
            Arc::clone(&question_keys),
            idle_tx.clone(),
            activity_tx,
            control_rx,
        ));

        if let Some(input) = initial_input {
            debug!(
                target: "lucarne::agent_runtime::session",
                provider_id = %provider_id,
                instance_id = %instance_id.0,
                text_bytes = input.text.len(),
                images = input.images.len(),
                "sending initial input during attach"
            );
            let input = decode_agent_input(input)?;
            if let Err(err) = session.lock().await.send(input).await {
                warn!(
                    target: "lucarne::agent_runtime::session",
                    provider_id = %provider_id,
                    instance_id = %instance_id.0,
                    error = %err,
                    "initial input send failed during attach"
                );
                session.lock().await.close().await;
                let mut drain_task = drain_task;
                if timeout(CLOSE_DRAIN_TIMEOUT, &mut drain_task).await.is_err() {
                    drain_task.abort();
                    let _ = drain_task.await;
                }
                return Err(map_lucarne_error(err));
            }
        }

        let session_id = match known_session_id {
            Some(session_id) => match timeout(KNOWN_SESSION_ID_GRACE, ready_rx).await {
                Ok(Ok(result)) => result?,
                Ok(Err(_)) => {
                    warn!(
                        target: "lucarne::agent_runtime::session",
                        provider_id = %provider_id,
                        instance_id = %instance_id.0,
                        "ready task ended before surfacing provider-native session id"
                    );
                    return Err(internal(
                        "session readiness task ended before surfacing a provider-native session id",
                    ));
                }
                Err(_) => {
                    debug!(
                        target: "lucarne::agent_runtime::session",
                        provider_id = %provider_id,
                        instance_id = %instance_id.0,
                        known_session_id = %session_id.0,
                        "timed out waiting for provider-native session id, using known session id"
                    );
                    let _ = control_tx.send(DrainControl::ForceReady {
                        session_id: session_id.clone(),
                    });
                    session_id
                }
            },
            None if !starts_busy => match timeout(FRESH_SESSION_ID_GRACE, ready_rx).await {
                Ok(Ok(result)) => result?,
                Ok(Err(_)) => {
                    warn!(
                        target: "lucarne::agent_runtime::session",
                        provider_id = %provider_id,
                        instance_id = %instance_id.0,
                        "ready task ended before surfacing provider-native session id"
                    );
                    return Err(internal(
                        "session readiness task ended before surfacing a provider-native session id",
                    ));
                }
                Err(_) => {
                    debug!(
                        target: "lucarne::agent_runtime::session",
                        provider_id = %provider_id,
                        instance_id = %instance_id.0,
                        session_id = %runtime_session_id.0,
                        "timed out waiting for provider-native session id, using runtime session id"
                    );
                    let _ = control_tx.send(DrainControl::ForceReady {
                        session_id: runtime_session_id.clone(),
                    });
                    runtime_session_id.clone()
                }
            },
            None => match ready_rx.await {
                Ok(result) => result?,
                Err(_) => {
                    warn!(
                        target: "lucarne::agent_runtime::session",
                        provider_id = %provider_id,
                        instance_id = %instance_id.0,
                        "ready task ended before surfacing provider-native session id"
                    );
                    return Err(internal(
                        "session readiness task ended before surfacing a provider-native session id",
                    ));
                }
            },
        };

        if known_session_id_hint.is_some() || session_id != runtime_session_id {
            *provider_session_id
                .write()
                .expect("provider session id lock") = Some(session_id.clone());
        }

        let idle_task = options.idle_timeout.map(|idle_timeout| {
            tokio::spawn(run_idle_reaper(
                provider_id,
                instance_id.clone(),
                session_id.clone(),
                Arc::clone(&session),
                Arc::clone(&close_reason),
                Arc::clone(&closing),
                idle_rx,
                idle_timeout,
            ))
        });
        if idle_task.is_some() {
            let _ = idle_tx.send(if starts_busy {
                IdleSignal::Busy
            } else {
                IdleSignal::Idle
            });
        }

        let facade = Self {
            provider_id,
            instance_id,
            session_id,
            process_id,
            provider_session_id,
            provider_ready_rx: AsyncMutex::new(provider_ready_rx),
            session,
            events_rx: StdMutex::new(Some(public_rx)),
            activity_rx: StdMutex::new(Some(activity_rx)),
            close_reason,
            closing,
            question_keys,
            control_tx,
            idle_tx: idle_task.as_ref().map(|_| idle_tx),
            idle_task: StdMutex::new(idle_task),
            drain_task: StdMutex::new(Some(drain_task)),
        };

        info!(
            target: "lucarne::agent_runtime::session",
            provider_id = %provider_id,
            instance_id = %facade.instance_id.0,
            session_id = %facade.session_id.0,
            "agent session facade attached"
        );
        Ok(facade)
    }

    pub fn instance_id(&self) -> &InstanceId {
        &self.instance_id
    }

    fn signal_idle(&self, signal: IdleSignal) {
        if let Some(tx) = self.idle_tx.as_ref() {
            let _ = tx.send(signal);
        }
    }

    async fn wait_for_provider_ready_for_command(&self) -> Result<(), AgentError> {
        let mut ready_rx = self.provider_ready_rx.lock().await;
        if *ready_rx.borrow() {
            return Ok(());
        }
        match timeout(COMMAND_READY_TIMEOUT, ready_rx.changed()).await {
            Ok(Ok(())) if *ready_rx.borrow() => Ok(()),
            Ok(Ok(())) => Err(invalid_state("provider session readiness regressed")),
            Ok(Err(_)) => Err(invalid_state(
                "session readiness channel closed before provider became ready",
            )),
            Err(_) => Err(internal(format!(
                "timed out waiting for provider-native session readiness before command ({:?})",
                COMMAND_READY_TIMEOUT
            ))),
        }
    }

    async fn invoke_typed_command(
        &self,
        command: AgentCommandInvocation,
    ) -> Result<AgentCommandResult, AgentError> {
        self.wait_for_provider_ready_for_command().await?;
        let command_name = command.name.clone();
        let source = AgentCommandSource::AdapterMapped;
        let command = AgentCommandInvocation { source, ..command };
        let (registered_tx, registered_rx) = oneshot::channel();
        let (result_tx, result_rx) = oneshot::channel();
        self.control_tx
            .send(DrainControl::BeginTypedCommand {
                command_name: command_name.clone(),
                registered_tx,
                result_tx,
            })
            .map_err(|_| invalid_state("session event drain task is not running"))?;
        registered_rx.await.map_err(|_| {
            invalid_state("session event drain task ended before registering typed command")
        })??;
        self.signal_idle(IdleSignal::Busy);
        let invoke_result = self.session.lock().await.invoke_command(command).await;
        if let Err(err) = invoke_result {
            self.signal_idle(IdleSignal::Idle);
            let err = map_lucarne_error(err);
            let _ = self.control_tx.send(DrainControl::CancelTypedCommand {
                command_name,
                error: err.clone(),
            });
            return Err(err);
        }
        let data = timeout(COMMAND_RESULT_TIMEOUT, result_rx)
            .await
            .map_err(|_| {
                self.signal_idle(IdleSignal::Idle);
                let err = invalid_state(format!(
                    "timed out waiting for command {:?} to complete",
                    command_name.as_str()
                ));
                let _ = self.control_tx.send(DrainControl::CancelTypedCommand {
                    command_name: command_name.clone(),
                    error: err.clone(),
                });
                err
            })?
            .map_err(|_| {
                self.signal_idle(IdleSignal::Idle);
                invalid_state(format!(
                    "session event drain task ended before command {:?} completed",
                    command_name.as_str()
                ))
            })??;
        self.signal_idle(IdleSignal::Idle);
        Ok(AgentCommandResult {
            name: command_name,
            source,
            data,
        })
    }
}

#[async_trait]
impl AgentSession for AgentSessionFacade {
    fn id(&self) -> &SessionId {
        &self.session_id
    }

    fn instance_id(&self) -> &InstanceId {
        &self.instance_id
    }

    fn provider_id(&self) -> ProviderId {
        self.provider_id
    }

    fn process_id(&self) -> Option<i32> {
        self.process_id
    }

    async fn submit(&self, input: AgentInput) -> Result<(), AgentError> {
        debug!(
            target: "lucarne::agent_runtime::session",
            provider_id = %self.provider_id,
            instance_id = %self.instance_id.0,
            session_id = %self.session_id.0,
            text_bytes = input.text.len(),
            images = input.images.len(),
            "submitting agent input"
        );
        let input = decode_agent_input(input)?;
        self.signal_idle(IdleSignal::Busy);
        let result = self.session.lock().await.send(input).await;
        if result.is_err() {
            self.signal_idle(IdleSignal::Idle);
        }
        result.map_err(map_lucarne_error)
    }

    async fn list_commands(&self) -> Result<AgentCommandCatalog, AgentError> {
        debug!(
            target: "lucarne::agent_runtime::session",
            provider_id = %self.provider_id,
            instance_id = %self.instance_id.0,
            session_id = %self.session_id.0,
            "listing agent commands"
        );
        let catalog = self.session.lock().await.command_catalog().await;
        if catalog.complete {
            return Ok(catalog);
        }
        self.wait_for_provider_ready_for_command().await?;
        let catalog = self.session.lock().await.command_catalog().await;
        if catalog.complete {
            return Ok(catalog);
        }
        match self
            .invoke_typed_command(AgentCommandInvocation {
                name: "list_commands".into(),
                args: None,
                values: serde_json::Value::Null,
                source: AgentCommandSource::AdapterMapped,
            })
            .await?
            .data
        {
            AgentCommandResultData::Commands(catalog) => Ok(catalog),
            data => Err(unexpected_command_result(
                "list_commands",
                "commands",
                &data,
            )),
        }
    }

    async fn invoke_command(
        &self,
        command: AgentCommandInvocation,
    ) -> Result<AgentCommandResult, AgentError> {
        info!(
            target: "lucarne::agent_runtime::session",
            provider_id = %self.provider_id,
            instance_id = %self.instance_id.0,
            session_id = %self.session_id.0,
            command = %command.name,
            "invoking agent command"
        );
        self.wait_for_provider_ready_for_command().await?;
        let result_name = command.name.clone();
        let catalog = self.session.lock().await.command_catalog().await;
        let result_source = command_source_for_catalog(&catalog, &command);
        let completion_policy = command_completion_for_catalog(&catalog, command.name.as_str());
        let command = AgentCommandInvocation {
            source: result_source,
            ..command
        };
        self.signal_idle(IdleSignal::Busy);
        let result = self.session.lock().await.invoke_command(command).await;
        if result.is_err() {
            self.signal_idle(IdleSignal::Idle);
        }
        result.map_err(map_lucarne_error)?;
        if completion_policy == AgentCommandCompletion::NoOutputAck {
            self.signal_idle(IdleSignal::Idle);
        }
        Ok(AgentCommandResult {
            name: result_name,
            source: result_source,
            data: AgentCommandResultData::Empty,
        })
    }

    async fn list_models(&self) -> Result<AgentModelCatalog, AgentError> {
        match self
            .invoke_typed_command(AgentCommandInvocation {
                name: "model".into(),
                args: None,
                values: serde_json::Value::Null,
                source: AgentCommandSource::AdapterMapped,
            })
            .await?
            .data
        {
            AgentCommandResultData::Models(catalog) => Ok(catalog),
            data => Err(unexpected_command_result("model", "models", &data)),
        }
    }

    async fn set_model(&self, selection: AgentModelSelection) -> Result<AgentStatus, AgentError> {
        let args = match selection.reasoning.as_deref() {
            Some(reasoning) => format!("{} {reasoning}", selection.model).into(),
            None => selection.model.to_string().into(),
        };
        match self
            .invoke_typed_command(AgentCommandInvocation {
                name: "model".into(),
                args: Some(args),
                values: serde_json::json!({
                    "model": selection.model.as_str(),
                    "reasoning": selection.reasoning.as_deref(),
                }),
                source: AgentCommandSource::AdapterMapped,
            })
            .await?
            .data
        {
            AgentCommandResultData::Status(status) => Ok(status),
            AgentCommandResultData::Empty => Ok(AgentStatus {
                model: Some(selection.model),
                reasoning: selection.reasoning,
                ..Default::default()
            }),
            data => Err(unexpected_command_result("model", "status", &data)),
        }
    }

    async fn list_permissions(&self) -> Result<AgentPermissionCatalog, AgentError> {
        match self
            .invoke_typed_command(AgentCommandInvocation {
                name: "permissions".into(),
                args: None,
                values: serde_json::Value::Null,
                source: AgentCommandSource::AdapterMapped,
            })
            .await?
            .data
        {
            AgentCommandResultData::Permissions(catalog) => Ok(catalog),
            data => Err(unexpected_command_result(
                "permissions",
                "permission catalog",
                &data,
            )),
        }
    }

    async fn set_permissions(
        &self,
        selection: AgentPermissionSelection,
    ) -> Result<AgentStatus, AgentError> {
        match self
            .invoke_typed_command(AgentCommandInvocation {
                name: "permissions".into(),
                args: Some(selection.mode.to_string().into()),
                values: serde_json::json!({ "mode": selection.mode.as_str() }),
                source: AgentCommandSource::AdapterMapped,
            })
            .await?
            .data
        {
            AgentCommandResultData::Status(status) => Ok(status),
            AgentCommandResultData::Empty => Ok(AgentStatus {
                permissions: Some(selection.mode.to_string().into()),
                ..Default::default()
            }),
            data => Err(unexpected_command_result("permissions", "status", &data)),
        }
    }

    async fn list_skills(&self) -> Result<AgentSkillCatalog, AgentError> {
        match self
            .invoke_typed_command(AgentCommandInvocation {
                name: "skills".into(),
                args: None,
                values: serde_json::Value::Null,
                source: AgentCommandSource::AdapterMapped,
            })
            .await?
            .data
        {
            AgentCommandResultData::Skills(catalog) => Ok(catalog),
            data => Err(unexpected_command_result("skills", "skills", &data)),
        }
    }

    async fn new(&self) -> Result<(), AgentError> {
        self.invoke_typed_command(AgentCommandInvocation {
            name: "new".into(),
            args: None,
            values: serde_json::Value::Null,
            source: AgentCommandSource::AdapterMapped,
        })
        .await
        .map(|_| ())
    }

    async fn quit(&self) -> Result<(), AgentError> {
        self.invoke_typed_command(AgentCommandInvocation {
            name: "quit".into(),
            args: None,
            values: serde_json::Value::Null,
            source: AgentCommandSource::AdapterMapped,
        })
        .await
        .map(|_| ())
    }

    async fn list_fork_targets(&self) -> Result<AgentForkTargetCatalog, AgentError> {
        match self
            .invoke_typed_command(AgentCommandInvocation {
                name: "fork".into(),
                args: None,
                values: serde_json::Value::Null,
                source: AgentCommandSource::AdapterMapped,
            })
            .await?
            .data
        {
            AgentCommandResultData::ForkTargets(catalog) => Ok(catalog),
            data => Err(unexpected_command_result("fork", "fork targets", &data)),
        }
    }

    async fn fork(&self, selection: AgentForkSelection) -> Result<AgentForkResult, AgentError> {
        match self
            .invoke_typed_command(AgentCommandInvocation {
                name: "fork".into(),
                args: Some(selection.target_id.to_string().into()),
                values: serde_json::json!({ "target_id": selection.target_id.as_str() }),
                source: AgentCommandSource::AdapterMapped,
            })
            .await?
            .data
        {
            AgentCommandResultData::Fork(result) => Ok(result),
            data => Err(unexpected_command_result("fork", "fork result", &data)),
        }
    }

    async fn status(&self) -> Result<AgentStatus, AgentError> {
        match self
            .invoke_typed_command(AgentCommandInvocation {
                name: "status".into(),
                args: None,
                values: serde_json::Value::Null,
                source: AgentCommandSource::AdapterMapped,
            })
            .await?
            .data
        {
            AgentCommandResultData::Status(status) => Ok(status),
            data => Err(unexpected_command_result("status", "status", &data)),
        }
    }

    async fn interrupt(&self) -> Result<(), AgentError> {
        info!(
            target: "lucarne::agent_runtime::session",
            provider_id = %self.provider_id,
            instance_id = %self.instance_id.0,
            session_id = %self.session_id.0,
            "interrupting session"
        );
        self.signal_idle(IdleSignal::Busy);
        self.session
            .lock()
            .await
            .interrupt()
            .await
            .map_err(map_lucarne_error)
    }

    async fn resolve(
        &self,
        req_id: &str,
        response: InterventionResponse,
    ) -> Result<(), AgentError> {
        let response_kind = intervention_response_name(&response);
        info!(
            target: "lucarne::agent_runtime::session",
            provider_id = %self.provider_id,
            instance_id = %self.instance_id.0,
            session_id = %self.session_id.0,
            req_id,
            response_kind,
            "resolving intervention request"
        );
        let question_keys = self
            .question_keys
            .read()
            .expect("question key registry lock")
            .get(req_id)
            .cloned();
        let response = into_permission_response(req_id, response, question_keys.as_deref())?;
        self.signal_idle(IdleSignal::Busy);
        self.session
            .lock()
            .await
            .resolve_with_response(req_id, &response)
            .await
            .map_err(map_lucarne_error)?;
        self.question_keys
            .write()
            .expect("question key registry lock")
            .remove(req_id);
        Ok(())
    }

    async fn take_events(&self) -> Result<AgentEventStream, AgentError> {
        debug!(
            target: "lucarne::agent_runtime::session",
            provider_id = %self.provider_id,
            instance_id = %self.instance_id.0,
            session_id = %self.session_id.0,
            "taking public event stream"
        );
        self.events_rx
            .lock()
            .expect("public event stream lock")
            .take()
            .ok_or_else(|| invalid_state("public event stream already taken"))
    }

    async fn take_activity_events(&self) -> Result<Option<AgentActivityStream>, AgentError> {
        debug!(
            target: "lucarne::agent_runtime::session",
            provider_id = %self.provider_id,
            instance_id = %self.instance_id.0,
            session_id = %self.session_id.0,
            "taking activity event stream"
        );
        Ok(Some(
            self.activity_rx
                .lock()
                .expect("activity event stream lock")
                .take()
                .ok_or_else(|| invalid_state("activity event stream already taken"))?,
        ))
    }

    async fn close(&self) -> Result<(), AgentError> {
        info!(
            target: "lucarne::agent_runtime::session",
            provider_id = %self.provider_id,
            instance_id = %self.instance_id.0,
            session_id = %self.session_id.0,
            "closing session facade"
        );
        self.signal_idle(IdleSignal::Stop);
        self.closing.start();
        self.session.lock().await.close().await;
        let idle_task = self.idle_task.lock().expect("idle task lock").take();
        if let Some(handle) = idle_task {
            let _ = handle.await;
        }
        let drain_task = self.drain_task.lock().expect("drain task lock").take();
        if let Some(handle) = drain_task {
            let mut handle = handle;
            if timeout(CLOSE_DRAIN_TIMEOUT, &mut handle).await.is_err() {
                warn!(
                    target: "lucarne::agent_runtime::session",
                    provider_id = %self.provider_id,
                    instance_id = %self.instance_id.0,
                    session_id = %self.session_id.0,
                    timeout_ms = CLOSE_DRAIN_TIMEOUT.as_millis() as u64,
                    "drain task did not finish before timeout; aborting"
                );
                handle.abort();
                let _ = handle.await;
            }
        }
        Ok(())
    }

    async fn observed_close_reason(&self) -> Option<SmolStr> {
        self.close_reason.read().expect("close reason lock").clone()
    }

    async fn provider_session_id(&self) -> Option<SessionId> {
        self.provider_session_id
            .read()
            .expect("provider session id lock")
            .clone()
    }

    async fn update_runtime_filter(&self, filter: RuntimeBusFilter) -> Result<bool, AgentError> {
        debug!(
            target: "lucarne::agent_runtime::session",
            provider_id = %self.provider_id,
            instance_id = %self.instance_id.0,
            session_id = %self.session_id.0,
            session_lifecycle = filter.session_lifecycle,
            user_messages = filter.user_messages,
            assistant_messages = filter.assistant_messages,
            reasoning = filter.reasoning,
            tool_calls = filter.tool_calls,
            tool_results = filter.tool_results,
            usage = filter.usage,
            intervention_requests = filter.intervention_requests,
            "updating session runtime filter"
        );
        let (applied_tx, applied_rx) = oneshot::channel();
        self.control_tx
            .send(DrainControl::UpdateFilter { filter, applied_tx })
            .map_err(|_| invalid_state("session event drain task is not running"))?;
        applied_rx.await.map_err(|_| {
            invalid_state("session event drain task ended before applying runtime filter")
        })?;
        Ok(true)
    }
}

async fn drain_events(
    provider_id: ProviderId,
    capabilities: AgentCapabilities,
    mut internal_events: mpsc::Receiver<crate::event::Event>,
    public_tx: mpsc::Sender<super::events::Event>,
    ready_tx: oneshot::Sender<Result<SessionId, AgentError>>,
    provider_ready_tx: watch::Sender<bool>,
    provider_session_id: Arc<StdRwLock<Option<SessionId>>>,
    close_reason: Arc<StdRwLock<Option<SmolStr>>>,
    closing: Arc<ClosingState>,
    question_keys: Arc<StdRwLock<BTreeMap<String, Vec<Vec<String>>>>>,
    idle_tx: mpsc::UnboundedSender<IdleSignal>,
    activity_tx: mpsc::Sender<()>,
    mut control_rx: mpsc::UnboundedReceiver<DrainControl>,
) {
    debug!(
        target: "lucarne::agent_runtime::session",
        provider_id = %provider_id,
        "starting internal→public event drain"
    );
    let mut ready_tx = Some(ready_tx);
    let mut ready = false;
    let mut source_closed = false;
    let mut filter = None;
    let mut pending_public: VecDeque<super::events::Event> = VecDeque::new();
    let mut typed_command: Option<TypedCommandWaiter> = None;

    loop {
        if source_closed {
            fail_typed_command(
                &mut typed_command,
                internal("event stream closed while waiting for typed command completion"),
            );
            if !ready {
                if let Some(ready_tx) = ready_tx.take() {
                    let _ = ready_tx.send(Err(internal(
                        "wrapped session exited before surfacing a provider-native session id",
                    )));
                }
                warn!(
                    target: "lucarne::agent_runtime::session",
                    provider_id = %provider_id,
                    "internal session exited before ready"
                );
                return;
            }
            if pending_public.is_empty() {
                debug!(
                    target: "lucarne::agent_runtime::session",
                    provider_id = %provider_id,
                    "event drain finished"
                );
                return;
            }
        }

        if ready {
            if let Some(event) = pending_public.pop_front() {
                if closing.is_active() {
                    pending_public.clear();
                    continue;
                }

                let mut closing_rx = closing.subscribe();
                if closing.is_active() {
                    pending_public.clear();
                    continue;
                }

                tokio::select! {
                    biased;
                    Some(control) = control_rx.recv() => {
                        pending_public.push_front(event);
                        apply_drain_control(
                            provider_id,
                            capabilities,
                            &mut internal_events,
                            &mut ready_tx,
                            &mut ready,
                            &provider_ready_tx,
                            &provider_session_id,
                            &close_reason,
                            closing.as_ref(),
                            &question_keys,
                            &idle_tx,
                            &activity_tx,
                            &mut filter,
                            &mut pending_public,
                            &mut typed_command,
                            &mut source_closed,
                            control,
                        ).await;
                    }
                    result = public_tx.send(event.clone()) => {
                        if result.is_err() {
                            warn!(
                                target: "lucarne::agent_runtime::session",
                                provider_id = %provider_id,
                                event = public_event_name(&event),
                                "public event receiver closed"
                            );
                            return;
                        }
                    }
                    result = closing_rx.changed() => {
                        if result.is_ok() {
                            pending_public.clear();
                            continue;
                        }
                        return;
                    }
                }
                continue;
            }
        }

        tokio::select! {
            biased;
            Some(control) = control_rx.recv() => {
                apply_drain_control(
                    provider_id,
                    capabilities,
                    &mut internal_events,
                    &mut ready_tx,
                    &mut ready,
                    &provider_ready_tx,
                    &provider_session_id,
                    &close_reason,
                    closing.as_ref(),
                    &question_keys,
                    &idle_tx,
                    &activity_tx,
                    &mut filter,
                    &mut pending_public,
                    &mut typed_command,
                    &mut source_closed,
                    control,
                ).await;
            }
            maybe_event = internal_events.recv(), if !source_closed => match maybe_event {
                Some(event) => {
                    process_internal_event(
                        provider_id,
                        capabilities,
                        event,
                        &mut ready_tx,
                        &mut ready,
                        &provider_ready_tx,
                        &provider_session_id,
                        &close_reason,
                        &question_keys,
                        &idle_tx,
                        &activity_tx,
                        filter.as_ref(),
                        &mut pending_public,
                        &mut typed_command,
                        &mut source_closed,
                    ).await;
                }
                None => {
                    source_closed = true;
                    debug!(
                        target: "lucarne::agent_runtime::session",
                        provider_id = %provider_id,
                        "internal event stream closed"
                    );
                }
            }
        }
    }
}

fn fail_typed_command(waiter: &mut Option<TypedCommandWaiter>, error: AgentError) {
    if let Some(waiter) = waiter.take() {
        let _ = waiter.tx.send(Err(error));
    }
}

fn route_typed_command_event(
    waiter: &mut Option<TypedCommandWaiter>,
    event: &super::events::Event,
) -> bool {
    let Some(active) = waiter.as_mut() else {
        return false;
    };
    match event {
        super::events::Event::CommandResult(result) => {
            let payload_command = result.result.command.clone();
            if result.command.as_str() == active.command_name.as_str()
                || payload_command == active.command_name.as_str()
                || (active.command_name.as_str() == "list_commands"
                    && (result.command.as_str() == "commands" || payload_command == "commands"))
            {
                active.result = Some(agent_command_result_data_from_event(
                    result.result.result.clone(),
                ));
            }
            true
        }
        super::events::Event::TurnCompleted(_) => {
            let active = waiter.take().expect("active waiter");
            let _ = active
                .tx
                .send(Ok(active.result.unwrap_or(AgentCommandResultData::Empty)));
            true
        }
        super::events::Event::TurnFailed(failed) => {
            let error = internal(if failed.error.is_empty() {
                format!(
                    "command {:?} failed ({})",
                    active.command_name.as_str(),
                    failed.code
                )
            } else {
                failed.error.to_string()
            });
            let active = waiter.take().expect("active waiter");
            let _ = active.tx.send(Err(error));
            true
        }
        _ => true,
    }
}

async fn run_idle_reaper(
    provider_id: ProviderId,
    instance_id: InstanceId,
    session_id: SessionId,
    session: Arc<AsyncMutex<runtime::Session>>,
    close_reason: Arc<StdRwLock<Option<SmolStr>>>,
    closing: Arc<ClosingState>,
    mut idle_rx: mpsc::UnboundedReceiver<IdleSignal>,
    idle_timeout: Duration,
) {
    let mut armed = false;
    loop {
        if !armed {
            match idle_rx.recv().await {
                Some(IdleSignal::Idle) => armed = true,
                Some(IdleSignal::Busy) | Some(IdleSignal::Activity) => armed = false,
                Some(IdleSignal::Stop) | None => return,
            }
            continue;
        }

        tokio::select! {
            signal = idle_rx.recv() => match signal {
                Some(IdleSignal::Idle) | Some(IdleSignal::Activity) => armed = true,
                Some(IdleSignal::Busy) => armed = false,
                Some(IdleSignal::Stop) | None => return,
            },
            _ = tokio::time::sleep(idle_timeout) => {
                if closing.is_active() {
                    return;
                }
                {
                    let mut reason = close_reason.write().expect("close reason lock");
                    if reason.is_none() {
                        *reason = Some(IDLE_CLOSE_REASON.into());
                    }
                }
                info!(
                    target: "lucarne::agent_runtime::session",
                    provider_id = %provider_id,
                    instance_id = %instance_id.0,
                    session_id = %session_id.0,
                    idle_timeout_ms = idle_timeout.as_millis() as u64,
                    "auto-closing idle agent session"
                );
                closing.start();
                session.lock().await.close().await;
                return;
            }
        }
    }
}

async fn apply_drain_control(
    provider_id: ProviderId,
    capabilities: AgentCapabilities,
    internal_events: &mut mpsc::Receiver<crate::event::Event>,
    ready_tx: &mut Option<oneshot::Sender<Result<SessionId, AgentError>>>,
    ready: &mut bool,
    provider_ready_tx: &watch::Sender<bool>,
    provider_session_id: &Arc<StdRwLock<Option<SessionId>>>,
    close_reason: &Arc<StdRwLock<Option<SmolStr>>>,
    closing: &ClosingState,
    question_keys: &Arc<StdRwLock<BTreeMap<String, Vec<Vec<String>>>>>,
    idle_tx: &mpsc::UnboundedSender<IdleSignal>,
    activity_tx: &mpsc::Sender<()>,
    filter: &mut Option<RuntimeBusFilter>,
    pending_public: &mut VecDeque<super::events::Event>,
    typed_command: &mut Option<TypedCommandWaiter>,
    source_closed: &mut bool,
    control: DrainControl,
) {
    match control {
        DrainControl::BeginTypedCommand {
            command_name,
            registered_tx,
            result_tx,
        } => {
            if typed_command.is_some() {
                let _ = registered_tx.send(Err(invalid_state(
                    "another typed command is already waiting for completion",
                )));
                drop(result_tx);
                return;
            }
            *typed_command = Some(TypedCommandWaiter {
                command_name,
                result: None,
                tx: result_tx,
            });
            let _ = registered_tx.send(Ok(()));
        }
        DrainControl::CancelTypedCommand {
            command_name,
            error,
        } => {
            if typed_command
                .as_ref()
                .is_some_and(|waiter| waiter.command_name == command_name)
            {
                fail_typed_command(typed_command, error);
            }
        }
        DrainControl::ForceReady { session_id } => {
            if !*ready {
                *ready = true;
                let _ = ready_tx.take();
                let _ = provider_ready_tx.send(true);
                debug!(
                    target: "lucarne::agent_runtime::session",
                    provider_id = %provider_id,
                    session_id = %session_id.0,
                    "forced session ready before provider-native session id"
                );
            }
        }
        DrainControl::UpdateFilter {
            filter: next_filter,
            applied_tx,
        } => {
            debug!(
                target: "lucarne::agent_runtime::session",
                provider_id = %provider_id,
                session_lifecycle = next_filter.session_lifecycle,
                user_messages = next_filter.user_messages,
                assistant_messages = next_filter.assistant_messages,
                reasoning = next_filter.reasoning,
                tool_calls = next_filter.tool_calls,
                tool_results = next_filter.tool_results,
                usage = next_filter.usage,
                intervention_requests = next_filter.intervention_requests,
                "applying drain filter update"
            );
            if !closing.is_active() {
                while !*source_closed {
                    match internal_events.try_recv() {
                        Ok(event) => {
                            process_internal_event(
                                provider_id,
                                capabilities,
                                event,
                                ready_tx,
                                ready,
                                provider_ready_tx,
                                provider_session_id,
                                close_reason,
                                question_keys,
                                idle_tx,
                                activity_tx,
                                filter.as_ref(),
                                pending_public,
                                typed_command,
                                source_closed,
                            )
                            .await;
                        }
                        Err(mpsc::error::TryRecvError::Empty) => break,
                        Err(mpsc::error::TryRecvError::Disconnected) => {
                            *source_closed = true;
                            break;
                        }
                    }
                }
            }
            *filter = Some(next_filter);
            let _ = applied_tx.send(());
        }
    }
}

async fn process_internal_event(
    provider_id: ProviderId,
    capabilities: AgentCapabilities,
    event: crate::event::Event,
    ready_tx: &mut Option<oneshot::Sender<Result<SessionId, AgentError>>>,
    ready: &mut bool,
    provider_ready_tx: &watch::Sender<bool>,
    provider_session_id: &Arc<StdRwLock<Option<SessionId>>>,
    close_reason: &Arc<StdRwLock<Option<SmolStr>>>,
    question_keys: &Arc<StdRwLock<BTreeMap<String, Vec<Vec<String>>>>>,
    idle_tx: &mpsc::UnboundedSender<IdleSignal>,
    activity_tx: &mpsc::Sender<()>,
    filter: Option<&RuntimeBusFilter>,
    pending_public: &mut VecDeque<super::events::Event>,
    typed_command: &mut Option<TypedCommandWaiter>,
    source_closed: &mut bool,
) {
    trace!(
        target: "lucarne::agent_runtime::session",
        provider_id = %provider_id,
        payload = ?event.payload.kind(),
        "processing internal event"
    );
    if let crate::event::Payload::PermissionRequest(request) = &event.payload {
        if capabilities.structured_intervention && !request.questions.is_empty() {
            question_keys
                .write()
                .expect("question key registry lock")
                .insert(
                    request.req_id.clone(),
                    request
                        .questions
                        .iter()
                        .map(question_lookup_keys)
                        .collect::<Vec<_>>(),
                );
        }
    }

    let _ = idle_tx.send(IdleSignal::Activity);
    let _ = activity_tx.try_send(());
    let projection = Projector::project(capabilities, event);

    if let Some(session_id) = projection.session_id {
        *ready = true;
        *provider_session_id
            .write()
            .expect("provider session id lock") = Some(session_id.clone());
        let _ = provider_ready_tx.send(true);
        info!(
            target: "lucarne::agent_runtime::session",
            provider_id = %provider_id,
            session_id = %session_id.0,
            "provider-native session id surfaced"
        );
        if let Some(ready_tx) = ready_tx.take() {
            let _ = ready_tx.send(Ok(session_id));
        }
    }

    let projected_count = projection.events.len();
    for projected_event in projection.events {
        if matches!(
            projected_event,
            super::events::Event::TurnCompleted(_) | super::events::Event::TurnFailed(_)
        ) {
            let _ = idle_tx.send(IdleSignal::Idle);
        }
        if route_typed_command_event(typed_command, &projected_event) {
            continue;
        }
        if filter.map_or(true, |filter| projected_event.matches_filter(filter)) {
            trace!(
                target: "lucarne::agent_runtime::session",
                provider_id = %provider_id,
                event = public_event_name(&projected_event),
                "queueing projected public event"
            );
            pending_public.push_back(projected_event);
        }
    }
    if projected_count > 0 {
        debug!(
            target: "lucarne::agent_runtime::session",
            provider_id = %provider_id,
            projected_count,
            pending = pending_public.len(),
            "projected public events queued"
        );
    }

    if let Some(reason) = projection.close_reason {
        let mut observed = close_reason.write().expect("close reason lock");
        if observed.is_none() {
            *observed = Some(reason.clone());
        }
        drop(observed);
        let _ = idle_tx.send(IdleSignal::Stop);
        info!(
            target: "lucarne::agent_runtime::session",
            provider_id = %provider_id,
            reason = %reason,
            "observed close reason from wrapped session"
        );
        if !*ready {
            if let Some(ready_tx) = ready_tx.take() {
                let _ = ready_tx.send(Err(internal(format!(
                    "wrapped session exited before surfacing a provider-native session id: {}",
                    reason
                ))));
            }
        }
        *source_closed = true;
    }
}

fn into_permission_response(
    req_id: &str,
    response: InterventionResponse,
    question_keys: Option<&[Vec<String>]>,
) -> Result<PermissionResponse, AgentError> {
    match response {
        InterventionResponse::Approval(decision) => {
            Ok(PermissionResponse::from_decision(match decision {
                ApprovalDecision::Allow => Decision::Allow,
                ApprovalDecision::Deny => Decision::Deny,
            }))
        }
        InterventionResponse::Answers(response) => Ok(PermissionResponse {
            decision: Decision::Allow,
            answers: build_answer_map(req_id, response.answers, question_keys)?,
        }),
    }
}

fn question_lookup_keys(question: &crate::event::PermissionQuestion) -> Vec<String> {
    let mut keys = Vec::new();
    for key in [&question.id, &question.header, &question.question] {
        if !key.is_empty() {
            keys.push(key.clone());
        }
    }
    keys
}

fn build_answer_map(
    req_id: &str,
    answers: Vec<super::QuestionAnswer>,
    question_keys: Option<&[Vec<String>]>,
) -> Result<BTreeMap<String, PermissionAnswer>, AgentError> {
    let answers = answers
        .into_iter()
        .map(|answer| PermissionAnswer {
            answers: answer.values.into_iter().map(Into::into).collect(),
            text: String::new(),
        })
        .collect::<Vec<_>>();

    let Some(question_keys) = question_keys else {
        return Ok(answers
            .into_iter()
            .enumerate()
            .map(|(idx, answer)| (fallback_answer_key(idx), answer))
            .collect());
    };

    let question_keys = question_keys
        .iter()
        .map(|keys| {
            let mut deduped = Vec::new();
            for key in keys {
                if !deduped.contains(key) {
                    deduped.push(key.clone());
                }
            }
            deduped
        })
        .collect::<Vec<_>>();

    let mut answer_map = BTreeMap::new();
    let mut key_owners = BTreeMap::<String, usize>::new();
    for keys in &question_keys {
        for key in keys {
            *key_owners.entry(key.clone()).or_default() += 1;
        }
    }

    for (idx, answer) in answers.iter().enumerate() {
        let keys = question_keys.get(idx).ok_or_else(|| {
            invalid_state(format!(
                "session facade answer mapping out of bounds for request {req_id}"
            ))
        })?;

        if keys.is_empty() {
            let fallback_key = fallback_answer_key(idx);
            if answer_map.contains_key(&fallback_key) || key_owners.contains_key(&fallback_key) {
                return Err(invalid_state(format!(
                    "session facade cannot map structured answers for request {req_id}: colliding question lookup keys"
                )));
            }
            answer_map.insert(fallback_key, answer.clone());
            continue;
        }

        let chosen_key = keys
            .iter()
            .find(|key| key_owners.get(*key) == Some(&1))
            .cloned()
            .ok_or_else(|| {
                invalid_state(format!(
                    "session facade cannot map structured answers for request {req_id}: colliding question lookup keys"
                ))
            })?;

        if answer_map.contains_key(&chosen_key) {
            return Err(invalid_state(format!(
                "session facade cannot map structured answers for request {req_id}: colliding question lookup keys"
            )));
        }

        answer_map.insert(chosen_key, answer.clone());
    }

    Ok(answer_map)
}

fn fallback_answer_key(idx: usize) -> String {
    format!("__lucarne_pos_{idx:020}")
}

fn decode_agent_input(input: AgentInput) -> Result<Input, AgentError> {
    let mut images = Vec::with_capacity(input.images.len());
    for (idx, image) in input.images.into_iter().enumerate() {
        let media_type = image.media_type.trim().to_string();
        if media_type.is_empty() {
            return Err(unsupported(format!(
                "agent input image[{idx}] must include a media_type"
            )));
        }
        let data = base64::engine::general_purpose::STANDARD
            .decode(image.data_base64.as_str())
            .map_err(|err| {
                unsupported(format!(
                    "agent input image[{idx}] data_base64 is not valid base64: {err}"
                ))
            })?;
        if data.is_empty() {
            return Err(unsupported(format!(
                "agent input image[{idx}] must not decode to empty bytes"
            )));
        }
        images.push(ImageRef { media_type, data });
    }

    if input.text.is_empty() && images.is_empty() {
        return Err(unsupported(
            "agent input must include non-empty text or at least one image",
        ));
    }

    Ok(Input {
        text: input.text.into(),
        images,
    })
}

fn map_lucarne_error(err: crate::error::LucarneError) -> AgentError {
    let kind = match &err {
        crate::error::LucarneError::Closed => AgentErrorKind::InvalidState,
        crate::error::LucarneError::Adapter(msg) if msg.starts_with("unknown id ") => {
            AgentErrorKind::Unsupported
        }
        crate::error::LucarneError::Adapter(_)
        | crate::error::LucarneError::Dialect(_)
        | crate::error::LucarneError::Protocol(_)
        | crate::error::LucarneError::Launcher(_)
        | crate::error::LucarneError::Runtime(_)
        | crate::error::LucarneError::Io(_)
        | crate::error::LucarneError::Json(_)
        | crate::error::LucarneError::Timeout
        | crate::error::LucarneError::Other(_) => AgentErrorKind::Internal,
    };

    AgentError {
        kind,
        message: err.to_string().into(),
    }
}

fn invalid_state(message: impl Into<String>) -> AgentError {
    AgentError {
        kind: AgentErrorKind::InvalidState,
        message: message.into().into(),
    }
}

fn internal(message: impl Into<String>) -> AgentError {
    AgentError {
        kind: AgentErrorKind::Internal,
        message: message.into().into(),
    }
}

fn unsupported(message: impl Into<String>) -> AgentError {
    AgentError {
        kind: AgentErrorKind::Unsupported,
        message: message.into().into(),
    }
}

fn public_event_name(event: &super::events::Event) -> &'static str {
    match event {
        super::events::Event::Message(_) => "message",
        super::events::Event::Attachment(_) => "attachment",
        super::events::Event::Reasoning(_) => "reasoning",
        super::events::Event::ToolCall(_) => "tool_call",
        super::events::Event::ToolResult(_) => "tool_result",
        super::events::Event::Usage(_) => "usage",
        super::events::Event::CommandResult(_) => "command_result",
        super::events::Event::InterventionRequest(_) => "intervention_request",
        super::events::Event::TurnCompleted(_) => "turn_completed",
        super::events::Event::TurnFailed(_) => "turn_failed",
    }
}

fn intervention_response_name(response: &InterventionResponse) -> &'static str {
    match response {
        InterventionResponse::Approval(_) => "approval",
        InterventionResponse::Answers(_) => "answers",
    }
}
