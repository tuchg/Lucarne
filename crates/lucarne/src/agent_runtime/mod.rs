mod catalog;
pub mod error;
pub mod events;
pub mod project;
pub mod provider;
mod provider_args;
pub mod runtime;
pub mod session;
pub mod types;

pub type AgentEventStream = tokio::sync::mpsc::Receiver<events::Event>;
pub type AgentActivityStream = tokio::sync::mpsc::Receiver<()>;
pub type RuntimeBusStream = tokio::sync::mpsc::Receiver<types::RuntimeBusOutput>;

pub use crate::ProviderId;
pub use catalog::KnownAgentProvider;
pub use error::{AgentError, AgentErrorKind};
pub use events::{
    Attachment, CommandResultEvent, Event, MessageEvent, MessageRole, ReasoningEvent,
    ToolCallEvent, ToolResultEvent, UsageEvent,
};
pub use provider::{AgentProvider, ProtocolProvider};
pub use runtime::{AgentDescriptor, AgentRuntime, RuntimeBus};
pub use session::{AgentSession, AgentSessionFacade, AgentSessionOptions};
pub use types::{
    AgentCapabilities, AgentCommand, AgentCommandCatalog, AgentCommandCompletion,
    AgentCommandInput, AgentCommandInvocation, AgentCommandResult, AgentCommandResultData,
    AgentCommandSource, AgentContextUsage, AgentForkResult, AgentForkSelection, AgentForkTarget,
    AgentForkTargetCatalog, AgentImageInput, AgentInput, AgentModelCatalog, AgentModelOption,
    AgentModelSelection, AgentPermissionCatalog, AgentPermissionOption, AgentPermissionSelection,
    AgentReasoningOption, AgentSkillCatalog, AgentSkillSummary, AgentStatus, AgentTokenUsage,
    ApprovalDecision, ApprovalRequest, CallId, CommandId, CommandRejectedEvent, InstanceId,
    InterventionRequest, InterventionResponse, OpenSession, ProbeResult, Question, QuestionAnswer,
    QuestionOption, QuestionRequest, QuestionResponse, ResumeSession, RuntimeBusEvent,
    RuntimeBusFilter, RuntimeBusOutput, RuntimeCommand, SessionClosedEvent, SessionId,
    SessionOpenedEvent, SessionRef,
};
