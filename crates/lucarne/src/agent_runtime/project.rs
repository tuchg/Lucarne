use crate::event::{
    self, Event as CanonicalEvent, Payload, PermissionQuestion, TimelineType, ToolCall, Usage,
};
use serde_json::Value;
use smol_str::SmolStr;

use super::events::{
    Attachment, CommandResultEvent, Event as PublicEvent, MessageEvent, MessageRole,
    ReasoningEvent, ToolCallEvent, ToolResultEvent, TurnCompletedEvent, TurnFailedEvent,
    UsageEvent,
};
use super::types::{
    ApprovalRequest, CallId, InterventionRequest, Question, QuestionOption, QuestionRequest,
    SessionId,
};
use super::AgentCapabilities;

#[derive(Debug, Clone, PartialEq, Default)]
pub struct Projection {
    pub events: Vec<PublicEvent>,
    pub session_id: Option<SessionId>,
    pub close_reason: Option<SmolStr>,
}

pub struct Projector;

impl Projector {
    pub fn project(capabilities: AgentCapabilities, event: CanonicalEvent) -> Projection {
        let mut projection = Projection::default();
        match event.payload {
            Payload::SessionStarted(started) => {
                projection.session_id = Some(SessionId(started.session_id.into()));
            }
            Payload::SessionClosed(closed) => {
                projection.close_reason = Some(closed.reason.into());
            }
            Payload::Timeline(timeline) => {
                if let Some(event) = project_timeline(&timeline.item) {
                    projection.events.push(event);
                }
            }
            Payload::Attachment(attachment) => {
                projection.events.push(PublicEvent::Attachment(Attachment {
                    id: attachment.id,
                    filename: attachment.filename,
                    media_type: attachment.media_type,
                    data_base64: attachment.data_base64,
                    caption: attachment.caption,
                }));
            }
            Payload::PermissionRequest(request) => {
                if capabilities.structured_intervention {
                    if let Some(event) = project_permission_request(request) {
                        projection.events.push(event);
                    }
                }
            }
            Payload::UsageDelta(delta) => {
                if let Some(event) = project_usage(&delta.delta) {
                    projection.events.push(event);
                }
            }
            Payload::CommandResult(result) => {
                projection
                    .events
                    .push(PublicEvent::CommandResult(CommandResultEvent {
                        command: result.command.clone().into(),
                        result,
                    }));
            }
            Payload::TurnCompleted(completed) => {
                let usage = completed.usage.as_ref().and_then(|u| {
                    project_usage(u).and_then(|e| match e {
                        PublicEvent::Usage(u) => Some(u),
                        _ => None,
                    })
                });
                projection
                    .events
                    .push(PublicEvent::TurnCompleted(TurnCompletedEvent {
                        turn_id: completed.turn_id.clone().into(),
                        usage,
                    }));
            }
            Payload::TurnFailed(failed) => {
                projection
                    .events
                    .push(PublicEvent::TurnFailed(TurnFailedEvent {
                        turn_id: failed.turn_id.clone().into(),
                        error: failed.error.clone().into(),
                        code: failed.code.clone().into(),
                    }));
            }
            Payload::TurnStarted(_) => {}
            Payload::PermissionResolved(_) | Payload::Log(_) => {}
        }
        projection
    }
}

fn project_timeline(item: &event::TimelineItem) -> Option<PublicEvent> {
    match item.ty {
        TimelineType::UserMessage => item.user_message.as_ref().map(|message| {
            PublicEvent::Message(MessageEvent {
                role: MessageRole::User,
                text: message.text.clone().into(),
                streaming: false,
            })
        }),
        TimelineType::AssistantMessage => item.assistant_message.as_ref().map(|message| {
            PublicEvent::Message(MessageEvent {
                role: MessageRole::Assistant,
                text: message.text.clone().into(),
                streaming: message.streaming,
            })
        }),
        TimelineType::Reasoning => item.reasoning.as_ref().map(|reasoning| {
            PublicEvent::Reasoning(ReasoningEvent {
                text: reasoning.text.clone().into(),
            })
        }),
        TimelineType::ToolCall => item.tool_call.as_ref().and_then(|tool_call| {
            if suppress_public_tool_call(&tool_call.call) {
                return None;
            }
            let (name, input) = project_tool_call(tool_call.call.clone());
            Some(PublicEvent::ToolCall(ToolCallEvent {
                call_id: CallId(item.id.clone().into()),
                name,
                input,
            }))
        }),
        TimelineType::ToolResult => item.tool_result.as_ref().map(|tool_result| {
            PublicEvent::ToolResult(ToolResultEvent {
                call_id: CallId(tool_result.call_id.clone().into()),
                output: serde_json::to_value(&tool_result.result)
                    .expect("serialize canonical tool result"),
                is_error: Some(
                    tool_result.result.exit_code != 0 || !tool_result.result.error.is_empty(),
                ),
            })
        }),
        TimelineType::Todo | TimelineType::Plan | TimelineType::Compaction => None,
    }
}

fn project_tool_call(call: ToolCall) -> (SmolStr, Value) {
    (call.name, call.input)
}

fn suppress_public_tool_call(call: &ToolCall) -> bool {
    matches!(call.name.as_str(), "request_user_input" | "AskUserQuestion")
}

fn project_permission_request(request: crate::event::PermissionRequest) -> Option<PublicEvent> {
    if request.questions.is_empty() {
        return Some(PublicEvent::InterventionRequest(
            InterventionRequest::Approval(ApprovalRequest {
                req_id: request.req_id.into(),
                tool_name: request.tool.into(),
                message: None,
                input: request.input,
            }),
        ));
    }

    let questions = request
        .questions
        .into_iter()
        .map(project_question)
        .collect();

    Some(PublicEvent::InterventionRequest(
        InterventionRequest::Question(QuestionRequest {
            req_id: request.req_id.into(),
            questions,
        }),
    ))
}

fn project_question(question: PermissionQuestion) -> Question {
    Question {
        header: non_empty_box(question.header),
        text: question.question.into(),
        options: question
            .options
            .into_iter()
            .map(|option| QuestionOption {
                label: option.label.into(),
                description: non_empty_box(option.description),
            })
            .collect(),
        multi_select: question.multi_select,
    }
}

fn non_empty_box(value: String) -> Option<SmolStr> {
    if value.is_empty() {
        None
    } else {
        Some(value.into())
    }
}

fn project_usage(usage: &Usage) -> Option<PublicEvent> {
    let total_tokens = usage.total_tokens();
    Some(PublicEvent::Usage(UsageEvent {
        input_tokens: non_zero_u64(usage.input_tokens),
        output_tokens: non_zero_u64(usage.output_tokens),
        total_tokens,
        raw: serde_json::to_value(usage).expect("serialize canonical usage"),
    }))
}

trait UsageTotals {
    fn total_tokens(&self) -> Option<u64>;
}

impl UsageTotals for Usage {
    fn total_tokens(&self) -> Option<u64> {
        let total = self.input_tokens
            + self.output_tokens
            + self.cache_read_tokens
            + self.cache_write_tokens;
        if total > 0 {
            Some(total as u64)
        } else {
            None
        }
    }
}

fn non_zero_u64(value: i64) -> Option<u64> {
    if value > 0 {
        Some(value as u64)
    } else {
        None
    }
}
