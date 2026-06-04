#![allow(clippy::while_let_loop)]

mod live;

use base64::Engine;
use live::runtime::{
    open_live_runtime_session, resume_live_runtime_session_from_existing,
    run_live_runtime_turn_with_hooks, summarize_runtime_events, LiveRuntimeHooks,
    LiveRuntimeSession, LiveRuntimeTurnSpec, ReplayEffectsTrigger,
};
use live::{
    ensure_live_git_repo, live_canonical_path, live_delete_prompt, live_failure_prompt,
    live_question_prompt, live_tool_prompt, maybe_wrap_live_binary, prepare_recorded_provider,
    recorded_provider_or_return as replay_provider_or_return, LiveProvider, PreparedRecordingRun,
    RecordedLiveCase,
};
use lucarne::agent_runtime::{
    AgentCommandCatalog, AgentCommandInvocation, AgentCommandSource, AgentImageInput, AgentInput,
    AgentRuntime, ApprovalDecision, CommandId, Event, InterventionRequest, InterventionResponse,
    MessageRole, OpenSession, ProtocolProvider, QuestionAnswer, QuestionRequest, QuestionResponse,
    RuntimeBusFilter, RuntimeBusOutput, RuntimeBusStream, RuntimeCommand,
};
use lucarne::ProviderId;
use once_cell::sync::Lazy;
use serde_json::json;
use std::collections::{BTreeMap, BTreeSet};
use std::fs;
use std::future::Future;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::{Mutex, MutexGuard};
use tokio::time::{timeout, Instant};

static LIVE_RUNTIME_TEST_MUTEX: Lazy<Mutex<()>> = Lazy::new(|| Mutex::new(()));
const LIVE_VISION_EXPECTED_REPLY: &str = "SHRIMP";
static LIVE_VISION_IMAGE_PNG_BASE64: Lazy<String> = Lazy::new(|| {
    base64::engine::general_purpose::STANDARD
        .encode(include_bytes!("../../../tests/data/live/shrimp.png"))
});

fn recorded_provider_or_return(
    name: &str,
    suite: &'static str,
    case_id: &'static str,
) -> Option<LiveProvider> {
    replay_provider_or_return(name, RecordedLiveCase { suite, case_id })
}

fn recorded_fixture_exists(
    provider_name: &str,
    suite: &'static str,
    case_id: &'static str,
) -> bool {
    live::repo_root()
        .join("tests")
        .join("data")
        .join("live_recordings")
        .join(suite)
        .join(provider_name)
        .join(case_id)
        .join("session.fixture")
        .exists()
}

fn recorded_provider_or_skip_if_missing(
    provider_name: &str,
    suite: &'static str,
    case_id: &'static str,
) -> Option<LiveProvider> {
    let live_enabled = std::env::var("LUCARNE_LIVE_E2E").unwrap_or_default() == "1";
    if !live_enabled && !recorded_fixture_exists(provider_name, suite, case_id) {
        return None;
    }
    recorded_provider_or_return(provider_name, suite, case_id)
}

#[test]
fn required_pi_runtime_replay_fixtures_exist() {
    // Keep the Pi journey-510 runtime replay surface explicit. Missing fixtures
    // must fail loudly instead of silently downgrading coverage to a skip.
    let case_ids = [
        "agent_runtime_live_basic_conversation_pi",
        "agent_runtime_live_command_round_trip_pi",
        "agent_runtime_live_image_input_pi",
        "agent_runtime_live_question_flow_pi",
        "agent_runtime_live_multi_turn_conversation_pi",
        "agent_runtime_live_resume_flow_pi_seed",
        "agent_runtime_live_resume_flow_pi_resume",
        "agent_runtime_live_tool_flow_pi",
        "agent_runtime_live_approval_flow_pi",
        "agent_runtime_live_tool_failure_flow_pi",
        "agent_runtime_live_reject_flow_pi",
        "agent_runtime_live_interrupt_flow_pi",
    ];

    for case_id in case_ids {
        assert!(
            recorded_fixture_exists("pi", "agent_runtime_live_e2e", case_id),
            "missing required Pi runtime replay fixture: {case_id}"
        );
    }
}

async fn live_runtime_test_guard() -> MutexGuard<'static, ()> {
    LIVE_RUNTIME_TEST_MUTEX.lock().await
}

fn workspace_with_readme(contents: &str) -> tempfile::TempDir {
    let temp = tempfile::tempdir().unwrap();
    fs::write(temp.path().join("README.md"), contents).unwrap();
    temp
}

fn assistant_texts(events: &[Event]) -> Vec<String> {
    events
        .iter()
        .filter_map(|event| match event {
            Event::Message(message) if message.role == MessageRole::Assistant => {
                Some(message.text.to_string())
            }
            _ => None,
        })
        .collect()
}

fn assistant_transcript(events: &[Event]) -> String {
    assistant_texts(events).join("")
}

fn last_assistant_text(events: &[Event]) -> Option<String> {
    assistant_texts(events)
        .into_iter()
        .rev()
        .find(|text| !text.trim().is_empty())
}

fn basic_reply_prompt(token: &str) -> String {
    format!("Reply with exactly {token} and nothing else.")
}

fn log_live_multi_turn(provider_name: &str, turn: usize, detail: impl AsRef<str>) {
    let _ = (provider_name, turn, detail.as_ref());
}

fn summarize_runtime_event(event: &Event) -> String {
    summarize_runtime_events(std::slice::from_ref(event))
}

fn vision_reply_prompt() -> String {
    format!(
        "Reply with exactly {LIVE_VISION_EXPECTED_REPLY} if the attached image is a shrimp illustration, otherwise reply with exactly UNKNOWN. Do not add any other text."
    )
}

fn live_vision_input() -> AgentInput {
    AgentInput {
        text: vision_reply_prompt().into(),
        images: vec![AgentImageInput {
            media_type: "image/png".into(),
            data_base64: LIVE_VISION_IMAGE_PNG_BASE64.clone().into(),
        }],
    }
}

fn resume_seed_prompt() -> &'static str {
    "In one short sentence, invent a distinctive codename for this chat and explain it in four words or fewer."
}

fn resume_quote_prompt() -> &'static str {
    "Quote your previous answer in this same session verbatim and nothing else."
}

fn normalized_text(text: &str) -> String {
    text.chars().filter(|ch| !ch.is_whitespace()).collect()
}

fn runtime_question_response(request: &QuestionRequest) -> QuestionResponse {
    QuestionResponse {
        answers: request
            .questions
            .iter()
            .map(|question| QuestionAnswer {
                values: vec![question
                    .options
                    .first()
                    .map(|option| option.label.clone())
                    .unwrap_or_else(|| "yes".into())],
            })
            .collect(),
    }
}

fn has_error_tool_result(events: &[Event]) -> bool {
    events
        .iter()
        .any(|event| matches!(event, Event::ToolResult(result) if result.is_error == Some(true)))
}

fn has_success_tool_result(events: &[Event]) -> bool {
    events
        .iter()
        .any(|event| matches!(event, Event::ToolResult(result) if result.is_error == Some(false)))
}

fn has_tool_call_named(events: &[Event], name: &str) -> bool {
    events
        .iter()
        .any(|event| matches!(event, Event::ToolCall(tool_call) if tool_call.name == name))
}

async fn next_runtime_event(live: &mut LiveRuntimeSession) -> Event {
    timeout(live.timeout, live.events.recv())
        .await
        .expect("runtime event timeout")
        .expect("runtime event")
}

async fn collect_until_quiet(live: &mut LiveRuntimeSession, events: &mut Vec<Event>) {
    loop {
        match timeout(live.post_turn_quiet(), live.events.recv()).await {
            Ok(Some(event)) => events.push(event),
            Ok(None) | Err(_) => break,
        }
    }
}

async fn collect_until_turn_finished(live: &mut LiveRuntimeSession, events: &mut Vec<Event>) {
    loop {
        let event = next_runtime_event(live).await;
        let done = matches!(event, Event::TurnCompleted(_) | Event::TurnFailed(_));
        events.push(event);
        if done {
            collect_until_quiet(live, events).await;
            break;
        }
    }
}

async fn close_live_runtime_session(
    live: &mut LiveRuntimeSession,
    events: &mut Vec<Event>,
    context: &str,
) -> bool {
    live.session
        .close()
        .await
        .expect("close live runtime session");
    let close_deadline = Instant::now() + Duration::from_secs(5);
    loop {
        tokio::select! {
            maybe_ev = live.events.recv() => {
                match maybe_ev {
                    Some(event) => events.push(event),
                    None => break true,
                }
            }
            _ = tokio::time::sleep_until(close_deadline) => {
                panic!(
                    "{context} did not close after turn; events: {}",
                    summarize_runtime_events(events)
                );
            }
        }
    }
}

struct RegisteredLiveProvider {
    provider: LiveProvider,
    recording: Option<PreparedRecordingRun>,
    _wrapper_root: tempfile::TempDir,
}

struct PreparedOpenSession {
    request: OpenSession,
    _temp_root: tempfile::TempDir,
}

fn register_live_provider(
    runtime: &AgentRuntime,
    provider: LiveProvider,
    workdir: &std::path::Path,
    recorded_case: Option<RecordedLiveCase>,
) -> Result<RegisteredLiveProvider, String> {
    let wrapper_root = tempfile::tempdir().map_err(|err| format!("tempdir: {err}"))?;
    let recording = match recorded_case {
        Some(case) => prepare_recorded_provider(wrapper_root.path(), &provider, case, workdir)?,
        None => None,
    };
    let runtime_provider = if let Some(recording) = recording.as_ref() {
        recording.provider.clone()
    } else {
        let wrapped_binary =
            maybe_wrap_live_binary(wrapper_root.path(), provider.name(), &provider.binary)?;
        provider.with_binary(wrapped_binary)
    };
    runtime.register(Arc::new(
        ProtocolProvider::new(runtime_provider.adapter()).expect("live protocol provider"),
    ));
    Ok(RegisteredLiveProvider {
        provider,
        recording,
        _wrapper_root: wrapper_root,
    })
}

impl RegisteredLiveProvider {
    fn post_turn_quiet(&self) -> Duration {
        if self
            .recording
            .as_ref()
            .is_some_and(PreparedRecordingRun::is_replay)
        {
            live::LIVE_REPLAY_POST_TURN_QUIET
        } else {
            self.provider.post_turn_quiet()
        }
    }

    fn apply_recorded_effects(&mut self, workdir: &std::path::Path) {
        if let Some(recording) = self.recording.as_mut() {
            recording
                .apply_recorded_effects(workdir)
                .unwrap_or_else(|err| panic!("apply shared runtime replay effects: {err}"));
        }
    }

    fn finish_recording(&mut self, workdir: &std::path::Path) {
        if let Some(recording) = self.recording.as_mut() {
            recording
                .finish(workdir)
                .unwrap_or_else(|err| panic!("finalize shared runtime recording: {err}"));
        }
    }
}

fn build_live_open_request(
    provider: &LiveProvider,
    workdir: &std::path::Path,
    prompt: &str,
) -> Result<PreparedOpenSession, String> {
    let workdir = live_canonical_path(workdir);
    ensure_live_git_repo(&workdir)?;
    let temp_root = tempfile::tempdir().map_err(|err| format!("tempdir: {err}"))?;
    let extra_env = provider.extra_env(temp_root.path(), &workdir)?;
    let extra_args = provider.extra_args(&workdir);
    let args = match provider.name() {
        "claude" => json!({
            "system_prompt": "Be concise. When asked to reply with an exact token, output only that token.",
            "extra_env": extra_env,
            "extra_args": extra_args,
        }),
        "codex" | "gemini" | "pi" => json!({
            "extra_env": extra_env,
            "extra_args": extra_args,
        }),
        other => return Err(format!("unsupported live runtime provider {other}")),
    };

    Ok(PreparedOpenSession {
        request: OpenSession {
            model: Some(provider.model.clone().into()),
            cwd: Some(workdir.to_string_lossy().into_owned().into()),
            initial_input: Some(AgentInput {
                text: prompt.to_string().into(),
                images: Vec::new(),
            }),
            idle_timeout_ms: None,
            args,
        },
        _temp_root: temp_root,
    })
}

fn build_live_open_request_without_input(
    provider: &LiveProvider,
    workdir: &std::path::Path,
) -> Result<PreparedOpenSession, String> {
    let workdir = live_canonical_path(workdir);
    ensure_live_git_repo(&workdir)?;
    let temp_root = tempfile::tempdir().map_err(|err| format!("tempdir: {err}"))?;
    let extra_env = provider.extra_env(temp_root.path(), &workdir)?;
    let extra_args = provider.extra_args(&workdir);
    let args = match provider.name() {
        "claude" => json!({
            "system_prompt": "Be concise. When asked to reply with an exact token, output only that token.",
            "extra_env": extra_env,
            "extra_args": extra_args,
        }),
        "codex" | "gemini" | "pi" => json!({
            "extra_env": extra_env,
            "extra_args": extra_args,
        }),
        other => return Err(format!("unsupported live runtime provider {other}")),
    };

    Ok(PreparedOpenSession {
        request: OpenSession {
            model: Some(provider.model.clone().into()),
            cwd: Some(workdir.to_string_lossy().into_owned().into()),
            initial_input: None,
            idle_timeout_ms: None,
            args,
        },
        _temp_root: temp_root,
    })
}

fn runtime_command_names(catalog: &AgentCommandCatalog) -> BTreeSet<String> {
    catalog
        .commands
        .iter()
        .map(|command| command.name.to_string())
        .collect()
}

async fn wait_for_command_catalog(
    session: &dyn lucarne::agent_runtime::AgentSession,
    expected: &[&str],
    context: &str,
) -> AgentCommandCatalog {
    let deadline = Instant::now() + Duration::from_secs(5);
    loop {
        let catalog = session
            .list_commands()
            .await
            .unwrap_or_else(|err| panic!("{context} commands failed: {err}"));
        let names = runtime_command_names(&catalog);
        if expected.iter().all(|name| names.contains(*name)) {
            return catalog;
        }
        assert!(
            Instant::now() < deadline,
            "{context} missing commands {expected:?}; saw {names:?}"
        );
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
}

async fn recv_bus_output(
    events: &mut RuntimeBusStream,
    timeout_window: Duration,
) -> RuntimeBusOutput {
    timeout(timeout_window, events.recv())
        .await
        .expect("runtime bus output timeout")
        .expect("runtime bus output")
}

async fn assert_live_resume_round_trip(provider_name: &str) {
    let seed_case = match provider_name {
        "claude" => "agent_runtime_live_resume_flow_claude_seed",
        "codex" => "agent_runtime_live_resume_flow_codex_seed",
        "gemini" => "agent_runtime_live_resume_flow_gemini_seed",
        "pi" => "agent_runtime_live_resume_flow_pi_seed",
        other => panic!("unsupported resume provider {other}"),
    };
    let resume_case = match provider_name {
        "claude" => "agent_runtime_live_resume_flow_claude_resume",
        "codex" => "agent_runtime_live_resume_flow_codex_resume",
        "gemini" => "agent_runtime_live_resume_flow_gemini_resume",
        "pi" => "agent_runtime_live_resume_flow_pi_resume",
        other => panic!("unsupported resume provider {other}"),
    };
    let Some(provider) =
        recorded_provider_or_return(provider_name, "agent_runtime_live_e2e", seed_case)
    else {
        return;
    };
    let temp = workspace_with_readme(&format!(
        "agent runtime live {provider_name} resume workspace\n"
    ));

    let mut initial = open_live_runtime_session(
        LiveRuntimeTurnSpec::new(
            provider.clone(),
            temp.path().to_path_buf(),
            resume_seed_prompt(),
        )
        .recorded("agent_runtime_live_e2e", seed_case),
    )
    .await
    .unwrap();

    let mut initial_events = Vec::new();
    loop {
        let event = next_runtime_event(&mut initial).await;
        initial_events.push(event);
        if !assistant_transcript(&initial_events).trim().is_empty() {
            break;
        }
    }
    collect_until_quiet(&mut initial, &mut initial_events).await;
    let seed_reply =
        normalized_text(&last_assistant_text(&initial_events).expect("seed assistant reply"));

    let session_ref = initial.session.id().0.to_string();
    let initial_closed = close_live_runtime_session(
        &mut initial,
        &mut initial_events,
        &format!("runtime live {provider_name} resume seed session"),
    )
    .await;

    assert!(
        !seed_reply.is_empty(),
        "missing assistant seed reply before resume; events: {}",
        summarize_runtime_events(&initial_events)
    );
    assert!(
        initial_closed,
        "expected live runtime seed session to close"
    );

    let mut resumed = resume_live_runtime_session_from_existing(
        &initial,
        temp.path().to_path_buf(),
        &session_ref,
        Some(RecordedLiveCase {
            suite: "agent_runtime_live_e2e",
            case_id: resume_case,
        }),
    )
    .await
    .unwrap();
    let resumed_id = resumed.session.id().0.to_string();

    resumed
        .session
        .submit(AgentInput {
            text: resume_quote_prompt().into(),
            images: Vec::new(),
        })
        .await
        .expect("submit live runtime resume prompt");

    let mut resumed_events = Vec::new();
    loop {
        let event = next_runtime_event(&mut resumed).await;
        resumed_events.push(event);
        if !assistant_transcript(&resumed_events).trim().is_empty() {
            break;
        }
    }
    collect_until_quiet(&mut resumed, &mut resumed_events).await;
    let resumed_reply =
        normalized_text(&last_assistant_text(&resumed_events).expect("resumed assistant reply"));

    let resumed_closed = close_live_runtime_session(
        &mut resumed,
        &mut resumed_events,
        &format!("runtime live {provider_name} resumed session"),
    )
    .await;

    assert_eq!(
        resumed_id, session_ref,
        "resumed session id should match prior provider-native id"
    );
    assert!(
        resumed_reply.contains(&seed_reply),
        "missing resumed assistant recall for provider {provider_name}; seed={seed_reply:?}; resumed={resumed_reply:?}; events: {}",
        summarize_runtime_events(&resumed_events)
    );
    assert!(
        resumed_closed,
        "expected live runtime resumed session to close"
    );
}

async fn assert_live_resume_round_trip_with_retry(provider_name: &'static str, attempts: usize) {
    let mut last_error = None;
    for attempt in 1..=attempts {
        match tokio::spawn(assert_live_resume_round_trip(provider_name)).await {
            Ok(()) => return,
            Err(err) => {
                last_error = Some(format!("attempt {attempt}: {err}"));
            }
        }
    }
    panic!(
        "live runtime resume round-trip failed for provider {provider_name} after {attempts} attempts: {}",
        last_error.unwrap_or_else(|| "unknown failure".into())
    );
}

async fn run_live_test_with_retry<F, Fut>(name: &'static str, attempts: usize, scenario: F)
where
    F: Fn() -> Fut,
    Fut: Future<Output = ()> + Send + 'static,
{
    let mut last_error = None;
    for attempt in 1..=attempts {
        match tokio::spawn(scenario()).await {
            Ok(()) => return,
            Err(err) => last_error = Some(format!("attempt {attempt}: {err}")),
        }
    }
    panic!(
        "{name} failed after {attempts} attempts: {}",
        last_error.unwrap_or_else(|| "unknown failure".into())
    );
}

struct LiveCommandSpec {
    name: &'static str,
    args: Option<&'static str>,
    expected_source: AgentCommandSource,
    expect_public_event: bool,
}

async fn assert_live_command_round_trips(
    provider_name: &'static str,
    case_id: &'static str,
    commands: &[LiveCommandSpec],
) {
    let Some(provider) =
        recorded_provider_or_return(provider_name, "agent_runtime_live_e2e", case_id)
    else {
        return;
    };
    let temp = workspace_with_readme(&format!(
        "agent runtime live {provider_name} command workspace\n"
    ));
    let runtime = AgentRuntime::new();
    let mut registered = register_live_provider(
        &runtime,
        provider,
        temp.path(),
        Some(RecordedLiveCase {
            suite: "agent_runtime_live_e2e",
            case_id,
        }),
    )
    .unwrap();
    let open = if provider_name == "claude" {
        build_live_open_request(
            &registered.provider,
            temp.path(),
            &basic_reply_prompt("COMMAND_BOOTSTRAP_OK"),
        )
    } else {
        build_live_open_request_without_input(&registered.provider, temp.path())
    }
    .unwrap();

    let session = timeout(
        registered.provider.timeout(),
        runtime.open(registered.provider.name(), open.request),
    )
    .await
    .expect("open live command session timeout")
    .expect("open live command session");
    let mut events = session
        .take_events()
        .await
        .expect("take live command events");
    let catalog_context = format!("runtime live {provider_name} command catalog");
    let mut collected = Vec::new();

    if provider_name == "claude" {
        let hard_deadline = Instant::now() + registered.provider.timeout();
        loop {
            tokio::select! {
                maybe_ev = events.recv() => {
                    let Some(event) = maybe_ev else {
                        panic!(
                            "runtime live claude command bootstrap stream closed before init reply; events: {}",
                            summarize_runtime_events(&collected)
                        );
                    };
                    let failure = match &event {
                        Event::TurnFailed(failure) => Some((failure.error.clone(), failure.code.clone())),
                        _ => None,
                    };
                    collected.push(event);
                    if let Some((error, code)) = failure {
                        panic!(
                            "runtime live claude command bootstrap failed: {error} (code={code}); events: {}",
                            summarize_runtime_events(&collected)
                        );
                    }
                    if assistant_transcript(&collected).contains("COMMAND_BOOTSTRAP_OK")
                        && collected.iter().any(|event| matches!(event, Event::TurnCompleted(_)))
                    {
                        break;
                    }
                }
                _ = tokio::time::sleep_until(hard_deadline) => {
                    panic!(
                        "runtime live claude command bootstrap timed out before init reply; events: {}",
                        summarize_runtime_events(&collected)
                    );
                }
            }
        }
        loop {
            match timeout(registered.post_turn_quiet(), events.recv()).await {
                Ok(Some(event)) => {
                    let failure = match &event {
                        Event::TurnFailed(failure) => {
                            Some((failure.error.clone(), failure.code.clone()))
                        }
                        _ => None,
                    };
                    collected.push(event);
                    if let Some((error, code)) = failure {
                        panic!(
                            "runtime live claude command bootstrap failed after completion: {error} (code={code}); events: {}",
                            summarize_runtime_events(&collected)
                        );
                    }
                }
                Ok(None) | Err(_) => break,
            }
        }
    }

    let expected_commands: Vec<&str> = commands
        .iter()
        .filter(|command| command.expected_source == AgentCommandSource::ProviderNative)
        .map(|command| command.name)
        .collect();
    let catalog =
        wait_for_command_catalog(session.as_ref(), &expected_commands, &catalog_context).await;

    for command in commands {
        if command.expected_source == AgentCommandSource::ProviderNative {
            let catalog_command = catalog
                .commands
                .iter()
                .find(|catalog_command| catalog_command.name.as_str() == command.name)
                .unwrap_or_else(|| panic!("missing command {} in {catalog:?}", command.name));
            assert_eq!(catalog_command.source, command.expected_source);
        } else {
            assert!(
                !catalog
                    .commands
                    .iter()
                    .any(|catalog_command| catalog_command.name.as_str() == command.name),
                "adapter-mapped command {} must not be injected into /commands: {catalog:?}",
                command.name
            );
        }

        let before_len = collected.len();
        session
            .invoke_command(AgentCommandInvocation {
                name: command.name.into(),
                args: command.args.map(Into::into),
                values: serde_json::Value::Null,
                source: command.expected_source,
            })
            .await
            .unwrap_or_else(|err| panic!("invoke live command {}: {err}", command.name));

        if command.expect_public_event {
            let hard_deadline = Instant::now() + registered.provider.timeout();
            loop {
                tokio::select! {
                    maybe_ev = events.recv() => {
                        let Some(event) = maybe_ev else {
                            panic!(
                                "runtime live {provider_name} command {} stream closed before public completion event; events: {}",
                                command.name,
                                summarize_runtime_events(&collected[before_len..])
                            );
                        };
                        let completes_command = matches!(
                            event,
                            Event::Message(ref message)
                                if message.role == MessageRole::Assistant
                                    && !message.text.trim().is_empty()
                        ) || matches!(event, Event::TurnCompleted(_));
                        let failure = match &event {
                            Event::TurnFailed(failure) => Some((failure.error.clone(), failure.code.clone())),
                            _ => None,
                        };
                        collected.push(event);
                        if let Some((error, code)) = failure {
                            panic!(
                                "runtime live {provider_name} command {} failed: {error} (code={code}); events: {}",
                                command.name,
                                summarize_runtime_events(&collected[before_len..])
                            );
                        }
                        if completes_command {
                            break;
                        }
                    }
                    _ = tokio::time::sleep_until(hard_deadline) => {
                        panic!(
                            "runtime live {provider_name} command {} produced no public completion event; events: {}",
                            command.name,
                            summarize_runtime_events(&collected[before_len..])
                        );
                    }
                }
            }
        }

        loop {
            match timeout(registered.post_turn_quiet(), events.recv()).await {
                Ok(Some(event)) => {
                    let failure = match &event {
                        Event::TurnFailed(failure) => {
                            Some((failure.error.clone(), failure.code.clone()))
                        }
                        _ => None,
                    };
                    collected.push(event);
                    if let Some((error, code)) = failure {
                        panic!(
                            "runtime live {provider_name} command {} failed after public completion: {error} (code={code}); events: {}",
                            command.name,
                            summarize_runtime_events(&collected[before_len..])
                        );
                    }
                }
                Ok(None) | Err(_) => break,
            }
        }
        let response_text = assistant_transcript(&collected[before_len..]);
        let expected_fragments =
            expected_live_command_response_fragments(provider_name, command.name, command.args);
        assert!(
            !command.expect_public_event
                || !expected_fragments.is_empty()
                || fragmentless_live_command_reason(provider_name, command.name, command.args)
                    .is_some(),
            "runtime live {provider_name} command {} has no expected output assertion",
            command.name
        );
        for fragment in expected_fragments {
            assert!(
                response_text.contains(fragment),
                "runtime live {provider_name} command {} response missing fragment {fragment:?}; response: {response_text:?}; events: {}",
                command.name,
                summarize_runtime_events(&collected[before_len..])
            );
        }
    }

    session.close().await.expect("close live command session");
    let close_deadline = Instant::now() + Duration::from_secs(5);
    loop {
        tokio::select! {
            maybe_ev = events.recv() => {
                match maybe_ev {
                    Some(event) => collected.push(event),
                    None => break,
                }
            }
            _ = tokio::time::sleep_until(close_deadline) => {
                panic!(
                    "runtime live {provider_name} command session did not close; events: {}",
                    summarize_runtime_events(&collected)
                );
            }
        }
    }
    registered.finish_recording(temp.path());

    assert!(
        !assistant_transcript(&collected).trim().is_empty(),
        "missing command reply for provider {provider_name}; events: {}",
        summarize_runtime_events(&collected)
    );
}

fn expected_live_command_response_fragments(
    provider_name: &str,
    command_name: &str,
    command_args: Option<&str>,
) -> &'static [&'static str] {
    match (provider_name, command_name, command_args) {
        ("codex", "rename", Some("live command thread")) => &["Renamed thread."],
        ("codex", "stop", None) => &["Stopping all background terminals."],
        ("codex", "status", None) => &["Status:", "Model:", "Directory:", "Session:"],
        ("codex", "model", None) => &[
            "Available models",
            "`gpt-5.5`",
            "reasoning: `low`, `medium` (default), `high`, `xhigh`",
        ],
        ("codex", "model", Some("gpt-5.4 high")) => {
            &["Updated model to gpt-5.4 with reasoning effort high."]
        }
        ("codex", "skills", None) => &["Available skills", "`superpowers:brainstorming`"],
        ("codex", "mcp", None) => &["MCP servers"],
        ("codex", "plugins", None) => &["Plugins"],
        ("codex", "permissions", None) => &["Permission modes", "1. `default`", "3. `full-access`"],
        ("codex", "permissions", Some("auto-review")) => &["Updated permissions to Auto-review."],
        ("claude", "context", None) => &["Context Usage", "Model:", "Tokens:"],
        ("claude", "status", None) => &["Status:", "Model:", "Directory:", "Session:"],
        ("claude", "model", None) => &["Available models", "current: `"],
        ("claude", "model", Some("claude-sonnet-4-6 high")) => {
            &["Updated model to claude-sonnet-4-6 with reasoning effort high."]
        }
        ("claude", "skills", None) => &["Available skills", "`"],
        ("claude", "permissions", None) => {
            &["Permission modes", "1. `default`", "5. `bypassPermissions`"]
        }
        ("claude", "permissions", Some("acceptEdits")) => &["Updated permissions to acceptEdits."],
        ("gemini", "model", None) => &["Available models"],
        ("gemini", "model", Some("gemini-2.5-flash")) => &["Updated model to gemini-2.5-flash."],
        ("gemini", "permissions", None) => &["Permission modes"],
        ("gemini", "permissions", Some("autoEdit")) => &["Updated permissions to autoEdit."],
        ("pi", "status", None) => &["Status:", "Model:", "Directory:", "Session:"],
        ("pi", "model", None) => &["Available models"],
        ("pi", "skills", None) => &["Available skills"],
        ("pi", "permissions", None) => &["Permission modes"],
        _ => &[],
    }
}

fn fragmentless_live_command_reason(
    provider_name: &str,
    command_name: &str,
    command_args: Option<&str>,
) -> Option<&'static str> {
    match (provider_name, command_name, command_args) {
        ("codex", "compact", None) => {
            Some("Codex compact currently completes with turn_completed and no assistant text")
        }
        ("codex", "review", None) => Some("provider-native review text is model-specific"),
        ("claude", "review", None) => {
            Some("provider-native PR review text is environment-specific")
        }
        ("gemini", "help", None) => Some("provider-native help text is version-specific"),
        ("gemini", "about", None) => Some("provider-native about text is version-specific"),
        _ => None,
    }
}

#[tokio::test]
async fn agent_runtime_live_basic_conversation_claude() {
    let _guard = live_runtime_test_guard().await;
    let Some(provider) = recorded_provider_or_return(
        "claude",
        "agent_runtime_live_e2e",
        "agent_runtime_live_basic_conversation_claude",
    ) else {
        return;
    };
    let temp = workspace_with_readme("agent runtime live basic claude workspace\n");

    let mut live = open_live_runtime_session(
        LiveRuntimeTurnSpec::new(
            provider,
            temp.path().to_path_buf(),
            basic_reply_prompt("LIVE_OK"),
        )
        .recorded(
            "agent_runtime_live_e2e",
            "agent_runtime_live_basic_conversation_claude",
        ),
    )
    .await
    .unwrap();

    let mut events = Vec::new();
    loop {
        let event = next_runtime_event(&mut live).await;
        events.push(event);
        if assistant_transcript(&events).contains("LIVE_OK") {
            break;
        }
    }
    let closed =
        close_live_runtime_session(&mut live, &mut events, "runtime live claude basic session")
            .await;

    assert!(
        assistant_transcript(&events).contains("LIVE_OK"),
        "missing LIVE_OK assistant reply; events: {}",
        summarize_runtime_events(&events)
    );
    assert!(closed, "expected live runtime session to close");
}

#[tokio::test]
async fn agent_runtime_live_basic_conversation_codex() {
    let _guard = live_runtime_test_guard().await;
    let Some(provider) = recorded_provider_or_return(
        "codex",
        "agent_runtime_live_e2e",
        "agent_runtime_live_basic_conversation_codex",
    ) else {
        return;
    };
    let temp = workspace_with_readme("agent runtime live basic codex workspace\n");

    let res = run_live_runtime_turn_with_hooks(
        LiveRuntimeTurnSpec::new(
            provider,
            temp.path().to_path_buf(),
            basic_reply_prompt("LIVE_OK"),
        )
        .recorded(
            "agent_runtime_live_e2e",
            "agent_runtime_live_basic_conversation_codex",
        ),
        LiveRuntimeHooks {
            finish_when: Some(Arc::new(|events| {
                assistant_transcript(events).contains("LIVE_OK")
            })),
            ..Default::default()
        },
    )
    .await
    .unwrap();

    assert!(
        assistant_transcript(&res.events).contains("LIVE_OK"),
        "missing LIVE_OK assistant reply; events: {}",
        summarize_runtime_events(&res.events)
    );
    assert!(res.closed, "expected live runtime session to close");
}

#[tokio::test]
async fn agent_runtime_live_basic_conversation_gemini() {
    let _guard = live_runtime_test_guard().await;
    let Some(provider) = recorded_provider_or_return(
        "gemini",
        "agent_runtime_live_e2e",
        "agent_runtime_live_basic_conversation_gemini",
    ) else {
        return;
    };
    let temp = workspace_with_readme("agent runtime live basic gemini workspace\n");

    let res = run_live_runtime_turn_with_hooks(
        LiveRuntimeTurnSpec::new(
            provider,
            temp.path().to_path_buf(),
            basic_reply_prompt("LIVE_OK"),
        )
        .recorded(
            "agent_runtime_live_e2e",
            "agent_runtime_live_basic_conversation_gemini",
        ),
        LiveRuntimeHooks {
            finish_when: Some(Arc::new(|events| {
                assistant_transcript(events).contains("LIVE_OK")
            })),
            ..Default::default()
        },
    )
    .await
    .unwrap();

    assert!(
        assistant_transcript(&res.events).contains("LIVE_OK"),
        "missing LIVE_OK assistant reply; events: {}",
        summarize_runtime_events(&res.events)
    );
    assert!(res.closed, "expected live runtime session to close");
}

#[tokio::test]
async fn agent_runtime_live_basic_conversation_pi() {
    let _guard = live_runtime_test_guard().await;
    let Some(provider) = recorded_provider_or_return(
        "pi",
        "agent_runtime_live_e2e",
        "agent_runtime_live_basic_conversation_pi",
    ) else {
        return;
    };
    let temp = workspace_with_readme("agent runtime live basic pi workspace\n");
    let res = run_live_runtime_turn_with_hooks(
        LiveRuntimeTurnSpec::new(
            provider,
            temp.path().to_path_buf(),
            basic_reply_prompt("LIVE_OK"),
        )
        .recorded(
            "agent_runtime_live_e2e",
            "agent_runtime_live_basic_conversation_pi",
        ),
        LiveRuntimeHooks {
            finish_when: Some(Arc::new(|events| {
                assistant_transcript(events).contains("LIVE_OK")
            })),
            ..Default::default()
        },
    )
    .await
    .unwrap();
    assert!(
        assistant_transcript(&res.events).contains("LIVE_OK"),
        "missing LIVE_OK assistant reply; events: {}",
        summarize_runtime_events(&res.events)
    );
    assert!(res.closed, "expected live runtime session to close");
}

#[tokio::test]
async fn agent_runtime_live_command_round_trip_claude() {
    let _guard = live_runtime_test_guard().await;
    assert_live_command_round_trips(
        "claude",
        "agent_runtime_live_command_round_trip_claude",
        &[
            LiveCommandSpec {
                name: "context",
                args: None,
                expected_source: AgentCommandSource::ProviderNative,
                expect_public_event: true,
            },
            LiveCommandSpec {
                name: "review",
                args: None,
                expected_source: AgentCommandSource::ProviderNative,
                expect_public_event: true,
            },
            LiveCommandSpec {
                name: "status",
                args: None,
                expected_source: AgentCommandSource::AdapterMapped,
                expect_public_event: true,
            },
            LiveCommandSpec {
                name: "model",
                args: None,
                expected_source: AgentCommandSource::AdapterMapped,
                expect_public_event: true,
            },
            LiveCommandSpec {
                name: "model",
                args: Some("claude-sonnet-4-6 high"),
                expected_source: AgentCommandSource::AdapterMapped,
                expect_public_event: true,
            },
            LiveCommandSpec {
                name: "skills",
                args: None,
                expected_source: AgentCommandSource::AdapterMapped,
                expect_public_event: true,
            },
            LiveCommandSpec {
                name: "permissions",
                args: None,
                expected_source: AgentCommandSource::AdapterMapped,
                expect_public_event: true,
            },
            LiveCommandSpec {
                name: "permissions",
                args: Some("acceptEdits"),
                expected_source: AgentCommandSource::AdapterMapped,
                expect_public_event: true,
            },
        ],
    )
    .await;
}

#[tokio::test]
async fn agent_runtime_live_command_round_trip_codex() {
    let _guard = live_runtime_test_guard().await;
    assert_live_command_round_trips(
        "codex",
        "agent_runtime_live_command_round_trip_codex",
        &[
            LiveCommandSpec {
                name: "review",
                args: None,
                expected_source: AgentCommandSource::ProviderNative,
                expect_public_event: true,
            },
            LiveCommandSpec {
                name: "compact",
                args: None,
                expected_source: AgentCommandSource::ProviderNative,
                expect_public_event: true,
            },
            LiveCommandSpec {
                name: "rename",
                args: Some("live command thread"),
                expected_source: AgentCommandSource::ProviderNative,
                expect_public_event: true,
            },
            LiveCommandSpec {
                name: "status",
                args: None,
                expected_source: AgentCommandSource::AdapterMapped,
                expect_public_event: true,
            },
            LiveCommandSpec {
                name: "model",
                args: None,
                expected_source: AgentCommandSource::AdapterMapped,
                expect_public_event: true,
            },
            LiveCommandSpec {
                name: "model",
                args: Some("gpt-5.4 high"),
                expected_source: AgentCommandSource::AdapterMapped,
                expect_public_event: true,
            },
            LiveCommandSpec {
                name: "skills",
                args: None,
                expected_source: AgentCommandSource::AdapterMapped,
                expect_public_event: true,
            },
            LiveCommandSpec {
                name: "mcp",
                args: None,
                expected_source: AgentCommandSource::ProviderNative,
                expect_public_event: true,
            },
            LiveCommandSpec {
                name: "plugins",
                args: None,
                expected_source: AgentCommandSource::ProviderNative,
                expect_public_event: true,
            },
            LiveCommandSpec {
                name: "permissions",
                args: None,
                expected_source: AgentCommandSource::AdapterMapped,
                expect_public_event: true,
            },
            LiveCommandSpec {
                name: "permissions",
                args: Some("auto-review"),
                expected_source: AgentCommandSource::AdapterMapped,
                expect_public_event: true,
            },
            LiveCommandSpec {
                name: "stop",
                args: None,
                expected_source: AgentCommandSource::ProviderNative,
                expect_public_event: true,
            },
        ],
    )
    .await;
}

#[tokio::test]
async fn agent_runtime_live_command_round_trip_gemini() {
    let _guard = live_runtime_test_guard().await;
    assert_live_command_round_trips(
        "gemini",
        "agent_runtime_live_command_round_trip_gemini",
        &[
            LiveCommandSpec {
                name: "help",
                args: None,
                expected_source: AgentCommandSource::ProviderNative,
                expect_public_event: true,
            },
            LiveCommandSpec {
                name: "about",
                args: None,
                expected_source: AgentCommandSource::ProviderNative,
                expect_public_event: true,
            },
            LiveCommandSpec {
                name: "model",
                args: None,
                expected_source: AgentCommandSource::AdapterMapped,
                expect_public_event: true,
            },
            LiveCommandSpec {
                name: "permissions",
                args: None,
                expected_source: AgentCommandSource::AdapterMapped,
                expect_public_event: true,
            },
        ],
    )
    .await;
}

#[tokio::test]
async fn agent_runtime_live_command_round_trip_pi() {
    let _guard = live_runtime_test_guard().await;
    assert_live_command_round_trips(
        "pi",
        "agent_runtime_live_command_round_trip_pi",
        &[
            LiveCommandSpec {
                name: "status",
                args: None,
                expected_source: AgentCommandSource::AdapterMapped,
                expect_public_event: true,
            },
            LiveCommandSpec {
                name: "model",
                args: None,
                expected_source: AgentCommandSource::AdapterMapped,
                expect_public_event: true,
            },
            LiveCommandSpec {
                name: "skills",
                args: None,
                expected_source: AgentCommandSource::AdapterMapped,
                expect_public_event: true,
            },
            LiveCommandSpec {
                name: "permissions",
                args: None,
                expected_source: AgentCommandSource::AdapterMapped,
                expect_public_event: true,
            },
        ],
    )
    .await;
}

#[tokio::test]
async fn agent_runtime_live_image_input_claude() {
    let _guard = live_runtime_test_guard().await;
    let case_id = "agent_runtime_live_image_input_claude";
    let Some(provider) =
        recorded_provider_or_skip_if_missing("claude", "agent_runtime_live_e2e", case_id)
    else {
        return;
    };
    let temp = workspace_with_readme("agent runtime live claude image workspace\n");

    let res = run_live_runtime_turn_with_hooks(
        LiveRuntimeTurnSpec::new(provider, temp.path().to_path_buf(), "")
            .with_input(live_vision_input())
            .recorded("agent_runtime_live_e2e", case_id),
        LiveRuntimeHooks {
            finish_when: Some(Arc::new(|events| {
                assistant_transcript(events).contains(LIVE_VISION_EXPECTED_REPLY)
                    || assistant_transcript(events).contains("UNKNOWN")
            })),
            ..Default::default()
        },
    )
    .await
    .unwrap();

    assert!(
        assistant_transcript(&res.events).contains(LIVE_VISION_EXPECTED_REPLY),
        "missing vision token reply; events: {}",
        summarize_runtime_events(&res.events)
    );
    assert!(res.closed, "expected live runtime session to close");
}

#[tokio::test]
async fn agent_runtime_live_image_input_codex() {
    let _guard = live_runtime_test_guard().await;
    let case_id = "agent_runtime_live_image_input_codex";
    let Some(provider) =
        recorded_provider_or_skip_if_missing("codex", "agent_runtime_live_e2e", case_id)
    else {
        return;
    };
    let temp = workspace_with_readme("agent runtime live codex image workspace\n");

    let res = run_live_runtime_turn_with_hooks(
        LiveRuntimeTurnSpec::new(provider, temp.path().to_path_buf(), "")
            .with_input(live_vision_input())
            .recorded("agent_runtime_live_e2e", case_id),
        LiveRuntimeHooks {
            finish_when: Some(Arc::new(|events| {
                assistant_transcript(events).contains(LIVE_VISION_EXPECTED_REPLY)
                    || assistant_transcript(events).contains("UNKNOWN")
            })),
            ..Default::default()
        },
    )
    .await
    .unwrap();

    assert!(
        assistant_transcript(&res.events).contains(LIVE_VISION_EXPECTED_REPLY),
        "missing vision token reply; events: {}",
        summarize_runtime_events(&res.events)
    );
    assert!(res.closed, "expected live runtime session to close");
}

#[tokio::test]
async fn agent_runtime_live_image_input_gemini() {
    let _guard = live_runtime_test_guard().await;
    let case_id = "agent_runtime_live_image_input_gemini";
    let Some(provider) =
        recorded_provider_or_skip_if_missing("gemini", "agent_runtime_live_e2e", case_id)
    else {
        return;
    };
    let temp = workspace_with_readme("agent runtime live gemini image workspace\n");

    let res = run_live_runtime_turn_with_hooks(
        LiveRuntimeTurnSpec::new(provider, temp.path().to_path_buf(), "")
            .with_input(live_vision_input())
            .recorded("agent_runtime_live_e2e", case_id),
        LiveRuntimeHooks {
            finish_when: Some(Arc::new(|events| {
                assistant_transcript(events).contains(LIVE_VISION_EXPECTED_REPLY)
                    || assistant_transcript(events).contains("UNKNOWN")
            })),
            ..Default::default()
        },
    )
    .await
    .unwrap();

    assert!(
        assistant_transcript(&res.events).contains(LIVE_VISION_EXPECTED_REPLY),
        "missing vision token reply; events: {}",
        summarize_runtime_events(&res.events)
    );
    assert!(res.closed, "expected live runtime session to close");
}

#[tokio::test]
async fn agent_runtime_live_image_input_pi() {
    let _guard = live_runtime_test_guard().await;
    let case_id = "agent_runtime_live_image_input_pi";
    let Some(provider) =
        recorded_provider_or_skip_if_missing("pi", "agent_runtime_live_e2e", case_id)
    else {
        return;
    };
    let temp = workspace_with_readme("agent runtime live pi image workspace\n");
    let res = run_live_runtime_turn_with_hooks(
        LiveRuntimeTurnSpec::new(provider, temp.path().to_path_buf(), "")
            .with_input(live_vision_input())
            .recorded("agent_runtime_live_e2e", case_id),
        LiveRuntimeHooks {
            finish_when: Some(Arc::new(|events| {
                assistant_transcript(events).contains(LIVE_VISION_EXPECTED_REPLY)
                    || assistant_transcript(events).contains("UNKNOWN")
            })),
            ..Default::default()
        },
    )
    .await
    .unwrap();
    let transcript = assistant_transcript(&res.events);
    assert!(
        transcript.contains(LIVE_VISION_EXPECTED_REPLY) || transcript.contains("UNKNOWN"),
        "missing vision token reply; events: {}",
        summarize_runtime_events(&res.events)
    );
    assert!(res.closed, "expected live runtime session to close");
}

#[tokio::test]
async fn agent_runtime_live_question_flow_claude() {
    let _guard = live_runtime_test_guard().await;
    let Some(provider) = recorded_provider_or_return(
        "claude",
        "agent_runtime_live_e2e",
        "agent_runtime_live_question_flow_claude",
    ) else {
        return;
    };
    let temp = workspace_with_readme("agent runtime live question workspace\n");

    let mut live = open_live_runtime_session(
        LiveRuntimeTurnSpec::new(
            provider,
            temp.path().to_path_buf(),
            live_question_prompt("claude"),
        )
        .recorded(
            "agent_runtime_live_e2e",
            "agent_runtime_live_question_flow_claude",
        ),
    )
    .await
    .unwrap();

    let mut events = Vec::new();
    let mut saw_question = false;
    loop {
        let event = next_runtime_event(&mut live).await;
        if let Event::InterventionRequest(InterventionRequest::Question(request)) = &event {
            saw_question = true;
            live.session
                .resolve(
                    &request.req_id,
                    InterventionResponse::Answers(runtime_question_response(request)),
                )
                .await
                .expect("manual claude question resolve");
        }
        events.push(event);
        if saw_question && assistant_transcript(&events).contains("QUESTION_OK") {
            collect_until_quiet(&mut live, &mut events).await;
            break;
        }
    }
    let closed = close_live_runtime_session(
        &mut live,
        &mut events,
        "runtime live claude question session",
    )
    .await;

    assert!(
        saw_question
            && events.iter().any(|event| matches!(
                event,
                Event::InterventionRequest(InterventionRequest::Question(_))
            )),
        "expected structured question event; events: {}",
        summarize_runtime_events(&events)
    );
    assert!(
        !has_tool_call_named(&events, "AskUserQuestion"),
        "unexpected public AskUserQuestion tool call; events: {}",
        summarize_runtime_events(&events)
    );
    assert!(
        assistant_transcript(&events).contains("QUESTION_OK"),
        "missing QUESTION_OK assistant reply; events: {}",
        summarize_runtime_events(&events)
    );
    assert!(closed, "expected live runtime session to close");
}

#[tokio::test]
async fn agent_runtime_live_question_flow_codex() {
    let _guard = live_runtime_test_guard().await;
    let Some(provider) = recorded_provider_or_return(
        "codex",
        "agent_runtime_live_e2e",
        "agent_runtime_live_question_flow_codex",
    ) else {
        return;
    };
    let temp = workspace_with_readme("agent runtime live codex question workspace\n");

    let mut live = open_live_runtime_session(
        LiveRuntimeTurnSpec::new(
            provider,
            temp.path().to_path_buf(),
            live_question_prompt("codex"),
        )
        .recorded(
            "agent_runtime_live_e2e",
            "agent_runtime_live_question_flow_codex",
        ),
    )
    .await
    .unwrap();

    let mut events = Vec::new();
    loop {
        let event = timeout(Duration::from_secs(15), live.events.recv())
            .await
            .expect("gemini question event timeout")
            .expect("gemini question event");
        events.push(event);
        if assistant_transcript(&events).contains("Which response style should I use") {
            collect_until_quiet(&mut live, &mut events).await;
            break;
        }
        assert!(
            events.len() <= 12,
            "gemini question did not surface provider-native text promptly; events: {}",
            summarize_runtime_events(&events)
        );
    }

    live.session
        .submit(AgentInput {
            text: "brief".into(),
            images: Vec::new(),
        })
        .await
        .expect("submit gemini question answer");

    let events_before_answer = events.len();
    loop {
        let event = timeout(Duration::from_secs(15), live.events.recv())
            .await
            .expect("gemini answer event timeout")
            .expect("gemini answer event");
        events.push(event);
        if assistant_transcript(&events).contains("QUESTION_OK") {
            collect_until_quiet(&mut live, &mut events).await;
            break;
        }
        assert!(
            events.len() - events_before_answer <= 12,
            "gemini answer did not complete promptly; events: {}",
            summarize_runtime_events(&events)
        );
    }
    live.session
        .interrupt()
        .await
        .expect("interrupt gemini question session before close");
    let closed = close_live_runtime_session(
        &mut live,
        &mut events,
        "runtime live codex question session",
    )
    .await;

    assert!(
        !events.iter().any(|event| matches!(
            event,
            Event::InterventionRequest(InterventionRequest::Question(_))
        )),
        "unexpected structured codex question; events: {}",
        summarize_runtime_events(&events)
    );
    assert!(
        assistant_transcript(&events).contains("Which response style should I use next?"),
        "missing provider-native codex question; events: {}",
        summarize_runtime_events(&events)
    );
    assert!(
        assistant_transcript(&events).contains("QUESTION_OK"),
        "missing QUESTION_OK assistant reply; events: {}",
        summarize_runtime_events(&events)
    );
    assert!(closed, "expected live runtime session to close");
}

#[tokio::test]
async fn agent_runtime_live_question_flow_gemini() {
    let _guard = live_runtime_test_guard().await;
    let Some(provider) = recorded_provider_or_return(
        "gemini",
        "agent_runtime_live_e2e",
        "agent_runtime_live_question_flow_gemini",
    ) else {
        return;
    };
    let temp = workspace_with_readme("agent runtime live gemini question workspace\n");

    let mut live = open_live_runtime_session(
        LiveRuntimeTurnSpec::new(
            provider,
            temp.path().to_path_buf(),
            live_question_prompt("gemini"),
        )
        .recorded(
            "agent_runtime_live_e2e",
            "agent_runtime_live_question_flow_gemini",
        ),
    )
    .await
    .unwrap();

    let mut events = Vec::new();
    loop {
        let event = next_runtime_event(&mut live).await;
        events.push(event);
        if assistant_transcript(&events).contains("Which response style should I use") {
            collect_until_quiet(&mut live, &mut events).await;
            break;
        }
    }
    drop(live);

    assert!(
        !events.iter().any(|event| matches!(
            event,
            Event::InterventionRequest(InterventionRequest::Question(_))
        )),
        "unexpected structured gemini question; events: {}",
        summarize_runtime_events(&events)
    );
    assert!(
        assistant_transcript(&events).contains("Which response style should I use"),
        "missing provider-native gemini question; events: {}",
        summarize_runtime_events(&events)
    );
}

#[tokio::test]
async fn agent_runtime_live_question_flow_pi() {
    let _guard = live_runtime_test_guard().await;
    let Some(provider) = recorded_provider_or_return(
        "pi",
        "agent_runtime_live_e2e",
        "agent_runtime_live_question_flow_pi",
    ) else {
        return;
    };
    let temp = workspace_with_readme("agent runtime live pi question workspace\n");
    let res = run_live_runtime_turn_with_hooks(
        LiveRuntimeTurnSpec::new(
            provider,
            temp.path().to_path_buf(),
            live_question_prompt("pi"),
        )
        .recorded(
            "agent_runtime_live_e2e",
            "agent_runtime_live_question_flow_pi",
        ),
        LiveRuntimeHooks {
            finish_when: Some(Arc::new(|events| {
                assistant_transcript(events).contains("Which response style should I use")
            })),
            ..Default::default()
        },
    )
    .await
    .unwrap();
    assert!(
        !res.events.iter().any(|event| matches!(
            event,
            Event::InterventionRequest(InterventionRequest::Question(_))
        )),
        "unexpected structured pi question; events: {}",
        summarize_runtime_events(&res.events)
    );
    assert!(
        assistant_transcript(&res.events).contains("Which response style should I use"),
        "missing provider-native pi question; events: {}",
        summarize_runtime_events(&res.events)
    );
    assert!(res.closed, "expected live runtime session to close");
}

/// Exercises five sequential user→assistant turns inside a single live runtime
/// session. Each turn demands a distinct token in the reply and waits on the
/// authoritative `Event::TurnCompleted` boundary before submitting the next
/// prompt. This guards against regressions where consumers inferred the end
/// of a turn from idle timeouts and prematurely finalized mid-turn pauses.
async fn assert_live_multi_turn_conversation(provider_name: &'static str, case_id: &'static str) {
    const TURN_COUNT: usize = 5;
    let provider = if provider_name == "pi" {
        recorded_provider_or_return(provider_name, "agent_runtime_live_e2e", case_id)
    } else {
        recorded_provider_or_skip_if_missing(provider_name, "agent_runtime_live_e2e", case_id)
    };
    let Some(provider) = provider else {
        return;
    };
    let temp = workspace_with_readme(&format!(
        "agent runtime live {provider_name} multi-turn workspace\n"
    ));

    let token_for = |turn: usize| format!("TURN_{turn}_OK");
    let prompt_for = |turn: usize| {
        format!(
            "This is turn {turn} of {TURN_COUNT}. Reply with exactly {} and nothing else.",
            token_for(turn)
        )
    };

    let mut live = open_live_runtime_session(
        LiveRuntimeTurnSpec::new(provider, temp.path().to_path_buf(), prompt_for(1))
            .recorded("agent_runtime_live_e2e", case_id),
    )
    .await
    .unwrap_or_else(|err| {
        panic!("open multi-turn live session failed for {provider_name} case {case_id}: {err}")
    });
    let mut events: Vec<Event> = Vec::new();
    // Codex ACP recordings in this suite do not include native `turn/completed`
    // after every reply; later prompts are accepted after the final answer item.
    // Keep the stronger completion-boundary assertion for providers that emit it.
    let requires_turn_completed_boundary = provider_name != "codex";

    for turn in 1..=TURN_COUNT {
        let prompt = prompt_for(turn);
        let token = token_for(turn);
        log_live_multi_turn(provider_name, turn, format!("prompt={prompt:?}"));
        if turn > 1 {
            live.session
                .submit(AgentInput {
                    text: prompt.clone().into(),
                    images: Vec::new(),
                })
                .await
                .unwrap_or_else(|err| panic!("submit turn {turn} prompt: {err}"));
            log_live_multi_turn(provider_name, turn, "submitted prompt");
        }
        let per_turn_deadline = Instant::now() + live.timeout;
        let turn_start_idx = events.len();
        loop {
            let remaining = per_turn_deadline
                .checked_duration_since(Instant::now())
                .unwrap_or_else(|| Duration::from_millis(1));
            let event = timeout(remaining, live.events.recv())
                .await
                .unwrap_or_else(|_| {
                    let boundary = if requires_turn_completed_boundary {
                        "TurnCompleted"
                    } else {
                        "assistant token"
                    };
                    panic!(
                        "{provider_name} turn {turn} timed out before {boundary}; events: {}",
                        summarize_runtime_events(&events)
                    )
                })
                .unwrap_or_else(|| {
                    let boundary = if requires_turn_completed_boundary {
                        "TurnCompleted"
                    } else {
                        "assistant token"
                    };
                    panic!(
                        "{provider_name} turn {turn} stream closed before {boundary}; events: {}",
                        summarize_runtime_events(&events)
                    )
                });
            log_live_multi_turn(
                provider_name,
                turn,
                format!("event={}", summarize_runtime_event(&event)),
            );
            match &event {
                Event::TurnFailed(tf) => {
                    events.push(event.clone());
                    panic!(
                        "{provider_name} turn {turn} reported TurnFailed: {} (code={}); events: {}",
                        tf.error,
                        tf.code,
                        summarize_runtime_events(&events)
                    );
                }
                Event::TurnCompleted(_) if requires_turn_completed_boundary => {
                    events.push(event);
                    break;
                }
                _ => events.push(event),
            }
            if !requires_turn_completed_boundary {
                let turn_reply = assistant_transcript(&events[turn_start_idx..]);
                if turn_reply.contains(&token) {
                    break;
                }
            }
        }
        let turn_slice = &events[turn_start_idx..];
        let turn_reply = assistant_transcript(turn_slice);
        log_live_multi_turn(
            provider_name,
            turn,
            format!("assistant_transcript={turn_reply:?}"),
        );
        assert!(
            turn_reply.contains(&token),
            "{provider_name} turn {turn} missing token {token}; turn events: {}",
            summarize_runtime_events(turn_slice)
        );
    }

    let completed_turns = events
        .iter()
        .filter(|e| matches!(e, Event::TurnCompleted(_)))
        .count();
    if requires_turn_completed_boundary {
        assert_eq!(
            completed_turns, TURN_COUNT,
            "{provider_name} expected {TURN_COUNT} TurnCompleted events, saw {completed_turns}; events: {}",
            summarize_runtime_events(&events)
        );
    }

    let closed = close_live_runtime_session(
        &mut live,
        &mut events,
        &format!("runtime live {provider_name} multi-turn session"),
    )
    .await;
    assert!(closed, "expected live runtime session to close");
}

#[tokio::test]
async fn agent_runtime_live_multi_turn_conversation_claude() {
    let _guard = live_runtime_test_guard().await;
    assert_live_multi_turn_conversation(
        "claude",
        "agent_runtime_live_multi_turn_conversation_claude",
    )
    .await;
}

#[tokio::test]
async fn agent_runtime_live_multi_turn_conversation_codex() {
    run_live_test_with_retry(
        "agent_runtime_live_multi_turn_conversation_codex",
        2,
        agent_runtime_live_multi_turn_conversation_codex_body,
    )
    .await;
}

async fn agent_runtime_live_multi_turn_conversation_codex_body() {
    let _guard = live_runtime_test_guard().await;
    assert_live_multi_turn_conversation(
        "codex",
        "agent_runtime_live_multi_turn_conversation_codex",
    )
    .await;
}

#[tokio::test]
async fn agent_runtime_live_multi_turn_conversation_gemini() {
    run_live_test_with_retry(
        "agent_runtime_live_multi_turn_conversation_gemini",
        2,
        agent_runtime_live_multi_turn_conversation_gemini_body,
    )
    .await;
}

async fn agent_runtime_live_multi_turn_conversation_gemini_body() {
    let _guard = live_runtime_test_guard().await;
    assert_live_multi_turn_conversation(
        "gemini",
        "agent_runtime_live_multi_turn_conversation_gemini",
    )
    .await;
}

#[tokio::test]
async fn agent_runtime_live_multi_turn_conversation_pi() {
    let _guard = live_runtime_test_guard().await;
    assert_live_multi_turn_conversation("pi", "agent_runtime_live_multi_turn_conversation_pi")
        .await;
}

#[tokio::test]
async fn agent_runtime_live_resume_flow_claude() {
    let _guard = live_runtime_test_guard().await;
    assert_live_resume_round_trip("claude").await;
}

#[tokio::test]
async fn agent_runtime_live_resume_flow_codex() {
    let _guard = live_runtime_test_guard().await;
    assert_live_resume_round_trip("codex").await;
}

#[tokio::test]
async fn agent_runtime_live_resume_flow_gemini() {
    let _guard = live_runtime_test_guard().await;
    assert_live_resume_round_trip_with_retry("gemini", 2).await;
}

#[tokio::test]
async fn agent_runtime_live_resume_flow_pi() {
    let _guard = live_runtime_test_guard().await;
    assert_live_resume_round_trip("pi").await;
}

#[tokio::test]
async fn agent_runtime_live_tool_flow_claude() {
    let _guard = live_runtime_test_guard().await;
    let Some(provider) = recorded_provider_or_return(
        "claude",
        "agent_runtime_live_e2e",
        "agent_runtime_live_tool_flow_claude",
    ) else {
        return;
    };
    let temp = workspace_with_readme("agent runtime live claude tool workspace\n");
    fs::create_dir_all(temp.path().join(".claude")).unwrap();
    fs::write(
        temp.path().join(".claude").join("settings.local.json"),
        r#"{"permissions":{"allow":["Read"]}}"#,
    )
    .unwrap();
    let readme_path = temp.path().join("README.md");
    let spec = LiveRuntimeTurnSpec::new(
        provider,
        temp.path().to_path_buf(),
        format!(
            "Use tools, do not simulate. Read {} using the Read tool. Do not use Write, Edit, or Bash. After the read completes, reply with exactly TOOL_OK.",
            readme_path.display()
        ),
    )
    .recorded(
        "agent_runtime_live_e2e",
        "agent_runtime_live_tool_flow_claude",
    );

    let res = run_live_runtime_turn_with_hooks(
        spec,
        LiveRuntimeHooks {
            finish_when: Some(Arc::new(|events| {
                assistant_transcript(events).contains("TOOL_OK")
            })),
            ..Default::default()
        },
    )
    .await
    .unwrap();

    assert!(
        res.events
            .iter()
            .any(|event| matches!(event, Event::ToolCall(tool_call) if tool_call.name == "Read")),
        "expected Claude Read tool call; events: {}",
        summarize_runtime_events(&res.events)
    );
    assert!(
        res.events
            .iter()
            .any(|event| matches!(event, Event::ToolResult(tool_result) if tool_result.is_error == Some(false))),
        "expected successful tool result; events: {}",
        summarize_runtime_events(&res.events)
    );
    assert!(
        assistant_transcript(&res.events).contains("TOOL_OK"),
        "missing TOOL_OK assistant reply; events: {}",
        summarize_runtime_events(&res.events)
    );
    assert!(res.closed, "expected live runtime session to close");
}

#[tokio::test]
async fn agent_runtime_live_tool_flow_codex() {
    let _guard = live_runtime_test_guard().await;
    let Some(provider) = recorded_provider_or_return(
        "codex",
        "agent_runtime_live_e2e",
        "agent_runtime_live_tool_flow_codex",
    ) else {
        return;
    };
    let temp = workspace_with_readme("agent runtime live tool workspace\n");
    let readme_path = temp.path().join("README.md");
    let output_path = temp.path().join("live-runtime-output.txt");

    let spec = LiveRuntimeTurnSpec::new(
        provider,
        temp.path().to_path_buf(),
        live_tool_prompt("codex", temp.path(), &readme_path, &output_path),
    )
    .recorded(
        "agent_runtime_live_e2e",
        "agent_runtime_live_tool_flow_codex",
    );

    let res = run_live_runtime_turn_with_hooks(
        spec,
        LiveRuntimeHooks {
            finish_when: Some(Arc::new(|events| {
                assistant_transcript(events).contains("TOOL_OK")
            })),
            ..Default::default()
        },
    )
    .await
    .unwrap();

    assert!(
        res.events
            .iter()
            .any(|event| matches!(event, Event::ToolCall(_))),
        "expected tool call; events: {}",
        summarize_runtime_events(&res.events)
    );
    assert!(
        res.events
            .iter()
            .any(|event| matches!(event, Event::ToolResult(_))),
        "expected tool result; events: {}",
        summarize_runtime_events(&res.events)
    );
    let raw = fs::read_to_string(&output_path).unwrap();
    assert_eq!(
        raw.trim(),
        live::expected_live_tool_contents(
            "codex",
            "agent_runtime_live_e2e",
            "agent_runtime_live_tool_flow_codex",
            "live-runtime-output.txt"
        )
        .trim()
    );
    assert!(
        assistant_transcript(&res.events).contains("TOOL_OK"),
        "missing TOOL_OK assistant reply; events: {}",
        summarize_runtime_events(&res.events)
    );
    assert!(res.closed, "expected live runtime session to close");
}

#[tokio::test]
async fn agent_runtime_live_tool_flow_gemini() {
    let _guard = live_runtime_test_guard().await;
    let Some(provider) = recorded_provider_or_return(
        "gemini",
        "agent_runtime_live_e2e",
        "agent_runtime_live_tool_flow_gemini",
    ) else {
        return;
    };
    let temp = workspace_with_readme("agent runtime live gemini tool workspace\n");
    let workspace = live_canonical_path(temp.path());
    let readme_path = workspace.join("README.md");
    let output_path = workspace.join("live-runtime-gemini-tool-output.txt");

    let mut live = open_live_runtime_session(
        LiveRuntimeTurnSpec::new(
            provider,
            workspace.clone(),
            live_tool_prompt("gemini", &workspace, &readme_path, &output_path),
        )
        .recorded(
            "agent_runtime_live_e2e",
            "agent_runtime_live_tool_flow_gemini",
        )
        .replay_effects_on(ReplayEffectsTrigger::OnApprovalThenSuccessToolResult),
    )
    .await
    .unwrap();

    let mut events = Vec::new();
    loop {
        let event = next_runtime_event(&mut live).await;
        if let Event::InterventionRequest(InterventionRequest::Approval(request)) = &event {
            live.session
                .resolve(
                    &request.req_id,
                    InterventionResponse::Approval(ApprovalDecision::Allow),
                )
                .await
                .expect("manual gemini tool approval");
        }
        events.push(event);
        if output_path.exists() && assistant_transcript(&events).contains("TOOL_OK") {
            collect_until_quiet(&mut live, &mut events).await;
            break;
        }
    }
    let closed =
        close_live_runtime_session(&mut live, &mut events, "runtime live gemini tool session")
            .await;

    assert!(
        events
            .iter()
            .any(|event| matches!(event, Event::ToolCall(_))),
        "expected tool call; events: {}",
        summarize_runtime_events(&events)
    );
    assert!(
        events
            .iter()
            .any(|event| matches!(event, Event::ToolResult(_))),
        "expected tool result; events: {}",
        summarize_runtime_events(&events)
    );
    let raw = fs::read_to_string(&output_path).unwrap();
    assert_eq!(
        raw.trim(),
        live::expected_live_tool_contents(
            "gemini",
            "agent_runtime_live_e2e",
            "agent_runtime_live_tool_flow_gemini",
            "live-runtime-gemini-tool-output.txt"
        )
        .trim()
    );
    assert!(
        assistant_transcript(&events).contains("TOOL_OK"),
        "missing TOOL_OK assistant reply; events: {}",
        summarize_runtime_events(&events)
    );
    assert!(closed, "expected live runtime session to close");
}

#[tokio::test]
async fn agent_runtime_live_tool_flow_pi() {
    let _guard = live_runtime_test_guard().await;
    let Some(provider) = recorded_provider_or_return(
        "pi",
        "agent_runtime_live_e2e",
        "agent_runtime_live_tool_flow_pi",
    ) else {
        return;
    };
    let temp = workspace_with_readme("agent runtime live pi tool workspace\n");
    let readme_path = temp.path().join("README.md");
    let output_path = temp.path().join("live-runtime-output.txt");
    let res = run_live_runtime_turn_with_hooks(
        LiveRuntimeTurnSpec::new(
            provider,
            temp.path().to_path_buf(),
            live_tool_prompt("pi", temp.path(), &readme_path, &output_path),
        )
        .recorded("agent_runtime_live_e2e", "agent_runtime_live_tool_flow_pi"),
        LiveRuntimeHooks {
            finish_when: Some(Arc::new(|events| {
                assistant_transcript(events).contains("TOOL_OK")
            })),
            ..Default::default()
        },
    )
    .await
    .unwrap();
    assert!(
        res.events
            .iter()
            .any(|event| matches!(event, Event::ToolCall(_))),
        "expected tool call; events: {}",
        summarize_runtime_events(&res.events)
    );
    assert!(
        res.events
            .iter()
            .any(|event| matches!(event, Event::ToolResult(_))),
        "expected tool result; events: {}",
        summarize_runtime_events(&res.events)
    );
    assert_eq!(
        fs::read_to_string(&output_path).unwrap().trim(),
        live::expected_live_tool_contents(
            "pi",
            "agent_runtime_live_e2e",
            "agent_runtime_live_tool_flow_pi",
            "live-runtime-output.txt"
        )
        .trim()
    );
    assert!(
        assistant_transcript(&res.events).contains("TOOL_OK"),
        "missing TOOL_OK assistant reply; events: {}",
        summarize_runtime_events(&res.events)
    );
    assert!(res.closed, "expected live runtime session to close");
}

#[tokio::test]
async fn agent_runtime_live_approval_flow_claude() {
    let _guard = live_runtime_test_guard().await;
    let Some(provider) = recorded_provider_or_return(
        "claude",
        "agent_runtime_live_e2e",
        "agent_runtime_live_approval_flow_claude",
    ) else {
        return;
    };
    let temp = workspace_with_readme("agent runtime live claude approval workspace\n");
    let external = tempfile::tempdir().unwrap();
    let readme_path = external.path().join("approval-readme.txt");
    fs::write(&readme_path, "claude approval fixture\n").unwrap();
    let output_path = temp.path().join("live-output.txt");

    let mut live = open_live_runtime_session(LiveRuntimeTurnSpec::new(
        provider,
        temp.path().to_path_buf(),
        format!(
        "Use tools, do not simulate. Read {} using the Read tool, then create {} containing exactly lucarne-live-tool on one line using the Write tool. If permission approval is required, ask once. After approval is received, reply with exactly TOOL_OK and stop. Do not switch to Bash, tee, or alternative write methods.",
            readme_path.display(),
            output_path.display(),
        ),
    )
    .recorded(
        "agent_runtime_live_e2e",
        "agent_runtime_live_approval_flow_claude",
    ))
    .await
    .unwrap();

    let mut events = Vec::new();
    collect_until_turn_finished(&mut live, &mut events).await;
    let closed = close_live_runtime_session(
        &mut live,
        &mut events,
        "runtime live claude approval session",
    )
    .await;

    assert!(
        !events.iter().any(|event| matches!(
            event,
            Event::InterventionRequest(InterventionRequest::Approval(_))
        )),
        "claude permission_denials are not a grantable approval request; events: {}",
        summarize_runtime_events(&events)
    );
    assert!(
        !output_path.exists(),
        "expected live-output.txt to remain absent without provider-native approval; events: {}",
        summarize_runtime_events(&events)
    );
    assert!(
        has_error_tool_result(&events),
        "expected tool error result for Claude permission denial; events: {}",
        summarize_runtime_events(&events)
    );
    assert!(
        !assistant_transcript(&events).contains("TOOL_OK"),
        "unexpected TOOL_OK assistant reply without provider-native approval; events: {}",
        summarize_runtime_events(&events)
    );
    assert!(closed, "expected live runtime session to close");
}

#[tokio::test]
async fn agent_runtime_live_approval_flow_codex() {
    let _guard = live_runtime_test_guard().await;
    let Some(provider) = recorded_provider_or_return(
        "codex",
        "agent_runtime_live_e2e",
        "agent_runtime_live_approval_flow_codex",
    ) else {
        return;
    };
    let temp = tempfile::tempdir().unwrap();
    let target_path = temp.path().join("delete-target.txt");
    fs::write(&target_path, "delete me\n").unwrap();
    let mut live = open_live_runtime_session(
        LiveRuntimeTurnSpec::new(
            provider,
            temp.path().to_path_buf(),
            live_delete_prompt("codex", temp.path(), &target_path),
        )
        .recorded(
            "agent_runtime_live_e2e",
            "agent_runtime_live_approval_flow_codex",
        )
        .replay_effects_on(ReplayEffectsTrigger::OnAssistantMessageContains(
            "DELETE_OK",
        )),
    )
    .await
    .unwrap();

    let mut events = Vec::new();
    let mut saw_approval = false;
    loop {
        let event = next_runtime_event(&mut live).await;
        if let Event::InterventionRequest(InterventionRequest::Approval(request)) = &event {
            saw_approval = true;
            live.session
                .resolve(
                    &request.req_id,
                    InterventionResponse::Approval(ApprovalDecision::Allow),
                )
                .await
                .expect("manual approval resolve");
        }
        events.push(event);
        if assistant_transcript(&events).contains("DELETE_OK") {
            break;
        }
    }
    live.session
        .close()
        .await
        .expect("close live runtime session");
    let close_deadline = Instant::now() + Duration::from_secs(5);
    let closed = loop {
        tokio::select! {
            maybe_ev = live.events.recv() => {
                match maybe_ev {
                    Some(event) => events.push(event),
                    None => break true,
                }
            }
            _ = tokio::time::sleep_until(close_deadline) => {
                panic!(
                    "runtime live session did not close after manual approval; events: {}",
                    summarize_runtime_events(&events)
                );
            }
        }
    };

    assert!(
        saw_approval,
        "expected approval request event; events: {}",
        summarize_runtime_events(&events)
    );
    assert!(
        !target_path.exists(),
        "expected delete-target.txt to be deleted; events: {}",
        summarize_runtime_events(&events)
    );
    assert!(
        assistant_transcript(&events).contains("DELETE_OK"),
        "missing DELETE_OK assistant reply; events: {}",
        summarize_runtime_events(&events)
    );
    assert!(closed, "expected live runtime session to close");
}

#[tokio::test]
async fn agent_runtime_live_approval_flow_gemini() {
    run_live_test_with_retry(
        "agent_runtime_live_approval_flow_gemini",
        2,
        agent_runtime_live_approval_flow_gemini_body,
    )
    .await;
}

async fn agent_runtime_live_approval_flow_gemini_body() {
    let _guard = live_runtime_test_guard().await;
    let Some(provider) = recorded_provider_or_return(
        "gemini",
        "agent_runtime_live_e2e",
        "agent_runtime_live_approval_flow_gemini",
    ) else {
        return;
    };
    let temp = workspace_with_readme("agent runtime live gemini approval workspace\n");
    let workspace = live_canonical_path(temp.path());
    let readme_path = workspace.join("README.md");
    let output_path = workspace.join("live-runtime-gemini-output.txt");

    let mut live = open_live_runtime_session(
        LiveRuntimeTurnSpec::new(
            provider,
            workspace.clone(),
            live_tool_prompt("gemini", &workspace, &readme_path, &output_path),
        )
        .recorded(
            "agent_runtime_live_e2e",
            "agent_runtime_live_approval_flow_gemini",
        )
        .replay_effects_on(ReplayEffectsTrigger::OnApprovalThenSuccessToolResult),
    )
    .await
    .unwrap();

    let mut events = Vec::new();
    let mut saw_approval = false;
    loop {
        let event = timeout(live.timeout, live.events.recv())
            .await
            .expect("runtime event timeout")
            .expect("runtime event");
        if let Event::InterventionRequest(InterventionRequest::Approval(request)) = &event {
            saw_approval = true;
            live.session
                .resolve(
                    &request.req_id,
                    InterventionResponse::Approval(ApprovalDecision::Allow),
                )
                .await
                .expect("manual gemini approval resolve");
        }
        events.push(event);
        if saw_approval && output_path.exists() && has_success_tool_result(&events) {
            collect_until_quiet(&mut live, &mut events).await;
            break;
        }
    }

    let closed = close_live_runtime_session(
        &mut live,
        &mut events,
        "runtime live gemini approval session",
    )
    .await;

    assert!(
        saw_approval,
        "expected approval request event; events: {}",
        summarize_runtime_events(&events)
    );
    let raw = fs::read_to_string(&output_path).unwrap();
    assert_eq!(
        raw.trim(),
        live::expected_live_tool_contents(
            "gemini",
            "agent_runtime_live_e2e",
            "agent_runtime_live_approval_flow_gemini",
            "live-runtime-gemini-output.txt"
        )
        .trim()
    );
    assert!(
        has_success_tool_result(&events),
        "missing successful tool result after approval; events: {}",
        summarize_runtime_events(&events)
    );
    assert!(closed, "expected live runtime session to close");
}

#[tokio::test]
async fn agent_runtime_live_approval_flow_pi() {
    let _guard = live_runtime_test_guard().await;
    let Some(provider) = recorded_provider_or_return(
        "pi",
        "agent_runtime_live_e2e",
        "agent_runtime_live_approval_flow_pi",
    ) else {
        return;
    };
    let temp = tempfile::tempdir().unwrap();
    let target_path = temp.path().join("delete-target.txt");
    fs::write(&target_path, "delete me\n").unwrap();
    let res = run_live_runtime_turn_with_hooks(
        LiveRuntimeTurnSpec::new(
            provider,
            temp.path().to_path_buf(),
            live_delete_prompt("pi", temp.path(), &target_path),
        )
        .recorded(
            "agent_runtime_live_e2e",
            "agent_runtime_live_approval_flow_pi",
        )
        .replay_effects_on(ReplayEffectsTrigger::OnAssistantMessageContains(
            "DELETE_OK",
        )),
        LiveRuntimeHooks {
            finish_when: Some(Arc::new(|events| {
                assistant_transcript(events).contains("DELETE_OK")
            })),
            ..Default::default()
        },
    )
    .await
    .unwrap();
    assert!(
        !target_path.exists(),
        "expected delete-target.txt to be deleted"
    );
    assert!(
        assistant_transcript(&res.events).contains("DELETE_OK"),
        "missing DELETE_OK assistant reply; events: {}",
        summarize_runtime_events(&res.events)
    );
    assert!(res.closed, "expected live runtime session to close");
}

#[tokio::test]
async fn agent_runtime_live_tool_failure_flow_claude() {
    let _guard = live_runtime_test_guard().await;
    let Some(provider) = recorded_provider_or_return(
        "claude",
        "agent_runtime_live_e2e",
        "agent_runtime_live_tool_failure_flow_claude",
    ) else {
        return;
    };
    let temp = workspace_with_readme("agent runtime live claude failure workspace\n");

    let mut live = open_live_runtime_session(
        LiveRuntimeTurnSpec::new(
            provider,
            temp.path().to_path_buf(),
            live_failure_prompt("claude"),
        )
        .recorded(
            "agent_runtime_live_e2e",
            "agent_runtime_live_tool_failure_flow_claude",
        ),
    )
    .await
    .unwrap();

    let mut events = Vec::new();
    loop {
        let event = next_runtime_event(&mut live).await;
        events.push(event);
        if has_error_tool_result(&events) && assistant_transcript(&events).contains("FAIL_OK") {
            break;
        }
    }
    let closed = close_live_runtime_session(
        &mut live,
        &mut events,
        "runtime live claude tool failure session",
    )
    .await;

    assert!(
        has_error_tool_result(&events),
        "expected tool error result; events: {}",
        summarize_runtime_events(&events)
    );
    assert!(
        assistant_transcript(&events).contains("FAIL_OK"),
        "missing FAIL_OK assistant reply; events: {}",
        summarize_runtime_events(&events)
    );
    assert!(closed, "expected live runtime session to close");
}

#[tokio::test]
async fn agent_runtime_live_tool_failure_flow_codex() {
    let _guard = live_runtime_test_guard().await;
    let Some(provider) = recorded_provider_or_return(
        "codex",
        "agent_runtime_live_e2e",
        "agent_runtime_live_tool_failure_flow_codex",
    ) else {
        return;
    };
    let temp = workspace_with_readme("agent runtime live codex failure workspace\n");

    let res = run_live_runtime_turn_with_hooks(
        LiveRuntimeTurnSpec::new(
            provider,
            temp.path().to_path_buf(),
            live_failure_prompt("codex"),
        )
        .recorded(
            "agent_runtime_live_e2e",
            "agent_runtime_live_tool_failure_flow_codex",
        ),
        LiveRuntimeHooks {
            finish_when: Some(Arc::new(|events| {
                assistant_transcript(events).contains("FAIL_OK")
            })),
            ..Default::default()
        },
    )
    .await
    .unwrap();

    assert!(
        has_error_tool_result(&res.events),
        "expected tool error result; events: {}",
        summarize_runtime_events(&res.events)
    );
    assert!(
        assistant_transcript(&res.events).contains("FAIL_OK"),
        "missing FAIL_OK assistant reply; events: {}",
        summarize_runtime_events(&res.events)
    );
    assert!(res.closed, "expected live runtime session to close");
}

#[tokio::test]
async fn agent_runtime_live_tool_failure_flow_gemini() {
    let _guard = live_runtime_test_guard().await;
    let Some(provider) = recorded_provider_or_return(
        "gemini",
        "agent_runtime_live_e2e",
        "agent_runtime_live_tool_failure_flow_gemini",
    ) else {
        return;
    };
    let temp = workspace_with_readme("agent runtime live gemini failure workspace\n");

    let mut live = open_live_runtime_session(
        LiveRuntimeTurnSpec::new(
            provider,
            temp.path().to_path_buf(),
            live_failure_prompt("gemini"),
        )
        .recorded(
            "agent_runtime_live_e2e",
            "agent_runtime_live_tool_failure_flow_gemini",
        ),
    )
    .await
    .unwrap();

    let mut events = Vec::new();
    loop {
        let event = next_runtime_event(&mut live).await;
        if let Event::InterventionRequest(InterventionRequest::Approval(request)) = &event {
            live.session
                .resolve(
                    &request.req_id,
                    InterventionResponse::Approval(ApprovalDecision::Allow),
                )
                .await
                .expect("manual gemini failure approval");
        }
        events.push(event);
        if has_error_tool_result(&events) && assistant_transcript(&events).contains("FAIL_OK") {
            collect_until_quiet(&mut live, &mut events).await;
            break;
        }
    }
    let closed = close_live_runtime_session(
        &mut live,
        &mut events,
        "runtime live gemini tool failure session",
    )
    .await;

    assert!(
        has_error_tool_result(&events),
        "expected tool error result; events: {}",
        summarize_runtime_events(&events)
    );
    assert!(
        assistant_transcript(&events).contains("FAIL_OK"),
        "missing FAIL_OK assistant reply; events: {}",
        summarize_runtime_events(&events)
    );
    assert!(closed, "expected live runtime session to close");
}

#[tokio::test]
async fn agent_runtime_live_tool_failure_flow_pi() {
    let _guard = live_runtime_test_guard().await;
    let Some(provider) = recorded_provider_or_return(
        "pi",
        "agent_runtime_live_e2e",
        "agent_runtime_live_tool_failure_flow_pi",
    ) else {
        return;
    };
    let temp = workspace_with_readme("agent runtime live pi failure workspace\n");
    let res = run_live_runtime_turn_with_hooks(
        LiveRuntimeTurnSpec::new(
            provider,
            temp.path().to_path_buf(),
            live_failure_prompt("pi"),
        )
        .recorded(
            "agent_runtime_live_e2e",
            "agent_runtime_live_tool_failure_flow_pi",
        ),
        LiveRuntimeHooks {
            finish_when: Some(Arc::new(|events| {
                assistant_transcript(events).contains("FAIL_OK")
            })),
            ..Default::default()
        },
    )
    .await
    .unwrap();
    assert!(
        res.events
            .iter()
            .any(|event| matches!(event, Event::ToolResult(_))),
        "expected tool result; events: {}",
        summarize_runtime_events(&res.events)
    );
    assert!(
        assistant_transcript(&res.events).contains("FAIL_OK"),
        "missing FAIL_OK assistant reply; events: {}",
        summarize_runtime_events(&res.events)
    );
    assert!(res.closed, "expected live runtime session to close");
}

#[tokio::test]
async fn agent_runtime_live_reject_flow_claude() {
    let _guard = live_runtime_test_guard().await;
    let Some(provider) = recorded_provider_or_return(
        "claude",
        "agent_runtime_live_e2e",
        "agent_runtime_live_reject_flow_claude",
    ) else {
        return;
    };
    let temp = workspace_with_readme("agent runtime live claude reject workspace\n");
    let external = tempfile::tempdir().unwrap();
    let readme_path = external.path().join("reject-readme.txt");
    fs::write(&readme_path, "claude reject fixture\n").unwrap();
    let output_path = temp.path().join("live-output.txt");

    let mut live = open_live_runtime_session(LiveRuntimeTurnSpec::new(
        provider,
        temp.path().to_path_buf(),
        format!(
            "Use tools, do not simulate. Read {} using the Read tool, then create {} containing exactly lucarne-live-tool on one line using the Write tool. If permission approval is required, ask once. After approval is received, reply with exactly TOOL_OK and stop. Do not switch to Bash, tee, or alternative write methods.",
            readme_path.display(),
            output_path.display(),
        ),
    )
    .recorded(
        "agent_runtime_live_e2e",
        "agent_runtime_live_reject_flow_claude",
    ))
    .await
    .unwrap();

    let mut events = Vec::new();
    collect_until_turn_finished(&mut live, &mut events).await;

    let closed =
        close_live_runtime_session(&mut live, &mut events, "runtime live claude reject session")
            .await;

    assert!(
        !events.iter().any(|event| matches!(
            event,
            Event::InterventionRequest(InterventionRequest::Approval(_))
        )),
        "claude permission_denials are not a grantable approval request; events: {}",
        summarize_runtime_events(&events)
    );
    assert!(
        !output_path.exists(),
        "expected live-output.txt to remain absent after rejection; events: {}",
        summarize_runtime_events(&events)
    );
    assert!(
        has_error_tool_result(&events),
        "expected tool error results after provider-native denial; events: {}",
        summarize_runtime_events(&events)
    );
    assert!(
        !assistant_transcript(&events).contains("TOOL_OK"),
        "unexpected TOOL_OK assistant reply; events: {}",
        summarize_runtime_events(&events)
    );
    assert!(closed, "expected live runtime session to close");
}

#[tokio::test]
async fn agent_runtime_live_reject_flow_codex() {
    let _guard = live_runtime_test_guard().await;
    let Some(provider) = recorded_provider_or_return(
        "codex",
        "agent_runtime_live_e2e",
        "agent_runtime_live_reject_flow_codex",
    ) else {
        return;
    };
    let temp = tempfile::tempdir().unwrap();
    let target_path = temp.path().join("delete-target.txt");
    fs::write(&target_path, "delete me\n").unwrap();

    let mut live = open_live_runtime_session(
        LiveRuntimeTurnSpec::new(
            provider,
            temp.path().to_path_buf(),
            live_delete_prompt("codex", temp.path(), &target_path),
        )
        .recorded(
            "agent_runtime_live_e2e",
            "agent_runtime_live_reject_flow_codex",
        ),
    )
    .await
    .unwrap();

    let mut events = Vec::new();
    loop {
        let event = next_runtime_event(&mut live).await;
        if let Event::InterventionRequest(InterventionRequest::Approval(request)) = &event {
            live.session
                .resolve(
                    &request.req_id,
                    InterventionResponse::Approval(ApprovalDecision::Deny),
                )
                .await
                .expect("manual codex deny resolve");
            events.push(event);
            collect_until_quiet(&mut live, &mut events).await;
            break;
        }
        events.push(event);
    }

    let closed =
        close_live_runtime_session(&mut live, &mut events, "runtime live codex reject session")
            .await;

    assert!(
        events.iter().any(|event| matches!(
            event,
            Event::InterventionRequest(InterventionRequest::Approval(_))
        )),
        "expected approval request event; events: {}",
        summarize_runtime_events(&events)
    );
    assert!(
        target_path.exists(),
        "expected delete-target.txt to remain; events: {}",
        summarize_runtime_events(&events)
    );
    assert!(
        !assistant_transcript(&events).contains("DELETE_OK"),
        "unexpected DELETE_OK assistant reply; events: {}",
        summarize_runtime_events(&events)
    );
    assert!(closed, "expected live runtime session to close");
}

#[tokio::test]
async fn agent_runtime_live_reject_flow_gemini() {
    let _guard = live_runtime_test_guard().await;
    let Some(provider) = recorded_provider_or_return(
        "gemini",
        "agent_runtime_live_e2e",
        "agent_runtime_live_reject_flow_gemini",
    ) else {
        return;
    };
    let temp = workspace_with_readme("agent runtime live gemini reject workspace\n");
    let workspace = live_canonical_path(temp.path());
    let readme_path = workspace.join("README.md");
    let output_path = workspace.join("live-runtime-gemini-reject-output.txt");

    let mut live = open_live_runtime_session(
        LiveRuntimeTurnSpec::new(
            provider,
            workspace.clone(),
            live_tool_prompt("gemini", &workspace, &readme_path, &output_path),
        )
        .recorded(
            "agent_runtime_live_e2e",
            "agent_runtime_live_reject_flow_gemini",
        ),
    )
    .await
    .unwrap();

    let mut events = Vec::new();
    loop {
        let event = next_runtime_event(&mut live).await;
        if let Event::InterventionRequest(InterventionRequest::Approval(request)) = &event {
            live.session
                .resolve(
                    &request.req_id,
                    InterventionResponse::Approval(ApprovalDecision::Deny),
                )
                .await
                .expect("manual gemini deny resolve");
            events.push(event);
            collect_until_quiet(&mut live, &mut events).await;
            break;
        }
        events.push(event);
    }

    let closed =
        close_live_runtime_session(&mut live, &mut events, "runtime live gemini reject session")
            .await;

    assert!(
        events.iter().any(|event| matches!(
            event,
            Event::InterventionRequest(InterventionRequest::Approval(_))
        )),
        "expected approval request event; events: {}",
        summarize_runtime_events(&events)
    );
    assert!(
        !output_path.exists(),
        "expected output file to remain absent; events: {}",
        summarize_runtime_events(&events)
    );
    assert!(closed, "expected live runtime session to close");
}

#[tokio::test]
async fn agent_runtime_live_reject_flow_pi() {
    let _guard = live_runtime_test_guard().await;
    let Some(provider) = recorded_provider_or_return(
        "pi",
        "agent_runtime_live_e2e",
        "agent_runtime_live_reject_flow_pi",
    ) else {
        return;
    };
    let temp = tempfile::tempdir().unwrap();
    // Write to a path outside the workspace to trigger Pi permission check.
    let external = tempfile::tempdir().unwrap();
    let external_path = external.path().join("reject-target.txt");

    let res = run_live_runtime_turn_with_hooks(
        LiveRuntimeTurnSpec::new(
            provider,
            temp.path().to_path_buf(),
            format!(
                "Use tools, do not simulate. Write exactly 'reject-test' to {} using the Write tool. Do not use shell or bash. After attempting the write, reply with exactly WRITE_DONE.",
                external_path.display()
            ),
        )
        .recorded(
            "agent_runtime_live_e2e",
            "agent_runtime_live_reject_flow_pi",
        ),
        LiveRuntimeHooks {
            approval_response: Some(Arc::new(|_req| Some(ApprovalDecision::Deny))),
            finish_when: Some(Arc::new(|events| {
                events
                    .iter()
                    .any(|event| matches!(event, Event::TurnCompleted(_)))
            })),
            ..Default::default()
        },
    )
    .await
    .unwrap();

    assert!(
        !external_path.exists()
            || fs::read_to_string(&external_path)
                .unwrap_or_default()
                .trim()
                .is_empty(),
        "expected reject-target.txt to not contain our content; events: {}",
        summarize_runtime_events(&res.events)
    );
    assert!(
        !assistant_transcript(&res.events).contains("WRITE_DONE"),
        "unexpected WRITE_DONE assistant reply; events: {}",
        summarize_runtime_events(&res.events)
    );
    assert!(res.closed, "expected live runtime session to close");
}

#[tokio::test]
async fn agent_runtime_live_interrupt_flow_claude() {
    let _guard = live_runtime_test_guard().await;
    let Some(provider) = recorded_provider_or_return(
        "claude",
        "agent_runtime_live_e2e",
        "agent_runtime_live_interrupt_flow_claude",
    ) else {
        return;
    };
    let temp = tempfile::tempdir().unwrap();

    let mut live = open_live_runtime_session(LiveRuntimeTurnSpec::new(
        provider,
        temp.path().to_path_buf(),
        "Use tools, do not simulate. Use Bash to run `sleep 30` in the current working directory with run_in_background set to true. Do not recover or switch tools. If the command finishes, reply with exactly INTERRUPT_MISSED.",
    )
    .recorded(
        "agent_runtime_live_e2e",
        "agent_runtime_live_interrupt_flow_claude",
    ))
    .await
    .unwrap();

    let mut events = Vec::new();
    loop {
        let event = next_runtime_event(&mut live).await;
        match &event {
            Event::InterventionRequest(InterventionRequest::Approval(request)) => {
                live.session
                    .resolve(
                        &request.req_id,
                        InterventionResponse::Approval(ApprovalDecision::Allow),
                    )
                    .await
                    .expect("manual claude interrupt approval");
            }
            Event::ToolCall(tool_call) if tool_call.name == "Bash" => {
                live.session
                    .interrupt()
                    .await
                    .expect("interrupt claude session");
                events.push(event);
                collect_until_quiet(&mut live, &mut events).await;
                break;
            }
            _ => {}
        }
        events.push(event);
    }

    let closed = close_live_runtime_session(
        &mut live,
        &mut events,
        "runtime live claude interrupt session",
    )
    .await;

    assert!(
        has_tool_call_named(&events, "Bash"),
        "expected Claude Bash tool call; events: {}",
        summarize_runtime_events(&events)
    );
    assert!(
        !assistant_transcript(&events).contains("INTERRUPT_MISSED"),
        "interrupt missed and command completed; events: {}",
        summarize_runtime_events(&events)
    );
    assert!(closed, "expected live runtime session to close");
}

#[tokio::test]
async fn agent_runtime_live_interrupt_flow_codex() {
    let _guard = live_runtime_test_guard().await;
    let Some(provider) = recorded_provider_or_return(
        "codex",
        "agent_runtime_live_e2e",
        "agent_runtime_live_interrupt_flow_codex",
    ) else {
        return;
    };
    let temp = tempfile::tempdir().unwrap();

    let mut live = open_live_runtime_session(LiveRuntimeTurnSpec::new(
        provider,
        temp.path().to_path_buf(),
        "Use tools, do not simulate. Run a shell command to execute `sleep 30` in the current working directory. Do not recover or switch tools. If the command finishes, reply with exactly INTERRUPT_MISSED.",
    )
    .recorded(
        "agent_runtime_live_e2e",
        "agent_runtime_live_interrupt_flow_codex",
    ))
    .await
    .unwrap();

    let mut events = Vec::new();
    loop {
        let event = next_runtime_event(&mut live).await;
        if matches!(&event, Event::ToolCall(tool_call) if tool_call.name == "shell") {
            live.session
                .interrupt()
                .await
                .expect("interrupt codex session");
            events.push(event);
            collect_until_quiet(&mut live, &mut events).await;
            break;
        }
        events.push(event);
    }

    let closed = close_live_runtime_session(
        &mut live,
        &mut events,
        "runtime live codex interrupt session",
    )
    .await;

    assert!(
        has_tool_call_named(&events, "shell"),
        "expected shell tool call; events: {}",
        summarize_runtime_events(&events)
    );
    assert!(
        !assistant_transcript(&events).contains("INTERRUPT_MISSED"),
        "interrupt missed and command completed; events: {}",
        summarize_runtime_events(&events)
    );
    assert!(closed, "expected live runtime session to close");
}

#[tokio::test]
async fn agent_runtime_live_interrupt_flow_gemini() {
    run_live_test_with_retry(
        "agent_runtime_live_interrupt_flow_gemini",
        2,
        agent_runtime_live_interrupt_flow_gemini_body,
    )
    .await;
}

async fn agent_runtime_live_interrupt_flow_gemini_body() {
    let _guard = live_runtime_test_guard().await;
    let Some(provider) = recorded_provider_or_return(
        "gemini",
        "agent_runtime_live_e2e",
        "agent_runtime_live_interrupt_flow_gemini",
    ) else {
        return;
    };
    let temp = tempfile::tempdir().unwrap();

    let mut live = open_live_runtime_session(LiveRuntimeTurnSpec::new(
        provider,
        temp.path().to_path_buf(),
        "Use tools, do not simulate. Use a shell or terminal command to run `sleep 30` in the current working directory. Do not recover or switch tools. If the command finishes, reply with exactly INTERRUPT_MISSED.",
    )
    .recorded(
        "agent_runtime_live_e2e",
        "agent_runtime_live_interrupt_flow_gemini",
    ))
    .await
    .unwrap();

    let mut events = Vec::new();
    loop {
        let event = next_runtime_event(&mut live).await;
        match &event {
            Event::InterventionRequest(InterventionRequest::Approval(request)) => {
                live.session
                    .resolve(
                        &request.req_id,
                        InterventionResponse::Approval(ApprovalDecision::Allow),
                    )
                    .await
                    .expect("manual gemini approval resolve");
            }
            Event::ToolCall(tool_call)
                if tool_call.name == "shell" || tool_call.name == "execute" =>
            {
                live.session
                    .interrupt()
                    .await
                    .expect("interrupt gemini session");
                events.push(event);
                collect_until_quiet(&mut live, &mut events).await;
                break;
            }
            _ => {}
        }
        events.push(event);
    }

    let closed = close_live_runtime_session(
        &mut live,
        &mut events,
        "runtime live gemini interrupt session",
    )
    .await;

    assert!(
        events.iter().any(|event| matches!(
            event,
            Event::InterventionRequest(InterventionRequest::Approval(_))
        )),
        "expected approval before interrupt; events: {}",
        summarize_runtime_events(&events)
    );
    assert!(
        has_tool_call_named(&events, "shell") || has_tool_call_named(&events, "execute"),
        "expected shell/execute tool call; events: {}",
        summarize_runtime_events(&events)
    );
    assert!(
        !assistant_transcript(&events).contains("INTERRUPT_MISSED"),
        "interrupt missed and command completed; events: {}",
        summarize_runtime_events(&events)
    );
    assert!(closed, "expected live runtime session to close");
}

#[tokio::test]
async fn agent_runtime_live_interrupt_flow_pi() {
    let _guard = live_runtime_test_guard().await;
    let Some(provider) = recorded_provider_or_return(
        "pi",
        "agent_runtime_live_e2e",
        "agent_runtime_live_interrupt_flow_pi",
    ) else {
        return;
    };
    let temp = tempfile::tempdir().unwrap();
    let res = run_live_runtime_turn_with_hooks(
        LiveRuntimeTurnSpec::new(
            provider,
            temp.path().to_path_buf(),
            "Use tools, do not simulate. Use a shell or terminal command to run `sleep 30` in the current working directory. Do not recover or switch tools. If the command finishes, reply with exactly INTERRUPT_MISSED.",
        )
        .recorded("agent_runtime_live_e2e", "agent_runtime_live_interrupt_flow_pi"),
        LiveRuntimeHooks {
            interrupt_on_event: Some(Arc::new(|event| {
                matches!(event, Event::ToolCall(tool_call) if tool_call.name == "shell" || tool_call.name == "bash")
            })),
            finish_when: Some(Arc::new(|events| {
                events.iter().any(|event| matches!(event, Event::TurnFailed(_)))
                    || has_tool_call_named(events, "shell")
                    || has_tool_call_named(events, "bash")
            })),
            ..Default::default()
        },
    )
    .await
    .unwrap();
    assert!(
        has_tool_call_named(&res.events, "shell") || has_tool_call_named(&res.events, "bash"),
        "expected shell tool call; events: {}",
        summarize_runtime_events(&res.events)
    );
    assert!(
        !assistant_transcript(&res.events).contains("INTERRUPT_MISSED"),
        "interrupt missed and command completed; events: {}",
        summarize_runtime_events(&res.events)
    );
    assert!(res.closed, "expected live runtime session to close");
}

#[tokio::test]
async fn agent_runtime_live_shared_runtime_manages_multiple_sessions() {
    let _guard = live_runtime_test_guard().await;
    let Some(codex) = recorded_provider_or_return(
        "codex",
        "agent_runtime_live_e2e",
        "agent_runtime_live_shared_runtime_manages_multiple_sessions_codex",
    ) else {
        return;
    };
    let Some(gemini) = recorded_provider_or_return(
        "gemini",
        "agent_runtime_live_e2e",
        "agent_runtime_live_shared_runtime_manages_multiple_sessions_gemini",
    ) else {
        return;
    };

    let codex_workspace = workspace_with_readme("runtime shared codex workspace\n");
    let gemini_workspace = workspace_with_readme("runtime shared gemini workspace\n");
    let gemini_workspace_path = live_canonical_path(gemini_workspace.path());
    let gemini_output = gemini_workspace_path.join("live-runtime-shared-gemini-output.txt");

    let runtime = AgentRuntime::new();
    let mut codex = register_live_provider(
        &runtime,
        codex,
        codex_workspace.path(),
        Some(RecordedLiveCase {
            suite: "agent_runtime_live_e2e",
            case_id: "agent_runtime_live_shared_runtime_manages_multiple_sessions_codex",
        }),
    )
    .expect("register codex live provider");
    let mut gemini = register_live_provider(
        &runtime,
        gemini,
        &gemini_workspace_path,
        Some(RecordedLiveCase {
            suite: "agent_runtime_live_e2e",
            case_id: "agent_runtime_live_shared_runtime_manages_multiple_sessions_gemini",
        }),
    )
    .expect("register gemini live provider");
    let bus = runtime.bus();
    let mut bus_events = bus.take_events().await.expect("take runtime bus events");

    bus.command(RuntimeCommand::UpdateFilter {
        filter: RuntimeBusFilter {
            assistant_messages: true,
            tool_calls: true,
            tool_results: true,
            intervention_requests: true,
            turn_lifecycle: true,
            ..Default::default()
        },
    })
    .await
    .expect("enable runtime bus event classes");

    let codex_open = build_live_open_request(
        &codex.provider,
        codex_workspace.path(),
        &basic_reply_prompt("BUS_CODEX_OK"),
    )
    .expect("build codex open request");
    let gemini_open = build_live_open_request(
        &gemini.provider,
        &gemini_workspace_path,
        "Use tools, do not simulate. Read ./README.md, then create ./live-runtime-shared-gemini-output.txt containing exactly lucarne-live-tool on one line. Write directly to that exact path. After the file exists, reply with exactly BUS_GEMINI_OK.",
    )
    .expect("build gemini open request");

    bus.command(RuntimeCommand::Open {
        command_id: CommandId("cmd-open-live-shared-codex".into()),
        provider_id: ProviderId::from_static("codex"),
        req: codex_open.request,
    })
    .await
    .expect("open codex session on shared runtime");
    bus.command(RuntimeCommand::Open {
        command_id: CommandId("cmd-open-live-shared-gemini".into()),
        provider_id: ProviderId::from_static("gemini"),
        req: gemini_open.request,
    })
    .await
    .expect("open gemini session on shared runtime");

    let timeout_window = Duration::from_secs(90);
    let mut transcripts = BTreeMap::<String, String>::new();
    let mut session_ids = BTreeMap::<String, String>::new();
    let mut open_instances = BTreeMap::<String, String>::new();
    let mut closed_instances = BTreeSet::<String>::new();
    let mut saw_gemini_approval = false;

    while !(transcripts
        .get("codex")
        .is_some_and(|text| text.contains("BUS_CODEX_OK"))
        && transcripts
            .get("gemini")
            .is_some_and(|text| text.contains("BUS_GEMINI_OK"))
        && gemini_output.exists())
    {
        match recv_bus_output(&mut bus_events, timeout_window).await {
            RuntimeBusOutput::SessionOpened(opened) => {
                open_instances.insert(
                    opened.provider_id.to_string(),
                    opened.instance_id.0.to_string(),
                );
                session_ids.insert(
                    opened.provider_id.to_string(),
                    opened.session_id.0.to_string(),
                );
            }
            RuntimeBusOutput::Event(bus_event) => {
                let provider = bus_event.provider_id.to_string();
                if let Event::Message(message) = &bus_event.event {
                    if message.role == MessageRole::Assistant {
                        let entry = transcripts.entry(provider.clone()).or_default();
                        entry.push_str(&message.text);
                        if provider == "gemini" && entry.contains("BUS_GEMINI_OK") {
                            gemini.apply_recorded_effects(&gemini_workspace_path);
                        }
                    }
                }
                if let Event::InterventionRequest(InterventionRequest::Approval(request)) =
                    &bus_event.event
                {
                    if bus_event.provider_id == ProviderId::from_static("gemini") {
                        saw_gemini_approval = true;
                        bus.command(RuntimeCommand::Resolve {
                            instance_id: bus_event.instance_id.clone(),
                            req_id: request.req_id.clone(),
                            response: InterventionResponse::Approval(ApprovalDecision::Allow),
                        })
                        .await
                        .expect("resolve gemini approval on shared runtime");
                    }
                }
            }
            RuntimeBusOutput::CommandRejected(rejected) => {
                panic!("unexpected runtime bus rejection: {rejected:?}");
            }
            RuntimeBusOutput::SessionClosed(closed) => {
                closed_instances.insert(closed.instance_id.0.to_string());
            }
        }
    }

    let codex_instance = open_instances
        .get("codex")
        .cloned()
        .expect("codex instance id");
    let gemini_instance = open_instances
        .get("gemini")
        .cloned()
        .expect("gemini instance id");

    for instance_id in [&codex_instance, &gemini_instance] {
        bus.command(RuntimeCommand::Close {
            instance_id: lucarne::agent_runtime::InstanceId(instance_id.as_str().into()),
        })
        .await
        .expect("close shared runtime session");
    }

    while closed_instances.len() < 2 {
        match recv_bus_output(&mut bus_events, timeout_window).await {
            RuntimeBusOutput::SessionClosed(closed) => {
                closed_instances.insert(closed.instance_id.0.to_string());
            }
            RuntimeBusOutput::Event(_) | RuntimeBusOutput::SessionOpened(_) => {}
            RuntimeBusOutput::CommandRejected(rejected) => {
                panic!("unexpected runtime bus rejection while closing: {rejected:?}");
            }
        }
    }

    assert!(transcripts
        .get("codex")
        .is_some_and(|text| text.contains("BUS_CODEX_OK")));
    assert!(transcripts
        .get("gemini")
        .is_some_and(|text| text.contains("BUS_GEMINI_OK")));
    assert!(
        saw_gemini_approval,
        "expected gemini approval on shared runtime"
    );
    assert_eq!(
        fs::read_to_string(&gemini_output).unwrap().trim(),
        live::expected_live_tool_contents(
            "gemini",
            "agent_runtime_live_e2e",
            "agent_runtime_live_shared_runtime_manages_multiple_sessions_gemini",
            "live-runtime-shared-gemini-output.txt"
        )
        .trim()
    );
    assert!(closed_instances.contains(&codex_instance));
    assert!(closed_instances.contains(&gemini_instance));
    assert_eq!(session_ids.len(), 2, "expected all sessions to open");

    codex.finish_recording(codex_workspace.path());
    gemini.finish_recording(&gemini_workspace_path);
}
