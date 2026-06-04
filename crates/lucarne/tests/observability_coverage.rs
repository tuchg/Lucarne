use std::{
    collections::BTreeSet,
    path::{Path, PathBuf},
    process::Command,
};

fn workspace_root() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .and_then(Path::parent)
        .expect("workspace root")
        .to_path_buf()
}

fn production_source(path: impl AsRef<Path>) -> String {
    let text = std::fs::read_to_string(path).expect("read source");
    let mut production = String::new();
    let mut lines = text.lines();
    while let Some(line) = lines.next() {
        if line.trim() != "#[cfg(test)]" {
            production.push_str(line);
            production.push('\n');
            continue;
        }

        let Some(first_item_line) = lines.next() else {
            break;
        };
        let trimmed = first_item_line.trim();
        if trimmed.starts_with("mod tests") {
            break;
        }
        if trimmed.starts_with("use ") || trimmed.ends_with(',') {
            continue;
        }

        let mut brace_depth = brace_delta(first_item_line);
        let mut saw_block = first_item_line.contains('{');
        if saw_block && brace_depth == 0 {
            continue;
        }
        for skipped in lines.by_ref() {
            brace_depth += brace_delta(skipped);
            saw_block |= skipped.contains('{');
            if saw_block && brace_depth == 0 {
                break;
            }
        }
    }
    production
}

fn brace_delta(line: &str) -> isize {
    line.chars().fold(0, |depth, ch| match ch {
        '{' => depth + 1,
        '}' => depth - 1,
        _ => depth,
    })
}

fn source_files(root: &Path, dir: &str) -> Vec<PathBuf> {
    let output = Command::new("rg")
        .args(["--files", dir, "-g", "*.rs"])
        .current_dir(root)
        .output()
        .expect("list rust source files");
    assert!(
        output.status.success(),
        "rg --files failed: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    String::from_utf8(output.stdout)
        .expect("utf-8 paths")
        .lines()
        .map(|line| root.join(line))
        .collect()
}

fn has_structured_tracing(source: &str) -> bool {
    source.contains("target: \"")
        || source.contains("#[instrument")
        || source.contains("use tracing")
}

#[test]
fn runtime_modules_have_structured_tracing_or_are_declared_pure() {
    let root = workspace_root();
    let pure_modules = [
        // agent-sessions: protocol/type definitions and deterministic projection
        // helpers. Parse/discovery and the generic projection boundary emit tracing.
        "agent-sessions/src/agent.rs",
        "agent-sessions/src/bash.rs",
        "agent-sessions/src/error.rs",
        "agent-sessions/src/input.rs",
        "agent-sessions/src/lib.rs",
        "agent-sessions/src/parse_selection.rs",
        "agent-sessions/src/paths.rs",
        "agent-sessions/src/reader.rs",
        "agent-sessions/src/providers/mod.rs",
        "agent-sessions/src/util.rs",
        "agent-sessions/src/watch/config.rs",
        "agent-sessions/src/watch/event.rs",
        "agent-sessions/src/watch/macos_fsevents.rs",
        "agent-sessions/src/watch/provider.rs",
        "agent-sessions/src/watch/raw.rs",
        "agent-sessions/src/watch/state.rs",
        "agent-sessions/src/watch/tests.rs",
        "agent-sessions/src/providers/claude/event.rs",
        "agent-sessions/src/providers/claude/types.rs",
        "agent-sessions/src/providers/codex/event.rs",
        "agent-sessions/src/providers/codex/types.rs",
        "agent-sessions/src/providers/copilot/event.rs",
        "agent-sessions/src/providers/copilot/mod.rs",
        "agent-sessions/src/providers/copilot/types.rs",
        "agent-sessions/src/providers/cursor/event.rs",
        "agent-sessions/src/providers/cursor/types.rs",
        "agent-sessions/src/providers/gemini/event.rs",
        "agent-sessions/src/providers/gemini/types.rs",
        "agent-sessions/src/providers/descriptor.rs",
        "agent-sessions/src/providers/pi/discovery.rs",
        "agent-sessions/src/providers/pi/event.rs",
        "agent-sessions/src/providers/pi/types.rs",
        "agent-sessions/src/agent_session/mod.rs",
        "agent-sessions/src/agent_session/projection.rs",
        // lucarne crate roots, open data types, static catalogs, and pure transforms.
        "crates/lucarne/src/lib.rs",
        "crates/lucarne/src/error.rs",
        "crates/lucarne/src/dialect.rs",
        "crates/lucarne/src/adapters/mod.rs",
        "crates/lucarne/src/adapters/version.rs",
        "crates/lucarne/src/agent_runtime/catalog.rs",
        "crates/lucarne/src/agent_runtime/error.rs",
        "crates/lucarne/src/agent_runtime/events.rs",
        "crates/lucarne/src/agent_runtime/mod.rs",
        "crates/lucarne/src/agent_runtime/project.rs",
        "crates/lucarne/src/agent_runtime/provider_args.rs",
        "crates/lucarne/src/agent_runtime/types.rs",
        "crates/lucarne/src/control_plane/activation.rs",
        "crates/lucarne/src/control_plane/commands.rs",
        "crates/lucarne/src/control_plane/fork.rs",
        "crates/lucarne/src/control_plane/history_replay.rs",
        "crates/lucarne/src/control_plane/interventions.rs",
        "crates/lucarne/src/control_plane/mod.rs",
        "crates/lucarne/src/control_plane/status.rs",
        "crates/lucarne/src/control_plane/subagents.rs",
        "crates/lucarne/src/control_plane/types.rs",
        "crates/lucarne/src/dialects/claude_transcript.rs",
        "crates/lucarne/src/dialects/mod.rs",
        "crates/lucarne/src/history/provider.rs",
        "crates/lucarne/src/history/render.rs",
        "crates/lucarne/src/history/transcript.rs",
        "crates/lucarne/src/host/mod.rs",
        "crates/lucarne/src/host/paths.rs",
        "crates/lucarne/src/host/process/mod.rs",
        "crates/lucarne/src/host/proxy_env.rs",
        "crates/lucarne/src/host/process_table/mod.rs",
        "crates/lucarne/src/host/file_users/mod.rs",
        "crates/lucarne/src/host/unix_tools.rs",
        "crates/lucarne/src/event/mod.rs",
        "crates/lucarne/src/event/command.rs",
        "crates/lucarne/src/event/core.rs",
        "crates/lucarne/src/event/permission.rs",
        "crates/lucarne/src/event/timeline.rs",
        "crates/lucarne/src/event/tool.rs",
        "crates/lucarne/src/event/usage.rs",
        "crates/lucarne/src/agent_registry.rs",
        "crates/lucarne/src/core_service/mod.rs",
        "crates/lucarne/src/core_service/types.rs",
        "crates/lucarne/src/core_service/api.rs",
        "crates/lucarne/src/observability.rs",
        "crates/lucarne/src/provider_id.rs",
        "crates/lucarne/src/time_display.rs",
        // Adapter/presentation crate roots and pure format conversion.
        "crates/lucarne-telegram/src/lib.rs",
        "crates/lucarne-telegram/src/agents.rs",
        "crates/lucarne-telegram/src/history.rs",
        "crates/lucarne-telegram/src/onboarding.rs",
        "crates/lucarne-channel/src/lib.rs",
        "crates/lucarne-channel/src/agent_message.rs",
        "crates/lucarne-channel/src/markdown.rs",
        "crates/lucarne-channel/src/types.rs",
        // testing/ modules moved from lucarne-test-support; test-only infrastructure
        "crates/lucarne/src/testing/mod.rs",
        "crates/lucarne/src/testing/live/mod.rs",
        "crates/lucarne/src/testing/live/common.rs",
        "crates/lucarne/src/testing/live/providers.rs",
        "crates/lucarne/src/testing/live/recording.rs",
        "crates/lucarne/src/testing/live/runtime.rs",
        "crates/lucarned/src/onboarding/config.rs",
        "crates/lucarned/src/onboarding/mod.rs",
        "crates/lucarned/src/onboarding/session.rs",
        "crates/lucarned/src/onboarding/terminal.rs",
        // `lucarned tui` dashboard: an interactive,
        // synchronous front-end that surfaces state through on-screen panels and
        // status lines, not a daemon-runtime path. Like the onboarding modules
        // above, it is observability-pure by design (no structured tracing).
        "crates/lucarned/src/tui/mod.rs",
        "crates/lucarned/src/tui/app.rs",
        "crates/lucarned/src/tui/event.rs",
        "crates/lucarned/src/tui/ui.rs",
        "crates/lucarned/src/tui/sessions.rs",
        "crates/lucarned/src/tui/remote.rs",
        "crates/lucarned/src/tui/config.rs",
        "crates/lucarned/src/tui/nav.rs",
    ]
    .into_iter()
    .collect::<BTreeSet<_>>();

    let mut missing = Vec::new();
    for dir in [
        "agent-sessions/src",
        "crates/lucarne/src",
        "crates/lucarne-adapter/src",
        "crates/lucarne-channel/src",
        "crates/lucarne-telegram/src",
        "crates/lucarned/src",
    ] {
        for path in source_files(&root, dir) {
            let rel = path.strip_prefix(&root).expect("relative path");
            let rel = rel.to_string_lossy().replace('\\', "/");
            if pure_modules.contains(rel.as_str()) {
                continue;
            }
            let source = production_source(&path);
            if !has_structured_tracing(&source) {
                missing.push(rel);
            }
        }
    }

    assert!(
        missing.is_empty(),
        "runtime modules must emit structured tracing or be explicitly declared pure:\n{}",
        missing.join("\n")
    );
}
