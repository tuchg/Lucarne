//! Terminal-adjacent journey coverage contract.
//!
//! This is not a substitute for provider/device live E2E. It prevents this
//! branch from claiming merge readiness with a checklist-only manifest: every
//! local/no-external-device part of the terminal journey must point at an
//! executable test or harness that is present in the repository.

use std::path::PathBuf;

#[derive(Clone, Copy, Debug)]
struct LocalJourneyEvidence {
    id: &'static str,
    source_file: &'static str,
    required_test_or_harness: &'static str,
    required_terms: &'static [&'static str],
}

const LOCAL_EVIDENCE: &[LocalJourneyEvidence] = &[
    LocalJourneyEvidence {
        id: "remote_quick_tunnel_harness",
        source_file: "../../scripts/remote-quick-tunnel-e2e.sh",
        required_test_or_harness: "LUCARNE_QUICK_TUNNEL_E2E",
        required_terms: &[
            "remote start",
            "/api/remote/status",
            "/api/sessions",
            "readonly",
            "cleanup",
        ],
    },
    LocalJourneyEvidence {
        id: "remote_readonly_agent_prompt_refusal",
        source_file: "../lucarne-termgw/src/lib.rs",
        required_test_or_harness: "agent_ws_readonly_prompt_refuses_before_terminal_inject",
        required_terms: &[
            "read-only session: prompts are not permitted",
            "monitor.injections().await.is_empty()",
        ],
    },
    LocalJourneyEvidence {
        id: "remote_readonly_terminal_write_refusal",
        source_file: "../lucarne-termgw/src/lib.rs",
        required_test_or_harness: "readonly_session_refuses_write_frames",
        required_terms: &["ClientFrame::Input", "readonly", "write"],
    },
    LocalJourneyEvidence {
        id: "terminal_transcript_bounded_initial_read",
        source_file: "src/terminal_agent_bind.rs",
        required_test_or_harness: "initial_read_uses_bounded_tail_window",
        required_terms: &[
            "INITIAL_TRANSCRIPT_WINDOW_BYTES",
            "INCREMENTAL_TRANSCRIPT_READ_BYTES",
        ],
    },
    LocalJourneyEvidence {
        id: "terminal_archive_owner_only_storage",
        source_file: "../lucarne-rmux/src/archive.rs",
        required_test_or_harness: "archive_dir_and_files_are_owner_only",
        required_terms: &["0o700", "0o600"],
    },
];

fn repo_path(relative: &str) -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR")).join(relative)
}

#[test]
fn local_terminal_journey_evidence_points_to_executable_checks() {
    for evidence in LOCAL_EVIDENCE {
        let path = repo_path(evidence.source_file);
        let source = std::fs::read_to_string(&path)
            .unwrap_or_else(|err| panic!("read {}: {err}", path.display()));
        assert!(
            source.contains(evidence.required_test_or_harness),
            "{} must point at an executable test/harness named `{}` in {}",
            evidence.id,
            evidence.required_test_or_harness,
            path.display()
        );
        for term in evidence.required_terms {
            assert!(
                source.contains(term),
                "{} evidence must include `{}` in {}",
                evidence.id,
                term,
                path.display()
            );
        }
    }
}

#[test]
fn terminal_history_and_agent_bootstrap_use_bounded_transcript_reads() {
    let source = std::fs::read_to_string(repo_path("src/terminal_agent_bind.rs"))
        .expect("read terminal_agent_bind source");
    assert!(
        source.contains("from == 0"),
        "initial/history reads must have explicit cursor bootstrap semantics"
    );
    assert!(
        source.contains("file.take(limit).read_to_end"),
        "transcript reads must use a bounded reader, not unbounded read_to_end"
    );
    assert!(
        !source.contains("file.read_to_end(&mut buf)"),
        "terminal transcript hot paths must not scan whole files"
    );
}

#[test]
fn terminal_archive_close_does_not_reach_core_live_sessions() {
    let termgw = std::fs::read_to_string(repo_path("../lucarne-termgw/src/lib.rs"))
        .expect("read termgw source");
    let archive_fn = termgw
        .split("async fn http_archive")
        .nth(1)
        .and_then(|tail| tail.split("async fn http_archives").next())
        .expect("http_archive source block");
    assert!(
        archive_fn.contains("archive::save"),
        "archive-and-close must persist terminal content"
    );
    assert!(
        archive_fn.contains("s.monitor.kill"),
        "archive-and-close must close the terminal/rmux session"
    );
    assert!(
        !archive_fn.contains("LucarneCore") && !archive_fn.contains("live_sessions"),
        "archive-and-close must not mutate LucarneCore live session state"
    );
}
