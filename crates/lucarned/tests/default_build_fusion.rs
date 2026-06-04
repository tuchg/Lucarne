//! Default-build capability-boundary guard.
//!
//! `lucarned` stays the single product entry, but terminal/rmux/remote/TUI
//! capabilities are explicit source features. Release packaging may opt into a
//! bundle; the default source build must keep the base daemon isolated from
//! remote/rmux/TUI drift.

use std::process::Command;

const CAPABILITY_CRATES: &[&str] = &[
    "ratatui",
    "crossterm",
    "lucarne-rmux",
    "lucarne-termgw",
    "lucarne-remote",
    "rmux-sdk",
];

fn tree_crate_names(output: &str) -> Vec<String> {
    output
        .lines()
        .filter_map(|line| {
            let trimmed = line.trim_start_matches(|c: char| {
                c.is_whitespace() || matches!(c, '|' | '`' | '+' | '-' | '├' | '│' | '└' | '─')
            });
            let name = trimmed.split_whitespace().next()?;
            (!name.is_empty()).then(|| name.to_string())
        })
        .collect()
}

#[test]
fn default_lucarned_build_keeps_terminal_gateway_tui_and_rmux_stack_out() {
    let output = Command::new("cargo")
        .args([
            "+nightly",
            "tree",
            "-Zbuild-dir-new-layout",
            "-p",
            "lucarned",
        ])
        .output()
        .expect("failed to run `cargo +nightly tree -Zbuild-dir-new-layout -p lucarned`");

    assert!(
        output.status.success(),
        "cargo tree failed:\nstdout:\n{}\nstderr:\n{}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr),
    );

    let stdout = String::from_utf8_lossy(&output.stdout);
    let names = tree_crate_names(&stdout);
    let present: Vec<&str> = CAPABILITY_CRATES
        .iter()
        .copied()
        .filter(|capability| names.iter().any(|name| name == capability))
        .collect();

    assert!(
        present.is_empty(),
        "default `lucarned` build must not link terminal/remote/TUI capability \
         crates; found {:?}. Full tree:\n{}",
        present,
        stdout,
    );
}

#[test]
fn product_terminal_bundle_contains_terminal_gateway_tui_and_rmux_stack() {
    let output = Command::new("cargo")
        .args([
            "+nightly",
            "tree",
            "-Zbuild-dir-new-layout",
            "-p",
            "lucarned",
            "--features",
            "product-terminal",
        ])
        .output()
        .expect("failed to run `cargo +nightly tree -Zbuild-dir-new-layout -p lucarned --features product-terminal`");

    assert!(
        output.status.success(),
        "cargo tree failed:\nstdout:\n{}\nstderr:\n{}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr),
    );

    let stdout = String::from_utf8_lossy(&output.stdout);
    let names = tree_crate_names(&stdout);
    let missing: Vec<&str> = CAPABILITY_CRATES
        .iter()
        .copied()
        .filter(|required| !names.iter().any(|name| name == required))
        .collect();

    assert!(
        missing.is_empty(),
        "`product-terminal` bundle must include explicit terminal/remote/TUI \
         capabilities; missing {:?}. Full tree:\n{}",
        missing,
        stdout,
    );
}

#[test]
fn individual_capability_features_compile_without_accidental_coupling() {
    for feature in ["remote-access", "terminal-rmux", "terminal-gateway", "tui"] {
        let output = Command::new("cargo")
            .args([
                "+nightly",
                "check",
                "-Zbuild-dir-new-layout",
                "-p",
                "lucarned",
                "--features",
                feature,
            ])
            .output()
            .unwrap_or_else(|err| panic!("failed to run cargo check for {feature}: {err}"));

        assert!(
            output.status.success(),
            "`lucarned` feature `{feature}` must compile independently:\nstdout:\n{}\nstderr:\n{}",
            String::from_utf8_lossy(&output.stdout),
            String::from_utf8_lossy(&output.stderr),
        );
    }
}

#[test]
fn tree_crate_names_parses_leftmost_name() {
    let sample = "\
lucarned v0.4.2 (/path)
├── lucarne-termgw v0.4.2 (/path)
│   └── crossterm v0.29.0 (*)
└── lucarne-rmux v0.4.2 (/path)
";
    let names = tree_crate_names(sample);
    assert!(names.contains(&"lucarned".to_string()));
    assert!(names.contains(&"lucarne-termgw".to_string()));
    assert!(names.contains(&"crossterm".to_string()));
    assert!(names.contains(&"lucarne-rmux".to_string()));
}
