//! Boundary contract: `lucarne-rmux` is the terminal capability package.
//!
//! Terminal domain/wire types, archive helpers, and the live rmux-sdk binding
//! intentionally live together here. Preview-API churn stops at this crate, and
//! downstream gateway/daemon crates consume the stable `lucarne_rmux` surface
//! instead of naming `rmux_sdk` directly.

use std::process::Command;

#[test]
fn rmux_contract_owns_rmux_sdk_binding() {
    let stdout = cargo_tree(&["tree", "-p", "lucarne-rmux", "--no-dev"]);
    let stdout = if stdout.trim().is_empty() {
        cargo_tree(&["tree", "-p", "lucarne-rmux"])
    } else {
        stdout
    };
    assert!(
        stdout.contains("rmux-sdk"),
        "lucarne-rmux is the single layer that binds rmux_sdk — its dependency \
         graph must contain rmux-sdk:\n{stdout}"
    );
}

#[test]
fn gateway_and_daemon_do_not_name_rmux_sdk_directly() {
    let workspace = workspace_root();
    for manifest in [
        "crates/lucarne-termgw/Cargo.toml",
        "crates/lucarned/Cargo.toml",
    ] {
        let content = std::fs::read_to_string(workspace.join(manifest)).expect("read manifest");
        assert!(
            !content.contains("rmux-sdk"),
            "{manifest} must consume rmux-sdk through lucarne-rmux, not directly"
        );
    }
}

#[test]
fn rmux_contract_exports_terminal_and_archive_modules() {
    let root = std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    for relative in [
        "src/term/mod.rs",
        "src/term/grid.rs",
        "src/term/diff.rs",
        "src/term/input.rs",
        "src/term/registry.rs",
        "src/term/wire.rs",
        "src/archive.rs",
    ] {
        assert!(
            root.join(relative).exists(),
            "lucarne-rmux must own merged terminal/archive module {relative}"
        );
    }
}

fn cargo_tree(args: &[&str]) -> String {
    let output = Command::new("cargo")
        .args(args)
        .output()
        .expect("run cargo tree");
    String::from_utf8(output.stdout).unwrap()
}

fn workspace_root() -> std::path::PathBuf {
    std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .and_then(|p| p.parent())
        .expect("workspace root")
        .to_path_buf()
}
