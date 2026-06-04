//! Integration test harness — builds `lucarne-fakeagent` once (via cargo)
//! and drives a [`ProtocolAdapter`] against a fixture file under `tests/data/`.

pub mod agent_runtime;

use lucarne::adapter::{ProtocolAdapter, SessionParams};
use lucarne::dialect::Input;
use lucarne::event::{
    Decision, Event, Kind, Payload, PermissionResponse, Timeline, TimelineItem, TimelineType,
};
use lucarne::runtime::Session as RuntimeSession;
use once_cell::sync::OnceCell;
use std::{
    collections::BTreeMap, future::Future, path::PathBuf, pin::Pin, process::Command, sync::Arc,
    time::Duration,
};
use tokio::time::timeout;

pub fn repo_root() -> PathBuf {
    let manifest = env!("CARGO_MANIFEST_DIR");
    PathBuf::from(manifest)
        .parent()
        .unwrap()
        .parent()
        .unwrap()
        .to_path_buf()
}

pub fn fakeagent_bin() -> PathBuf {
    static BIN: OnceCell<PathBuf> = OnceCell::new();
    BIN.get_or_init(|| {
        let root = repo_root();
        let status = Command::new("cargo")
            .args(["build", "-p", "lucarne-fakeagent", "--quiet"])
            .current_dir(&root)
            .status()
            .expect("cargo build lucarne-fakeagent");
        assert!(status.success(), "build lucarne-fakeagent failed");
        let mut target = std::env::var_os("CARGO_TARGET_DIR")
            .map(PathBuf::from)
            .unwrap_or_else(|| {
                let mut p = root.clone();
                p.push("target");
                p
            });
        target.push("debug");
        target.push(if cfg!(windows) {
            "lucarne-fakeagent.exe"
        } else {
            "lucarne-fakeagent"
        });
        assert!(target.exists(), "fakeagent missing at {:?}", target);
        target
    })
    .clone()
}

pub fn fixture_path(subdir: &str, name: &str) -> PathBuf {
    let mut p = repo_root();
    p.push("tests");
    p.push("data");
    p.push(subdir);
    p.push(name);
    p
}

/// Write a `#!/bin/sh` wrapper into a temporary dir that simply
/// `cat`s `fixture` to stdout. Used as a stand-in binary for adapters
/// whose fixtures are raw JSONL (copilot, pi) rather than fakeagent
/// directive scripts.
pub fn write_cat_script(fixture: &std::path::Path) -> PathBuf {
    let dir = tempfile::tempdir().expect("tempdir");
    #[cfg(unix)]
    let path = dir.path().join("agent.sh");
    #[cfg(windows)]
    let path = dir.path().join("agent.cmd");
    #[cfg(unix)]
    let script = format!(
        "#!/bin/sh\nexec cat {}\n",
        shell_quote(&fixture.to_string_lossy())
    );
    #[cfg(windows)]
    let script = format!("@echo off\r\ntype \"{}\"\r\n", batch_quote(fixture));
    std::fs::write(&path, script).expect("write script");
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o755)).expect("chmod");
    }
    // Leak the tempdir — it only lives for the test process lifetime.
    std::mem::forget(dir);
    path
}

/// Write a script that handles `pi --list-models` and otherwise cats the fixture.
pub fn write_pi_cat_script(fixture: &std::path::Path) -> PathBuf {
    let dir = tempfile::tempdir().expect("tempdir");
    #[cfg(unix)]
    let path = dir.path().join("agent.sh");
    #[cfg(windows)]
    let path = dir.path().join("agent.cmd");
    #[cfg(unix)]
    let script = format!(
        r#"#!/bin/sh
if [ "$1" = "--list-models" ]; then
printf '%s\n' 'provider  model'
printf '%s\n' 'xai  grok-4'
exit 0
fi
exec cat {}
"#,
        shell_quote(&fixture.to_string_lossy())
    );
    #[cfg(windows)]
    let script = format!(
        "@echo off\r\nif \"%~1\"==\"--list-models\" (\r\n  echo provider  model\r\n  echo xai  grok-4\r\n  exit /b 0\r\n)\r\ntype \"{}\"\r\n",
        batch_quote(fixture)
    );
    std::fs::write(&path, script).expect("write script");
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o755)).expect("chmod");
    }
    std::mem::forget(dir);
    path
}

#[cfg(unix)]
fn shell_quote(s: &str) -> String {
    let mut out = String::with_capacity(s.len() + 2);
    out.push('\'');
    for c in s.chars() {
        if c == '\'' {
            out.push_str("'\\''");
        } else {
            out.push(c);
        }
    }
    out.push('\'');
    out
}

#[cfg(windows)]
fn batch_quote(path: &std::path::Path) -> String {
    path.to_string_lossy().replace('"', "\"\"")
}

pub struct ScenarioResult {
    pub events: Vec<Event>,
    pub closed: bool,
}

pub type PermissionHandler =
    Arc<dyn Fn(&lucarne::event::PermissionRequest) -> PermissionResponse + Send + Sync>;
pub type EventHandler = Arc<
    dyn Fn(Arc<RuntimeSession>, Event) -> Pin<Box<dyn Future<Output = Result<(), String>> + Send>>
        + Send
        + Sync,
>;

pub struct Scenario {
    pub adapter: Arc<ProtocolAdapter>,
    pub fixture: PathBuf,
    pub first_prompt: String,
    pub model: String,
    pub cwd: Option<PathBuf>,
    pub drive_send: bool,
    pub on_permission: Option<PermissionHandler>,
    pub on_event: Option<EventHandler>,
    pub extra_env: BTreeMap<String, String>,
    pub resume: Option<lucarne::event::ResumeHandle>,
    pub timeout: Duration,
}

impl Scenario {
    pub fn new(adapter: Arc<ProtocolAdapter>, fixture: PathBuf) -> Self {
        Self {
            adapter,
            fixture,
            first_prompt: "hello".into(),
            model: "test-model".into(),
            cwd: None,
            drive_send: false,
            on_permission: None,
            on_event: None,
            extra_env: BTreeMap::new(),
            resume: None,
            timeout: Duration::from_secs(10),
        }
    }
}

pub async fn run_scenario(mut sc: Scenario) -> ScenarioResult {
    sc.extra_env.insert(
        "LUCARNE_FIXTURE".into(),
        sc.fixture.to_string_lossy().into_owned(),
    );
    sc.extra_env
        .insert("LUCARNE_PI_SKIP_MODEL_PROBE".into(), "1".into());

    let cwd = sc.cwd.unwrap_or_else(repo_root);
    let sess_first_prompt = sc.first_prompt.clone();
    let req = SessionParams {
        model: sc.model,
        cwd: cwd.to_string_lossy().into_owned(),
        first_prompt: sc.first_prompt,
        extra_env: sc.extra_env,
        resume: sc.resume,
        ..Default::default()
    };

    let sess = Arc::new(sc.adapter.start(req).await.expect("adapter.start"));
    let mut rx = sess.events().await.expect("events");

    if sc.drive_send {
        let _ = sess
            .send(Input {
                text: sess_first_prompt.clone(),
                images: vec![],
            })
            .await;
    }

    let on_perm = sc.on_permission;
    let on_event = sc.on_event;

    let collector: tokio::task::JoinHandle<Result<(Vec<Event>, bool), String>> = {
        let sess = Arc::clone(&sess);
        tokio::spawn(async move {
            let mut events = Vec::new();
            let mut closed = false;
            while let Some(ev) = rx.recv().await {
                if let Payload::PermissionRequest(req) = &ev.payload {
                    let resp = match &on_perm {
                        Some(f) => f(req),
                        None => PermissionResponse::from_decision(Decision::Deny),
                    };
                    let _ = sess.resolve_with_response(&req.req_id, &resp).await;
                }
                if let Some(handler) = &on_event {
                    handler(Arc::clone(&sess), ev.clone()).await?;
                }
                if matches!(ev.payload, Payload::SessionClosed(_)) {
                    closed = true;
                }
                events.push(ev);
            }
            Ok((events, closed))
        })
    };

    let res = timeout(sc.timeout, collector).await;
    sess.close().await;

    match res {
        Ok(Ok(Ok((events, closed)))) => ScenarioResult { events, closed },
        Ok(Ok(Err(err))) => panic!("scenario collector error: {err}"),
        Ok(Err(err)) => panic!("scenario task failed: {err}"),
        Err(err) => panic!("scenario timeout after {}: {err}", sc.timeout.as_secs_f32()),
    }
}

pub fn kinds(evs: &[Event]) -> Vec<Kind> {
    evs.iter().map(|e| e.kind()).collect()
}

pub fn find_timeline(evs: &[Event], ty: TimelineType) -> Option<TimelineItem> {
    for e in evs {
        if let Payload::Timeline(Timeline { item }) = &e.payload {
            if item.ty == ty {
                return Some(item.clone());
            }
        }
    }
    None
}

pub fn collect_timelines(evs: &[Event], ty: TimelineType) -> Vec<TimelineItem> {
    evs.iter()
        .filter_map(|e| match &e.payload {
            Payload::Timeline(Timeline { item }) if item.ty == ty => Some(item.clone()),
            _ => None,
        })
        .collect()
}
