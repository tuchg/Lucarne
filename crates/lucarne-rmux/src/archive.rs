//! Shared terminal-session archive store.
//!
//! Terminal archives are part of the rmux terminal capability. They are stored as
//! JSON records under `~/.lucarne/term-archive/<archive_id>.json`.

use std::fs;
use std::io::Write;
use std::path::PathBuf;

use serde::{Deserialize, Serialize};

#[cfg(test)]
static TEST_ARCHIVE_DIR: std::sync::Mutex<Option<PathBuf>> = std::sync::Mutex::new(None);

/// A full archived terminal record (with preserved content).
#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct ArchiveRecord {
    pub archive_id: String,
    pub session_id: String,
    pub title: String,
    pub cwd: Option<String>,
    pub archived_at: u64,
    pub content: String,
}

/// Archive metadata for listings (no content).
#[derive(Serialize, Clone, Debug)]
pub struct ArchiveMeta {
    pub archive_id: String,
    pub session_id: String,
    pub title: String,
    pub cwd: Option<String>,
    pub archived_at: u64,
}

fn dir() -> PathBuf {
    #[cfg(test)]
    if let Some(path) = TEST_ARCHIVE_DIR.lock().expect("archive test dir").clone() {
        return path;
    }

    dirs::home_dir()
        .unwrap_or_default()
        .join(".lucarne")
        .join("term-archive")
}

/// Persist an archive record; returns its `archive_id`.
pub fn save(
    session_id: &str,
    title: &str,
    cwd: Option<&str>,
    content: &str,
    archived_at: u64,
) -> std::io::Result<String> {
    let d = dir();
    create_owner_only_dir(&d)?;
    let archive_id = format!("{}-{}", session_id.replace([':', '/'], "_"), archived_at);
    let record = ArchiveRecord {
        archive_id: archive_id.clone(),
        session_id: session_id.to_string(),
        title: title.to_string(),
        cwd: cwd.map(str::to_string),
        archived_at,
        content: content.to_string(),
    };
    write_owner_only_file(
        &d.join(format!("{archive_id}.json")),
        &serde_json::to_vec(&record)?,
    )?;
    Ok(archive_id)
}

fn create_owner_only_dir(path: &std::path::Path) -> std::io::Result<()> {
    fs::create_dir_all(path)?;
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        fs::set_permissions(path, fs::Permissions::from_mode(0o700))?;
    }
    Ok(())
}

fn write_owner_only_file(path: &std::path::Path, bytes: &[u8]) -> std::io::Result<()> {
    let mut options = fs::OpenOptions::new();
    options.write(true).create(true).truncate(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;
        options.mode(0o600);
    }
    let mut file = options.open(path)?;
    file.write_all(bytes)?;
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        file.set_permissions(fs::Permissions::from_mode(0o600))?;
    }
    Ok(())
}

/// List archived sessions (newest first, without content).
pub fn list() -> Vec<ArchiveMeta> {
    let mut out = Vec::new();
    if let Ok(rd) = fs::read_dir(dir()) {
        for entry in rd.flatten() {
            if entry.path().extension().and_then(|x| x.to_str()) != Some("json") {
                continue;
            }
            let Ok(bytes) = fs::read(entry.path()) else {
                continue;
            };
            let Ok(rec) = serde_json::from_slice::<ArchiveRecord>(&bytes) else {
                continue;
            };
            out.push(ArchiveMeta {
                archive_id: rec.archive_id,
                session_id: rec.session_id,
                title: rec.title,
                cwd: rec.cwd,
                archived_at: rec.archived_at,
            });
        }
    }
    out.sort_by_key(|b| std::cmp::Reverse(b.archived_at));
    out
}

/// Read one archive record by id (rejects path-traversal ids).
pub fn get(archive_id: &str) -> Option<ArchiveRecord> {
    if archive_id.contains('/') || archive_id.contains("..") {
        return None;
    }
    let bytes = fs::read(dir().join(format!("{archive_id}.json"))).ok()?;
    serde_json::from_slice(&bytes).ok()
}

/// Current unix epoch seconds (archive timestamp helper).
pub fn now_epoch() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or(0)
}

#[cfg(test)]
mod tests {
    use std::fs;
    use std::sync::{Mutex, MutexGuard};

    use tempfile::TempDir;

    use super::{ArchiveRecord, TEST_ARCHIVE_DIR};

    static TEST_LOCK: Mutex<()> = Mutex::new(());

    struct ArchiveSandbox {
        _lock: MutexGuard<'static, ()>,
        _dir: TempDir,
    }

    impl ArchiveSandbox {
        fn new() -> Self {
            let lock = TEST_LOCK.lock().expect("archive test lock");
            let dir = tempfile::tempdir().expect("archive tempdir");
            *TEST_ARCHIVE_DIR.lock().expect("archive dir override") =
                Some(dir.path().to_path_buf());
            Self {
                _lock: lock,
                _dir: dir,
            }
        }

        fn path(&self) -> &std::path::Path {
            self._dir.path()
        }
    }

    impl Drop for ArchiveSandbox {
        fn drop(&mut self) {
            *TEST_ARCHIVE_DIR.lock().expect("archive dir override") = None;
        }
    }

    #[test]
    fn save_get_and_list_roundtrip_with_newest_first_metadata() {
        let sandbox = ArchiveSandbox::new();

        let older = super::save("work:0/0", "work", Some("/tmp/work"), "first", 10).unwrap();
        let newer = super::save("work:0/0", "work", None, "second", 20).unwrap();

        assert_eq!(older, "work_0_0-10");
        assert_eq!(newer, "work_0_0-20");
        assert!(sandbox.path().join("work_0_0-10.json").exists());

        let record = super::get(&older).expect("older archive");
        assert_eq!(record.session_id, "work:0/0");
        assert_eq!(record.title, "work");
        assert_eq!(record.cwd.as_deref(), Some("/tmp/work"));
        assert_eq!(record.content, "first");

        let list = super::list();
        assert_eq!(list.len(), 2);
        assert_eq!(list[0].archive_id, newer);
        assert_eq!(list[0].archived_at, 20);
        assert_eq!(list[1].archive_id, older);
        assert_eq!(list[1].archived_at, 10);
    }

    #[test]
    fn list_skips_bad_json_and_get_rejects_path_traversal() {
        let sandbox = ArchiveSandbox::new();

        let archive_id = super::save("safe", "safe", None, "content", 42).unwrap();
        fs::write(sandbox.path().join("bad.json"), b"not json").unwrap();
        fs::write(sandbox.path().join("ignore.txt"), b"ignored").unwrap();

        assert!(super::get("../safe").is_none());
        assert!(super::get("nested/safe").is_none());
        assert!(super::get(&archive_id).is_some());

        let list = super::list();
        assert_eq!(list.len(), 1);
        assert_eq!(list[0].archive_id, archive_id);
    }

    #[test]
    fn archive_record_schema_preserves_content_on_disk() {
        let sandbox = ArchiveSandbox::new();

        let archive_id = super::save("pane", "pane", None, "full scrollback", 7).unwrap();
        let bytes = fs::read(sandbox.path().join(format!("{archive_id}.json"))).unwrap();
        let record: ArchiveRecord = serde_json::from_slice(&bytes).unwrap();

        assert_eq!(record.archive_id, archive_id);
        assert_eq!(record.content, "full scrollback");
        assert!(super::now_epoch() > 0);
    }

    #[cfg(unix)]
    #[test]
    fn archive_dir_and_files_are_owner_only() {
        use std::os::unix::fs::PermissionsExt;

        let sandbox = ArchiveSandbox::new();
        let archive_id = super::save("pane", "pane", None, "secret scrollback", 9).unwrap();
        let file = sandbox.path().join(format!("{archive_id}.json"));

        let dir_mode = fs::metadata(sandbox.path()).unwrap().permissions().mode() & 0o777;
        let file_mode = fs::metadata(file).unwrap().permissions().mode() & 0o777;

        assert_eq!(dir_mode, 0o700);
        assert_eq!(file_mode, 0o600);
    }
}
