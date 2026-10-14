use std::{
    fs,
    path::PathBuf,
    sync::atomic::{AtomicU64, Ordering},
};

static NEXT_ID: AtomicU64 = AtomicU64::new(0);

pub struct Workspace {
    pub dir: PathBuf,
    pub log: PathBuf,
}

impl Workspace {
    pub fn new() -> Self {
        let dir = std::env::temp_dir().join(format!(
            "appendlog-test-{}-{}",
            std::process::id(),
            NEXT_ID.fetch_add(1, Ordering::Relaxed)
        ));
        fs::create_dir(&dir).expect("create test directory");
        let log = dir.join("events.alog");
        Self { dir, log }
    }
}

impl Drop for Workspace {
    fn drop(&mut self) {
        let _ = fs::remove_dir_all(&self.dir);
    }
}
