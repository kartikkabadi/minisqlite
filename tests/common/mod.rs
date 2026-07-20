use std::fs;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};

static COUNTER: AtomicU64 = AtomicU64::new(0);

/// A temporary directory that is removed on drop.
pub struct TempDir {
    path: PathBuf,
}

impl TempDir {
    /// Create a unique temporary directory under the system temp directory.
    pub fn new() -> Self {
        let n = COUNTER.fetch_add(1, Ordering::SeqCst);
        let ns = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let path = std::env::temp_dir().join(format!(
            "minisqlite_test_{}_{}_{}",
            std::process::id(),
            n,
            ns
        ));
        fs::create_dir_all(&path).unwrap();
        Self { path }
    }

    pub fn path(&self) -> &Path {
        &self.path
    }
}

impl Drop for TempDir {
    fn drop(&mut self) {
        let _ = fs::remove_dir_all(&self.path);
    }
}
