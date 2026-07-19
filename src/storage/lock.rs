use std::fs::{File, OpenOptions};
use std::io;
use std::path::Path;

use fs2::FileExt;

use crate::Error;

/// Owns an advisory, exclusive lock for the lifetime of the process.
#[derive(Debug)]
pub struct Lock {
    #[allow(dead_code)]
    file: File,
}

impl Lock {
    /// Try to acquire an exclusive lock at `path`.
    pub fn acquire(path: impl AsRef<Path>) -> Result<Self, Error> {
        let path = path.as_ref().to_path_buf();
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent)?;
        }
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(false)
            .open(&path)?;
        match file.try_lock_exclusive() {
            Ok(()) => Ok(Self { file }),
            Err(e) => {
                // fs2 returns the platform's lock-contention error:
                // EWOULDBLOCK (11 on Linux, 35 on macOS) or ERROR_LOCK_VIOLATION (33 on Windows).
                if e.kind() == io::ErrorKind::WouldBlock
                    || e.raw_os_error()
                        .is_some_and(|c| c == 11 || c == 33 || c == 35)
                {
                    Err(Error::AlreadyOpen)
                } else {
                    Err(Error::Io(e.to_string()))
                }
            }
        }
    }
}

impl Drop for Lock {
    fn drop(&mut self) {
        let _ = self.file.unlock();
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn lock_is_exclusive() {
        let tmp = std::env::temp_dir().join(format!("minisqlite_lock_{}", std::process::id()));
        let _ = std::fs::remove_file(&tmp);
        let _lock = Lock::acquire(&tmp).unwrap();
        let second = Lock::acquire(&tmp);
        assert!(matches!(second, Err(Error::AlreadyOpen)));
        let _ = std::fs::remove_file(&tmp);
    }
}
