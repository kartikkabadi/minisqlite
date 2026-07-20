use std::fs::{File, OpenOptions};
use std::io::{Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};

use crate::codec::frame::{FileHeader, FILE_HEADER_SIZE};
use crate::config::Durability;
use crate::Error;

// Use an audited platform binding for `O_NOFOLLOW` instead of hand-copied constants.
#[cfg(unix)]
const O_NOFOLLOW: i32 = libc::O_NOFOLLOW;

/// Owns the primary data file and the normal append path.
///
/// The underlying `File` is locked exclusively with `try_lock` for the lifetime of this
/// handle, so a second `Store` (or `DataFile` with `acquire_lock` enabled) cannot open the
/// same primary path. This makes the single-owner invariant independent of any sidecar lock
/// path and protects against hard-link aliases that refer to the same inode.
#[derive(Debug)]
pub struct DataFile {
    file: File,
    pub durability: Durability,
    pub len: u64,
    header: FileHeader,
    path: PathBuf,
}

fn resolve_database_path(path: impl AsRef<Path>, create: bool) -> Result<PathBuf, Error> {
    let path = path.as_ref();
    let abs = if path.is_absolute() {
        path.to_path_buf()
    } else {
        std::env::current_dir()
            .map_err(|e| Error::Io(e.to_string()))?
            .join(path)
    };
    let file_name = abs
        .file_name()
        .ok_or_else(|| Error::Validation("database path must have a file name".into()))?;
    let parent = abs.parent().filter(|&p| !p.as_os_str().is_empty());
    let resolved_parent = match parent {
        None => PathBuf::from("/"),
        Some(p) if p.exists() => std::fs::canonicalize(p).map_err(|e| Error::Io(e.to_string()))?,
        Some(p) if create => {
            create_private_dirs(p)?;
            std::fs::canonicalize(p).map_err(|e| Error::Io(e.to_string()))?
        }
        Some(p) => {
            return Err(Error::Validation(format!(
                "database parent directory does not exist: {}",
                p.display()
            )))
        }
    };
    Ok(resolved_parent.join(file_name))
}

impl DataFile {
    /// Open or create the primary data file and validate its header.
    ///
    /// When `acquire_lock` is `true`, the file is locked before any content is read or
    /// written. Callers that only need a temporary working copy (e.g. deterministic tests or
    /// fuzz targets) can pass `false` to avoid exclusive-ownership semantics.
    pub fn open_or_create(
        path: impl AsRef<Path>,
        durability: Durability,
        acquire_lock: bool,
    ) -> Result<Self, Error> {
        Self::open(path, durability, acquire_lock, true, true)
    }

    /// Open an existing primary data file. Fails if the file does not exist.
    ///
    /// This is the path used by read-only and operational CLI commands so a missing source
    /// is reported as an error instead of silently creating an empty database.
    pub fn open_existing(
        path: impl AsRef<Path>,
        durability: Durability,
        acquire_lock: bool,
    ) -> Result<Self, Error> {
        Self::open(path, durability, acquire_lock, false, true)
    }

    /// Open an existing primary data file for read-only verification.
    ///
    /// No lock is acquired and the file is never modified, so this can be used while another
    /// process owns the store. The caller must ensure no concurrent writes are in flight if
    /// an exact point-in-time result is required.
    pub fn open_read_only(path: impl AsRef<Path>, durability: Durability) -> Result<Self, Error> {
        Self::open(path, durability, false, false, false)
    }

    fn open(
        path: impl AsRef<Path>,
        durability: Durability,
        acquire_lock: bool,
        create: bool,
        writable: bool,
    ) -> Result<Self, Error> {
        let path = resolve_database_path(path, create)?;

        let mut opts = OpenOptions::new();
        opts.read(true);
        if writable {
            opts.write(true);
        }
        if create {
            opts.create(true).truncate(false);
        }
        #[cfg(unix)]
        {
            use std::os::unix::fs::OpenOptionsExt;
            opts.mode(0o600).custom_flags(O_NOFOLLOW);
        }
        #[cfg(windows)]
        {
            use std::os::windows::fs::OpenOptionsExt;
            // FILE_FLAG_OPEN_REPARSE_POINT opens a reparse point (e.g. a symlink) itself,
            // so we can detect and reject a symlinked final component after the open.
            opts.custom_flags(0x00200000);
        }

        let mut file = match opts.open(&path) {
            Ok(f) => f,
            Err(e) if e.kind() == std::io::ErrorKind::NotFound && !create => {
                return Err(Error::Validation(format!(
                    "database file does not exist: {}",
                    path.display()
                )));
            }
            Err(e) => {
                if std::fs::symlink_metadata(&path)
                    .map(|m| m.file_type().is_symlink())
                    .unwrap_or(false)
                {
                    return Err(Error::Validation(format!(
                        "database path is a symlink: {}",
                        path.display()
                    )));
                }
                return Err(Error::Io(e.to_string()));
            }
        };

        // A post-open `is_symlink` check protects unknown Unix targets and Windows
        // (where we opened the reparse point itself).
        if file.metadata()?.file_type().is_symlink() {
            return Err(Error::Validation(format!(
                "database path is a symlink: {}",
                path.display()
            )));
        }

        if acquire_lock {
            match file.try_lock() {
                Ok(()) => {}
                Err(std::fs::TryLockError::WouldBlock) => {
                    return Err(Error::AlreadyOpen);
                }
                Err(std::fs::TryLockError::Error(e)) => {
                    return Err(Error::Io(e.to_string()));
                }
            }
        }

        let len = file.seek(SeekFrom::End(0))?;

        let (file_len, header) = if len == 0 {
            if !create {
                return Err(Error::NotMiniSQLite);
            }
            #[cfg(unix)]
            {
                use std::fs::{set_permissions, Permissions};
                use std::os::unix::fs::PermissionsExt;
                set_permissions(&path, Permissions::from_mode(0o600))?;
            }
            let now_ms = current_time_ms();
            let header = FileHeader::new(now_ms);
            let bytes = header.encode();
            file.write_all(&bytes)?;
            if durability.requires_sync() {
                file.sync_all()?;
                sync_ancestors(&path)?;
            }
            (FILE_HEADER_SIZE as u64, header)
        } else {
            if len < FILE_HEADER_SIZE as u64 {
                return Err(Error::Corruption {
                    message: "file shorter than header".into(),
                    offset: 0,
                });
            }
            file.seek(SeekFrom::Start(0))?;
            let mut header_bytes = [0u8; FILE_HEADER_SIZE];
            file.read_exact(&mut header_bytes)?;
            let header = FileHeader::decode(&header_bytes)?;
            (len, header)
        };

        Ok(Self {
            path,
            file,
            durability,
            len: file_len,
            header,
        })
    }

    pub fn file_len(&self) -> u64 {
        self.len
    }

    pub fn path(&self) -> &Path {
        &self.path
    }

    pub fn format_version(&self) -> (u16, u16) {
        (self.header.major, self.header.minor)
    }

    /// Read `n` bytes at `offset`.
    pub fn read_at(&mut self, offset: u64, n: usize) -> Result<Vec<u8>, Error> {
        #[cfg(feature = "failpoint")]
        if offset >= FILE_HEADER_SIZE as u64
            && std::env::var_os("MINISQLITE_FAILPOINT").as_deref()
                == Some(std::ffi::OsStr::new("header-read-error"))
        {
            return Err(Error::Io("simulated frame header read error".into()));
        }

        self.file.seek(SeekFrom::Start(offset))?;
        let mut buf = vec![0u8; n];
        self.file.read_exact(&mut buf)?;
        Ok(buf)
    }

    #[cfg(test)]
    pub fn read_all(&mut self) -> Result<Vec<u8>, Error> {
        self.file.seek(SeekFrom::Start(0))?;
        let mut buf = Vec::new();
        self.file.read_to_end(&mut buf)?;
        Ok(buf)
    }

    /// Append a complete frame to the file.
    ///
    /// If the `failpoint` feature is enabled and `MINISQLITE_FAILPOINT` is set,
    /// this method may write a partial frame and abort the process to simulate a crash.
    pub fn append_frame(&mut self, frame_bytes: &[u8], payload_length: u64) -> Result<(), Error> {
        let _ = payload_length;
        #[cfg(feature = "failpoint")]
        {
            use crate::codec::frame::FRAME_HEADER_SIZE;
            if let Some(fp) = std::env::var_os("MINISQLITE_FAILPOINT") {
                let fp = fp.to_string_lossy();
                let header_len = FRAME_HEADER_SIZE as u64;
                let payload_len = payload_length;
                match fp.as_ref() {
                    "before-append" => std::process::abort(),
                    "partial-header" => {
                        let split = (header_len / 2) as usize;
                        self.file.write_all(&frame_bytes[..split])?;
                        let _ = self.file.flush();
                        std::process::abort();
                    }
                    "during-payload" => {
                        let split = (header_len + payload_len / 2) as usize;
                        self.file
                            .write_all(&frame_bytes[..split.min(frame_bytes.len())])?;
                        let _ = self.file.flush();
                        std::process::abort();
                    }
                    "after-payload" => {
                        let split = (header_len + payload_len) as usize;
                        self.file
                            .write_all(&frame_bytes[..split.min(frame_bytes.len())])?;
                        let _ = self.file.flush();
                        std::process::abort();
                    }
                    "after-trailer" | "before-sync" => {
                        self.file.write_all(frame_bytes)?;
                        let _ = self.file.flush();
                        std::process::abort();
                    }
                    "after-sync" => {
                        self.file.write_all(frame_bytes)?;
                        self.file.flush()?;
                        self.file.sync_all()?;
                        std::process::abort();
                    }
                    "append-error" => {
                        let split = (frame_bytes.len() / 2).max(1);
                        self.file.write_all(&frame_bytes[..split])?;
                        return Err(Error::Io("simulated disk-full short write".into()));
                    }
                    "sync-error" => {
                        self.file.write_all(frame_bytes)?;
                        self.file.flush()?;
                        return Err(Error::Io("simulated sync failure".into()));
                    }
                    "rollback-error" => {
                        let split = (frame_bytes.len() / 2).max(1);
                        self.file.write_all(&frame_bytes[..split])?;
                        return Err(Error::Io("simulated short write".into()));
                    }
                    _ => {}
                }
            }
        }

        self.file.seek(SeekFrom::End(0))?;
        self.file.write_all(frame_bytes)?;
        if self.durability.requires_sync() {
            self.file.sync_all()?;
        } else {
            self.file.flush()?;
        }
        self.len += frame_bytes.len() as u64;
        Ok(())
    }

    pub fn sync(&mut self) -> Result<(), Error> {
        if self.durability.requires_sync() {
            self.file.sync_all()?;
        } else {
            self.file.flush()?;
        }
        Ok(())
    }

    /// Copy the first `valid_len` bytes of the locked primary file to `dest`.
    ///
    /// `valid_len` is the durable prefix of the file (typically `self.len` for a fully
    /// repaired store, or the last valid frame offset for a store opened with an un-repaired
    /// tail). The copy reads from the already-open `File` handle so it works on Windows even
    /// though the file is exclusively locked; `std::fs::copy` would fail with ERROR_LOCK_VIOLATION.
    pub fn copy_to(&mut self, dest: impl AsRef<Path>, valid_len: u64) -> std::io::Result<()> {
        self.file.flush()?;
        self.file.seek(SeekFrom::Start(0))?;
        let mut dest_file = OpenOptions::new().write(true).open(dest.as_ref())?;
        let mut limited = (&self.file).take(valid_len);
        let copied = std::io::copy(&mut limited, &mut dest_file)?;
        if copied != valid_len {
            return Err(std::io::Error::other(format!(
                "copy length mismatch: copied {copied} bytes, expected {valid_len}"
            )));
        }
        dest_file.sync_all()?;
        Ok(())
    }

    /// fsync the parent directory of `path` on Unix. This makes the directory entry for
    /// an atomic rename or a newly created file durable on typical POSIX file systems.
    pub fn sync_parent_dir(_path: impl AsRef<Path>) -> std::io::Result<()> {
        #[cfg(unix)]
        if let Some(parent) = _path.as_ref().parent() {
            if !parent.as_os_str().is_empty() {
                let dir = File::open(parent)?;
                dir.sync_all()?;
            }
        }
        Ok(())
    }

    pub fn truncate(&mut self, len: u64) -> Result<(), Error> {
        #[cfg(feature = "failpoint")]
        {
            if std::env::var_os("MINISQLITE_FAILPOINT").as_deref()
                == Some(std::ffi::OsStr::new("rollback-error"))
            {
                return Err(Error::Io("simulated truncate failure".into()));
            }
        }
        self.file.set_len(len)?;
        self.file.seek(SeekFrom::Start(len))?;
        if self.durability.requires_sync() {
            self.file.sync_all()?;
        }
        self.len = len;
        Ok(())
    }
}

#[cfg(unix)]
fn create_private_dirs(path: &Path) -> std::io::Result<()> {
    use std::fs::{set_permissions, DirBuilder, Permissions};
    use std::os::unix::fs::{DirBuilderExt, PermissionsExt};

    if path.as_os_str().is_empty() || path == Path::new(".") {
        return Ok(());
    }

    let mut components = Vec::new();
    for ancestor in path.ancestors() {
        if ancestor.as_os_str().is_empty() || ancestor == Path::new(".") {
            break;
        }
        components.push(ancestor);
    }
    components.reverse();

    for dir in components {
        if !dir.exists() {
            DirBuilder::new().mode(0o700).create(dir)?;
            set_permissions(dir, Permissions::from_mode(0o700))?;
        }
    }
    Ok(())
}

#[cfg(not(unix))]
fn create_private_dirs(path: &Path) -> std::io::Result<()> {
    std::fs::create_dir_all(path)
}

#[cfg(unix)]
fn sync_ancestors(path: &Path) -> std::io::Result<()> {
    for dir in path.ancestors().skip(1) {
        if dir.as_os_str().is_empty() {
            break;
        }
        let f = File::open(dir)?;
        f.sync_all()?;
    }
    Ok(())
}

#[cfg(not(unix))]
fn sync_ancestors(_path: &Path) -> std::io::Result<()> {
    Ok(())
}

fn current_time_ms() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_millis() as i64)
        .unwrap_or(0)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn creates_and_validates_header() {
        let tmp = std::env::temp_dir().join(format!("minisqlite_data_{}", std::process::id()));
        let _ = std::fs::remove_file(&tmp);
        let mut file = DataFile::open_or_create(&tmp, Durability::Memory, false).unwrap();
        assert_eq!(file.len, FILE_HEADER_SIZE as u64);
        let header = file.read_at(0, FILE_HEADER_SIZE).unwrap();
        let _ = FileHeader::decode(&header.try_into().unwrap()).unwrap();
        let _ = std::fs::remove_file(&tmp);
    }
}
