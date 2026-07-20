use std::fs::{File, OpenOptions};
use std::io::{Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};

use crate::codec::frame::{FileHeader, FILE_HEADER_SIZE};
use crate::config::Durability;
use crate::Error;

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
    #[allow(dead_code)]
    path: PathBuf,
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
        let path = path.as_ref().to_path_buf();

        // Refuse to follow an existing symlink for the primary data path.
        if std::fs::symlink_metadata(&path)
            .map(|m| m.file_type().is_symlink())
            .unwrap_or(false)
        {
            return Err(Error::Validation(
                "primary database path is a symlink".into(),
            ));
        }

        if let Some(parent) = path.parent() {
            if !parent.as_os_str().is_empty() {
                create_private_dirs(parent)?;
            }
        }

        let mut opts = OpenOptions::new();
        opts.read(true).write(true).create(true).truncate(false);
        #[cfg(unix)]
        {
            use std::os::unix::fs::OpenOptionsExt;
            opts.mode(0o600);
        }
        let mut file = opts.open(&path)?;

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

    /// Copy the entire contents of the locked primary file to `dest`.
    ///
    /// The copy reads from the already-open `File` handle so it works on Windows even though
    /// the file is exclusively locked; `std::fs::copy` would fail with ERROR_LOCK_VIOLATION.
    pub fn copy_to(&mut self, dest: impl AsRef<Path>) -> std::io::Result<()> {
        self.file.flush()?;
        self.file.seek(SeekFrom::Start(0))?;
        let mut dest_file = OpenOptions::new().write(true).open(dest.as_ref())?;
        let copied = std::io::copy(&mut self.file, &mut dest_file)?;
        if copied != self.len {
            return Err(std::io::Error::other(format!(
                "copy length mismatch: copied {copied} bytes, expected {}",
                self.len
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
