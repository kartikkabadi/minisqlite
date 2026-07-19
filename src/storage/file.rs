use std::fs::{File, OpenOptions};
use std::io::{Read, Seek, SeekFrom, Write};
use std::path::Path;

#[cfg(unix)]
use std::{fs::Permissions, os::unix::fs::PermissionsExt};

use crate::codec::frame::{FileHeader, FILE_HEADER_SIZE};
use crate::config::Durability;
use crate::Error;

/// Owns the primary data file and the normal append path.
#[derive(Debug)]
pub struct DataFile {
    file: File,
    pub durability: Durability,
    pub len: u64,
    header: FileHeader,
}

impl DataFile {
    /// Open or create the primary data file and validate its header.
    pub fn open_or_create(path: impl AsRef<Path>, durability: Durability) -> Result<Self, Error> {
        let path = path.as_ref().to_path_buf();

        // Refuse to follow an existing symlink for the primary data path.
        if let Ok(meta) = std::fs::symlink_metadata(&path) {
            if meta.file_type().is_symlink() {
                return Err(Error::Validation(
                    "primary database path is a symlink".into(),
                ));
            }
        }

        if let Some(parent) = path.parent() {
            let parent_existed = parent.exists();
            std::fs::create_dir_all(parent)?;
            #[cfg(unix)]
            {
                if !parent_existed {
                    use std::fs::set_permissions;
                    let _ = set_permissions(parent, Permissions::from_mode(0o700));
                }
            }
        }

        let mut file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(false)
            .open(&path)?;

        #[cfg(unix)]
        {
            use std::fs::set_permissions;
            let _ = set_permissions(&path, Permissions::from_mode(0o600));
        }

        let len = file.seek(SeekFrom::End(0))?;

        let file_len = if len == 0 {
            FILE_HEADER_SIZE as u64
        } else {
            len
        };

        let header = if len == 0 {
            let now_ms = current_time_ms();
            let header = FileHeader::new(now_ms);
            let bytes = header.encode();
            file.write_all(&bytes)?;
            if durability.requires_sync() {
                file.sync_all()?;
            }
            file.seek(SeekFrom::Start(0))?;
            header
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
            FileHeader::decode(&header_bytes)?
        };

        Ok(Self {
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
        self.file.sync_all()?;
        Ok(())
    }

    pub fn truncate(&mut self, len: u64) -> Result<(), Error> {
        self.file.set_len(len)?;
        self.file.seek(SeekFrom::Start(len))?;
        if self.durability.requires_sync() {
            self.file.sync_all()?;
        }
        self.len = len;
        Ok(())
    }
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
        let mut file = DataFile::open_or_create(&tmp, Durability::Memory).unwrap();
        assert_eq!(file.len, FILE_HEADER_SIZE as u64);
        let header = file.read_at(0, FILE_HEADER_SIZE).unwrap();
        let _ = FileHeader::decode(&header.try_into().unwrap()).unwrap();
        let _ = std::fs::remove_file(&tmp);
    }
}
