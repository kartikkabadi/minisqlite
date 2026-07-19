use std::fs::{File, OpenOptions};
use std::io::{self, Read, Seek, SeekFrom, Write};

use crate::pager::PAGE_SIZE;

const WAL_MAGIC: u32 = 0x4D53514C; // "MSQL"

#[derive(Debug, Clone)]
pub struct WalEntry {
    pub page_num: u32,
    pub data: [u8; PAGE_SIZE],
}

#[derive(Debug)]
pub struct WriteAheadLog {
    file: Option<File>,
    entries: Vec<WalEntry>,
    active: bool,
    path: String,
}

impl WriteAheadLog {
    pub fn open(db_path: &str) -> io::Result<Self> {
        let wal_path = format!("{}-wal", db_path);
        let mut wal = WriteAheadLog {
            file: None,
            entries: Vec::new(),
            active: false,
            path: wal_path,
        };
        if std::path::Path::new(&wal.path).exists() {
            wal.recover()?;
        }
        Ok(wal)
    }

    pub fn begin(&mut self) -> io::Result<()> {
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(true)
            .open(&self.path)?;
        self.file = Some(file);
        self.entries.clear();
        self.active = true;
        if let Some(f) = &mut self.file {
            f.write_all(&WAL_MAGIC.to_be_bytes())?;
            f.write_all(&0u32.to_be_bytes())?; // entry count placeholder
        }
        Ok(())
    }

    pub fn commit(&mut self) -> io::Result<()> {
        if !self.active {
            return Ok(());
        }
        if let Some(f) = &mut self.file {
            f.seek(SeekFrom::Start(4))?;
            f.write_all(&(self.entries.len() as u32).to_be_bytes())?;
            f.sync_all()?;
        }
        self.active = false;
        self.cleanup()
    }

    pub fn rollback(&mut self) -> io::Result<()> {
        self.active = false;
        self.entries.clear();
        self.cleanup()
    }

    fn recover(&mut self) -> io::Result<()> {
        let mut file = File::open(&self.path)?;
        let mut header = [0u8; 8];
        if file.read_exact(&mut header).is_err() {
            return Ok(());
        }
        let magic = u32::from_be_bytes([header[0], header[1], header[2], header[3]]);
        if magic != WAL_MAGIC {
            return Ok(());
        }
        let count = u32::from_be_bytes([header[4], header[5], header[6], header[7]]) as usize;
        for _ in 0..count {
            let mut pn_buf = [0u8; 4];
            if file.read_exact(&mut pn_buf).is_err() {
                break;
            }
            let page_num = u32::from_be_bytes(pn_buf);
            let mut data = [0u8; PAGE_SIZE];
            if file.read_exact(&mut data).is_err() {
                break;
            }
            self.entries.push(WalEntry { page_num, data });
        }
        Ok(())
    }

    pub fn get_recovery_entries(&self) -> &[WalEntry] {
        &self.entries
    }

    fn cleanup(&mut self) -> io::Result<()> {
        self.file = None;
        if std::path::Path::new(&self.path).exists() {
            std::fs::remove_file(&self.path)?;
        }
        Ok(())
    }
}
