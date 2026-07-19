use std::fs::{File, OpenOptions};
use std::io::{self, Read, Seek, SeekFrom, Write};

pub const PAGE_SIZE: usize = 4096;
pub const MAGIC: &[u8; 16] = b"MiniSQL2\0\0\0\0\0\0\0\0";

#[derive(Debug)]
pub struct Pager {
    file: File,
    pub page_count: u32,
    pub catalog_root: u32,
    pub freelist: Vec<u32>,
    dirty_pages: Vec<(u32, [u8; PAGE_SIZE])>,
}

impl Pager {
    pub fn open(path: &str) -> io::Result<Self> {
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(false)
            .open(path)?;
        let meta = file.metadata()?;
        let page_count = if meta.len() == 0 {
            0
        } else {
            (meta.len() / PAGE_SIZE as u64) as u32
        };
        let mut pager = Pager {
            file,
            page_count,
            catalog_root: 0,
            freelist: Vec::new(),
            dirty_pages: Vec::new(),
        };
        if page_count == 0 {
            pager.init_db()?;
        } else {
            pager.read_header()?;
        }
        Ok(pager)
    }

    fn init_db(&mut self) -> io::Result<()> {
        self.file.seek(SeekFrom::Start(0))?;
        self.file.write_all(&[0u8; PAGE_SIZE])?;
        self.page_count = 1;
        self.catalog_root = 0;
        self.write_header()?;
        Ok(())
    }

    fn read_header(&mut self) -> io::Result<()> {
        let mut buf = [0u8; 30];
        self.file.seek(SeekFrom::Start(0))?;
        self.file.read_exact(&mut buf)?;
        if &buf[0..16] != MAGIC {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "Not a MiniSQLite database",
            ));
        }
        self.page_count = u32::from_be_bytes([buf[18], buf[19], buf[20], buf[21]]);
        let fl_count = u32::from_be_bytes([buf[22], buf[23], buf[24], buf[25]]) as usize;
        self.catalog_root = u32::from_be_bytes([buf[26], buf[27], buf[28], buf[29]]);
        if fl_count > 0 {
            let mut fl_buf = vec![0u8; fl_count * 4];
            self.file.seek(SeekFrom::Start(30))?;
            self.file.read_exact(&mut fl_buf)?;
            for i in 0..fl_count {
                let pg = u32::from_be_bytes([
                    fl_buf[i * 4],
                    fl_buf[i * 4 + 1],
                    fl_buf[i * 4 + 2],
                    fl_buf[i * 4 + 3],
                ]);
                self.freelist.push(pg);
            }
        }
        Ok(())
    }

    fn write_header(&mut self) -> io::Result<()> {
        let mut buf = [0u8; 30];
        buf[0..16].copy_from_slice(MAGIC);
        buf[16..18].copy_from_slice(&(PAGE_SIZE as u16).to_be_bytes());
        buf[18..22].copy_from_slice(&self.page_count.to_be_bytes());
        buf[22..26].copy_from_slice(&(self.freelist.len() as u32).to_be_bytes());
        buf[26..30].copy_from_slice(&self.catalog_root.to_be_bytes());
        self.file.seek(SeekFrom::Start(0))?;
        self.file.write_all(&buf)?;
        for pg in &self.freelist {
            self.file.write_all(&pg.to_be_bytes())?;
        }
        self.file.flush()?;
        Ok(())
    }

    pub fn read_page(&mut self, page_num: u32) -> io::Result<[u8; PAGE_SIZE]> {
        for (pn, data) in self.dirty_pages.iter().rev() {
            if *pn == page_num {
                return Ok(*data);
            }
        }
        let mut buf = [0u8; PAGE_SIZE];
        let offset = page_num as u64 * PAGE_SIZE as u64;
        self.file.seek(SeekFrom::Start(offset))?;
        self.file.read_exact(&mut buf)?;
        Ok(buf)
    }

    pub fn write_page(&mut self, page_num: u32, data: &[u8; PAGE_SIZE]) -> io::Result<()> {
        self.dirty_pages.push((page_num, *data));
        Ok(())
    }

    pub fn flush(&mut self) -> io::Result<()> {
        for (page_num, data) in self.dirty_pages.drain(..) {
            let offset = page_num as u64 * PAGE_SIZE as u64;
            self.file.seek(SeekFrom::Start(offset))?;
            self.file.write_all(&data)?;
        }
        self.write_header()?;
        self.file.flush()?;
        Ok(())
    }

    pub fn allocate_page(&mut self) -> io::Result<u32> {
        // Always extend the file to keep page numbers stable and avoid reuse bugs.
        let pg = self.page_count;
        self.page_count += 1;
        let zero = [0u8; PAGE_SIZE];
        self.write_page(pg, &zero)?;
        Ok(pg)
    }

    pub fn free_page(&mut self, _page_num: u32) {
        // Pages are not reused for simplicity.
    }
}
