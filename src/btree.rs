use crate::pager::{Pager, PAGE_SIZE};
use std::collections::BTreeMap;
use std::io;

#[derive(Debug, Clone)]
pub struct BTree {
    root: u32,
    data: BTreeMap<Vec<u8>, Vec<u8>>,
    pages: Vec<u32>,
}

impl BTree {
    pub fn create(pager: &mut Pager) -> io::Result<Self> {
        let root = pager.allocate_page()?;
        let mut page = [0u8; PAGE_SIZE];
        page[0..4].copy_from_slice(&0u32.to_be_bytes()); // next page = 0
        page[4..8].copy_from_slice(&0u32.to_be_bytes()); // count = 0
        pager.write_page(root, &page)?;
        Ok(BTree {
            root,
            data: BTreeMap::new(),
            pages: vec![root],
        })
    }

    pub fn open(root: u32) -> Self {
        BTree {
            root,
            data: BTreeMap::new(),
            pages: vec![root],
        }
    }

    pub fn load(&mut self, pager: &mut Pager) -> io::Result<()> {
        self.data.clear();
        let mut bytes = Vec::new();
        self.pages.clear();
        let mut page_num = self.root;
        loop {
            self.pages.push(page_num);
            let page = pager.read_page(page_num)?;
            let next = u32::from_be_bytes([page[0], page[1], page[2], page[3]]);
            // Use the whole data area; count tells us when to stop, so trailing zeroes are fine.
            bytes.extend_from_slice(&page[4..]);
            if next == 0 {
                break;
            }
            page_num = next;
        }
        if bytes.is_empty() {
            return Ok(());
        }
        let mut offset = 0;
        let count = read_u32(&bytes, &mut offset)? as usize;
        for _ in 0..count {
            let key_len = read_u32(&bytes, &mut offset)? as usize;
            let key = bytes[offset..offset + key_len].to_vec();
            offset += key_len;
            let payload_len = read_u32(&bytes, &mut offset)? as usize;
            let payload = bytes[offset..offset + payload_len].to_vec();
            offset += payload_len;
            self.data.insert(key, payload);
        }
        Ok(())
    }

    pub fn flush(&mut self, pager: &mut Pager) -> io::Result<u32> {
        let mut bytes = Vec::new();
        write_u32(&mut bytes, self.data.len() as u32);
        for (key, payload) in &self.data {
            write_u32(&mut bytes, key.len() as u32);
            bytes.extend_from_slice(key);
            write_u32(&mut bytes, payload.len() as u32);
            bytes.extend_from_slice(payload);
        }

        let data_per_page = PAGE_SIZE - 4;
        let pages_needed = if bytes.is_empty() {
            1
        } else {
            (bytes.len() + data_per_page - 1) / data_per_page
        };

        let mut new_pages = Vec::with_capacity(pages_needed);
        for _ in 0..pages_needed {
            new_pages.push(pager.allocate_page()?);
        }

        for (i, &page_num) in new_pages.iter().enumerate() {
            let mut page = [0u8; PAGE_SIZE];
            let next = if i + 1 < new_pages.len() {
                new_pages[i + 1]
            } else {
                0
            };
            page[0..4].copy_from_slice(&next.to_be_bytes());
            let start = i * data_per_page;
            let end = ((i + 1) * data_per_page).min(bytes.len());
            if start < bytes.len() {
                page[4..4 + (end - start)].copy_from_slice(&bytes[start..end]);
            }
            pager.write_page(page_num, &page)?;
        }

        for &old in &self.pages {
            pager.free_page(old);
        }

        self.pages = new_pages;
        self.root = self.pages[0];
        Ok(self.root)
    }

    pub fn insert(&mut self, key: i64, payload: &[u8]) {
        let mut k = Vec::with_capacity(8);
        k.extend_from_slice(&key.to_be_bytes());
        self.data.insert(k, payload.to_vec());
    }

    pub fn insert_kv(&mut self, key: Vec<u8>, payload: Vec<u8>) {
        self.data.insert(key, payload);
    }

    pub fn delete(&mut self, key: i64) -> Option<Vec<u8>> {
        let k = key.to_be_bytes().to_vec();
        self.data.remove(&k)
    }

    pub fn scan(&self) -> impl Iterator<Item = (&Vec<u8>, &Vec<u8>)> {
        self.data.iter()
    }

    pub fn root(&self) -> u32 {
        self.root
    }
}

fn read_u32(bytes: &[u8], offset: &mut usize) -> io::Result<u32> {
    if *offset + 4 > bytes.len() {
        return Err(io::Error::new(io::ErrorKind::UnexpectedEof, "truncated u32"));
    }
    let v = u32::from_be_bytes([
        bytes[*offset],
        bytes[*offset + 1],
        bytes[*offset + 2],
        bytes[*offset + 3],
    ]);
    *offset += 4;
    Ok(v)
}

fn write_u32(buf: &mut Vec<u8>, v: u32) {
    buf.extend_from_slice(&v.to_be_bytes());
}
