use crate::id::Id;
use crate::Error;

pub mod checksum;
pub mod frame;
pub mod record;

pub use record::encode_records;

/// A small, fallible little-endian encoder.
#[derive(Debug, Default, Clone)]
pub struct Writer {
    pub bytes: Vec<u8>,
}

#[allow(clippy::len_without_is_empty)]
impl Writer {
    pub fn new() -> Self {
        Self { bytes: Vec::new() }
    }

    pub fn with_capacity(cap: usize) -> Self {
        Self {
            bytes: Vec::with_capacity(cap),
        }
    }

    pub fn write_u8(&mut self, v: u8) {
        self.bytes.push(v);
    }

    pub fn write_u16(&mut self, v: u16) {
        self.bytes.extend_from_slice(&v.to_le_bytes());
    }

    pub fn write_u32(&mut self, v: u32) {
        self.bytes.extend_from_slice(&v.to_le_bytes());
    }

    pub fn write_u64(&mut self, v: u64) {
        self.bytes.extend_from_slice(&v.to_le_bytes());
    }

    pub fn write_i64(&mut self, v: i64) {
        self.bytes.extend_from_slice(&v.to_le_bytes());
    }

    pub fn write_bytes(&mut self, v: &[u8]) {
        self.write_u32(v.len() as u32);
        self.bytes.extend_from_slice(v);
    }

    pub fn write_string(&mut self, v: &str) {
        self.write_bytes(v.as_bytes());
    }

    pub fn write_id(&mut self, v: Id) {
        self.bytes.extend_from_slice(v.as_bytes());
    }

    pub fn write_optional_id(&mut self, v: Option<Id>) {
        self.write_u8(v.is_some() as u8);
        if let Some(id) = v {
            self.write_id(id);
        }
    }

    pub fn len(&self) -> usize {
        self.bytes.len()
    }
}

/// A small, fallible little-endian decoder over a byte slice.
#[derive(Debug, Clone)]
pub struct Reader<'a> {
    bytes: &'a [u8],
    pos: usize,
}

impl<'a> Reader<'a> {
    pub fn new(bytes: &'a [u8]) -> Self {
        Self { bytes, pos: 0 }
    }

    pub fn remaining(&self) -> usize {
        self.bytes.len().saturating_sub(self.pos)
    }

    pub fn is_empty(&self) -> bool {
        self.remaining() == 0
    }

    fn need(&self, n: usize) -> Result<(), Error> {
        if self.remaining() < n {
            Err(Error::Corruption {
                message: "short read".into(),
                offset: self.pos as u64,
            })
        } else {
            Ok(())
        }
    }

    pub fn read_u8(&mut self) -> Result<u8, Error> {
        self.need(1)?;
        let v = self.bytes[self.pos];
        self.pos += 1;
        Ok(v)
    }

    pub fn read_u16(&mut self) -> Result<u16, Error> {
        self.need(2)?;
        let v = u16::from_le_bytes(self.bytes[self.pos..self.pos + 2].try_into().unwrap());
        self.pos += 2;
        Ok(v)
    }

    pub fn read_u32(&mut self) -> Result<u32, Error> {
        self.need(4)?;
        let v = u32::from_le_bytes(self.bytes[self.pos..self.pos + 4].try_into().unwrap());
        self.pos += 4;
        Ok(v)
    }

    pub fn read_u64(&mut self) -> Result<u64, Error> {
        self.need(8)?;
        let v = u64::from_le_bytes(self.bytes[self.pos..self.pos + 8].try_into().unwrap());
        self.pos += 8;
        Ok(v)
    }

    pub fn read_i64(&mut self) -> Result<i64, Error> {
        self.need(8)?;
        let v = i64::from_le_bytes(self.bytes[self.pos..self.pos + 8].try_into().unwrap());
        self.pos += 8;
        Ok(v)
    }

    pub fn read_bytes(&mut self) -> Result<Vec<u8>, Error> {
        let len = self.read_u32()? as usize;
        self.need(len)?;
        let mut v = Vec::new();
        if let Err(e) = v.try_reserve_exact(len) {
            return Err(Error::Validation(format!(
                "cannot allocate {len} byte buffer: {e}"
            )));
        }
        v.extend_from_slice(&self.bytes[self.pos..self.pos + len]);
        self.pos += len;
        Ok(v)
    }

    pub fn read_string(&mut self) -> Result<String, Error> {
        let bytes = self.read_bytes()?;
        String::from_utf8(bytes).map_err(|_| Error::Corruption {
            message: "invalid utf-8 string".into(),
            offset: self.pos as u64,
        })
    }

    pub fn read_id(&mut self) -> Result<Id, Error> {
        self.need(16)?;
        let mut bytes = [0u8; 16];
        bytes.copy_from_slice(&self.bytes[self.pos..self.pos + 16]);
        self.pos += 16;
        Ok(Id::from_bytes(bytes))
    }

    pub fn read_optional_id(&mut self) -> Result<Option<Id>, Error> {
        let present = self.read_u8()?;
        match present {
            0 => Ok(None),
            1 => Ok(Some(self.read_id()?)),
            _ => Err(Error::Corruption {
                message: format!("invalid optional id marker {present}"),
                offset: self.pos as u64,
            }),
        }
    }

    pub fn read_slice(&mut self, n: usize) -> Result<&'a [u8], Error> {
        self.need(n)?;
        let slice = &self.bytes[self.pos..self.pos + n];
        self.pos += n;
        Ok(slice)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn roundtrip_primitives() {
        let mut w = Writer::new();
        w.write_u8(0xab);
        w.write_u16(0x1234);
        w.write_u32(0xdeadbeef);
        w.write_u64(0x0102030405060708);
        w.write_i64(-42);
        w.write_bytes(b"payload");
        w.write_string("hello");
        w.write_id(Id::from(42u128));
        w.write_optional_id(Some(Id::from(1u128)));
        w.write_optional_id(None);

        let mut r = Reader::new(&w.bytes);
        assert_eq!(r.read_u8().unwrap(), 0xab);
        assert_eq!(r.read_u16().unwrap(), 0x1234);
        assert_eq!(r.read_u32().unwrap(), 0xdeadbeef);
        assert_eq!(r.read_u64().unwrap(), 0x0102030405060708);
        assert_eq!(r.read_i64().unwrap(), -42);
        assert_eq!(r.read_bytes().unwrap(), b"payload");
        assert_eq!(r.read_string().unwrap(), "hello");
        assert_eq!(r.read_id().unwrap(), Id::from(42u128));
        assert_eq!(r.read_optional_id().unwrap(), Some(Id::from(1u128)));
        assert_eq!(r.read_optional_id().unwrap(), None);
    }
}
