use crate::codec::checksum::crc32;
use crate::codec::{Reader, Writer};
use crate::id::Id;
use crate::Error;

pub const FILE_MAGIC: &[u8; 8] = b"MINISQL3";
pub const FRAME_MAGIC: &[u8; 8] = b"MINIFRAM";
pub const TRAILER_MAGIC: &[u8; 8] = b"FRAMETRL";

pub const FILE_HEADER_SIZE: usize = 64;
pub const FRAME_HEADER_SIZE: usize = 64;
pub const FRAME_TRAILER_SIZE: usize = 32;

/// Hard upper bound on any transaction frame, including header, payload, and trailer.
/// This protects the recovery scanner from allocating unbounded memory on a corrupted file.
pub const MAX_FRAME_SIZE: usize = 64 << 20; // 64 MiB

pub const FORMAT_MAJOR: u16 = 0;
pub const FORMAT_MINOR: u16 = 1;

/// Fixed file header.
///
/// Layout (64 bytes, little-endian):
///   0..8   magic: "MINISQL3"
///   8..10  major version
///  10..12  minor version
///  12..14  header length
///  14..22  created_at_ms
///  22..26  flags
///  26..60  reserved
///  60..64  header CRC32
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct FileHeader {
    pub major: u16,
    pub minor: u16,
    pub header_length: u16,
    pub created_at_ms: i64,
    pub flags: u32,
}

impl FileHeader {
    pub fn new(created_at_ms: i64) -> Self {
        Self {
            major: FORMAT_MAJOR,
            minor: FORMAT_MINOR,
            header_length: FILE_HEADER_SIZE as u16,
            created_at_ms,
            flags: 0,
        }
    }

    pub fn encode(&self) -> [u8; FILE_HEADER_SIZE] {
        let mut w = Writer::with_capacity(FILE_HEADER_SIZE);
        w.bytes.extend_from_slice(FILE_MAGIC);
        w.write_u16(self.major);
        w.write_u16(self.minor);
        w.write_u16(self.header_length);
        w.write_i64(self.created_at_ms);
        w.write_u32(self.flags);
        // Reserved to fill header size minus checksum.
        w.bytes.resize(FILE_HEADER_SIZE - 4, 0);
        let checksum = crc32(&[&w.bytes]);
        w.write_u32(checksum);
        let mut out = [0u8; FILE_HEADER_SIZE];
        out.copy_from_slice(&w.bytes);
        out
    }

    pub fn decode(bytes: &[u8; FILE_HEADER_SIZE]) -> Result<Self, Error> {
        if &bytes[0..8] != FILE_MAGIC.as_slice() {
            return Err(Error::NotMiniSQLite);
        }
        let stored_checksum = u32::from_le_bytes(bytes[60..64].try_into().unwrap());
        let computed = crc32(&[&bytes[0..60]]);
        if stored_checksum != computed {
            return Err(Error::Corruption {
                message: "file header checksum mismatch".into(),
                offset: 0,
            });
        }
        let mut r = Reader::new(&bytes[8..60]);
        let major = r.read_u16()?;
        let minor = r.read_u16()?;
        let header_length = r.read_u16()?;
        let created_at_ms = r.read_i64()?;
        let flags = r.read_u32()?;
        if major > FORMAT_MAJOR {
            return Err(Error::UnsupportedVersion { major, minor });
        }
        if major == FORMAT_MAJOR && minor > FORMAT_MINOR {
            return Err(Error::UnsupportedVersion { major, minor });
        }
        Ok(Self {
            major,
            minor,
            header_length,
            created_at_ms,
            flags,
        })
    }
}

/// Transaction frame header.
///
/// Layout (64 bytes, little-endian):
///   0..8   magic: "MINIFRAM"
///   8..10  frame format version
///  10..18  total frame length
///  18..26  transaction sequence
///  26..42  transaction id
///  42..50  commit timestamp (ms)
///  50..54  record count
///  54..58  payload length
///  58..60  reserved
///  60..64  header CRC32
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FrameHeader {
    pub version: u16,
    pub total_frame_length: u64,
    pub transaction_sequence: u64,
    pub transaction_id: Id,
    pub commit_timestamp_ms: i64,
    pub record_count: u32,
    pub payload_length: u32,
}

impl FrameHeader {
    pub fn encode_without_checksum(&self) -> [u8; FRAME_HEADER_SIZE - 4] {
        let mut w = Writer::with_capacity(FRAME_HEADER_SIZE - 4);
        w.bytes.extend_from_slice(FRAME_MAGIC);
        w.write_u16(self.version);
        w.write_u64(self.total_frame_length);
        w.write_u64(self.transaction_sequence);
        w.write_id(self.transaction_id);
        w.write_i64(self.commit_timestamp_ms);
        w.write_u32(self.record_count);
        w.write_u32(self.payload_length);
        // Reserved bytes to fill frame header size minus checksum.
        w.bytes.resize(FRAME_HEADER_SIZE - 4, 0);
        let mut out = [0u8; FRAME_HEADER_SIZE - 4];
        out.copy_from_slice(&w.bytes);
        out
    }

    pub fn decode(bytes: &[u8; FRAME_HEADER_SIZE]) -> Result<Self, Error> {
        if &bytes[0..8] != FRAME_MAGIC.as_slice() {
            return Err(Error::Corruption {
                message: "frame magic mismatch".into(),
                offset: 0,
            });
        }
        let stored = u32::from_le_bytes(bytes[60..64].try_into().unwrap());
        let computed = crc32(&[&bytes[0..60]]);
        if stored != computed {
            return Err(Error::Corruption {
                message: "frame header checksum mismatch".into(),
                offset: 0,
            });
        }
        let mut r = Reader::new(&bytes[8..60]);
        let version = r.read_u16()?;
        let total_frame_length = r.read_u64()?;
        let transaction_sequence = r.read_u64()?;
        let transaction_id = r.read_id()?;
        let commit_timestamp_ms = r.read_i64()?;
        let record_count = r.read_u32()?;
        let payload_length = r.read_u32()?;
        Ok(Self {
            version,
            total_frame_length,
            transaction_sequence,
            transaction_id,
            commit_timestamp_ms,
            record_count,
            payload_length,
        })
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct FrameTrailer {
    pub transaction_sequence: u64,
    pub total_frame_length: u64,
    pub checksum: u32,
}

impl FrameTrailer {
    pub fn encode(&self) -> [u8; FRAME_TRAILER_SIZE] {
        let mut w = Writer::with_capacity(FRAME_TRAILER_SIZE);
        w.bytes.extend_from_slice(TRAILER_MAGIC);
        w.write_u64(self.transaction_sequence);
        w.write_u64(self.total_frame_length);
        w.write_u32(self.checksum);
        // Reserved
        w.write_u32(0);
        let mut out = [0u8; FRAME_TRAILER_SIZE];
        out.copy_from_slice(&w.bytes);
        out
    }

    pub fn decode(bytes: &[u8; FRAME_TRAILER_SIZE]) -> Result<Self, Error> {
        if &bytes[0..8] != TRAILER_MAGIC.as_slice() {
            return Err(Error::Corruption {
                message: "frame trailer magic mismatch".into(),
                offset: 0,
            });
        }
        let mut r = Reader::new(&bytes[8..]);
        let transaction_sequence = r.read_u64()?;
        let total_frame_length = r.read_u64()?;
        let checksum = r.read_u32()?;
        Ok(Self {
            transaction_sequence,
            total_frame_length,
            checksum,
        })
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Frame {
    pub header: FrameHeader,
    pub payload: Vec<u8>,
    pub trailer: FrameTrailer,
}

impl Frame {
    pub fn new(header: FrameHeader, payload: Vec<u8>) -> Self {
        let total = FRAME_HEADER_SIZE as u64 + payload.len() as u64 + FRAME_TRAILER_SIZE as u64;
        let mut header = header;
        header.total_frame_length = total;
        header.payload_length = payload.len() as u32;
        let header_bytes = header.encode_without_checksum();
        let trailer_body = {
            let mut w = Writer::with_capacity(FRAME_TRAILER_SIZE - 4);
            w.bytes.extend_from_slice(TRAILER_MAGIC);
            w.write_u64(header.transaction_sequence);
            w.write_u64(total);
            w.bytes
        };
        let checksum = crc32(&[&header_bytes, &payload, &trailer_body]);
        let trailer = FrameTrailer {
            transaction_sequence: header.transaction_sequence,
            total_frame_length: total,
            checksum,
        };
        Self {
            header,
            payload,
            trailer,
        }
    }

    pub fn encode(&self) -> Vec<u8> {
        let header_bytes = self.header.encode_without_checksum();
        let header_checksum = crc32(&[&header_bytes]);
        let trailer_bytes = self.trailer.encode();
        let mut out = Vec::with_capacity(self.header.total_frame_length as usize);
        out.extend_from_slice(&header_bytes);
        out.extend_from_slice(&header_checksum.to_le_bytes());
        out.extend_from_slice(&self.payload);
        out.extend_from_slice(&trailer_bytes);
        out
    }

    pub fn decode(bytes: &[u8]) -> Result<Self, Error> {
        if bytes.len() < FRAME_HEADER_SIZE + FRAME_TRAILER_SIZE {
            return Err(Error::Corruption {
                message: "frame too short".into(),
                offset: 0,
            });
        }
        let header_bytes: &[u8; FRAME_HEADER_SIZE] = bytes[..FRAME_HEADER_SIZE].try_into().unwrap();
        let header = FrameHeader::decode(header_bytes)?;

        if header.total_frame_length as usize > MAX_FRAME_SIZE {
            return Err(Error::Corruption {
                message: "frame exceeds maximum allowed size".into(),
                offset: 0,
            });
        }

        let expected_total =
            FRAME_HEADER_SIZE as u64 + header.payload_length as u64 + FRAME_TRAILER_SIZE as u64;
        if header.total_frame_length != expected_total {
            return Err(Error::Corruption {
                message: "frame total length does not match header fields".into(),
                offset: 0,
            });
        }

        if bytes.len() < header.total_frame_length as usize {
            return Err(Error::Corruption {
                message: "frame truncated".into(),
                offset: 0,
            });
        }

        let payload_start = FRAME_HEADER_SIZE;
        let payload_end = payload_start + header.payload_length as usize;
        let payload = bytes[payload_start..payload_end].to_vec();
        let trailer_start = payload_end;
        let trailer_end = trailer_start + FRAME_TRAILER_SIZE;
        if trailer_end > bytes.len() {
            return Err(Error::Corruption {
                message: "frame trailer truncated".into(),
                offset: 0,
            });
        }
        let trailer_bytes: &[u8; FRAME_TRAILER_SIZE] =
            bytes[trailer_start..trailer_end].try_into().unwrap();
        let trailer = FrameTrailer::decode(trailer_bytes)?;
        if trailer.total_frame_length != header.total_frame_length {
            return Err(Error::Corruption {
                message: "frame length mismatch between header and trailer".into(),
                offset: 0,
            });
        }
        if trailer.transaction_sequence != header.transaction_sequence {
            return Err(Error::Corruption {
                message: "transaction sequence mismatch between header and trailer".into(),
                offset: 0,
            });
        }
        let header_bytes_for_checksum = &bytes[0..FRAME_HEADER_SIZE - 4];
        let computed = crc32(&[
            header_bytes_for_checksum,
            &payload,
            &trailer_bytes[..FRAME_TRAILER_SIZE - 8],
        ]);
        if computed != trailer.checksum {
            return Err(Error::Corruption {
                message: "frame checksum mismatch".into(),
                offset: 0,
            });
        }
        Ok(Self {
            header,
            payload,
            trailer,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use proptest::prelude::*;

    proptest! {
        #![proptest_config(ProptestConfig::with_cases(256))]

        #[test]
        fn file_header_arbitrary_bytes_never_panics(bytes in proptest::collection::vec(any::<u8>(), 0..128)) {
            let mut buf = [0u8; FILE_HEADER_SIZE];
            for (i, b) in bytes.iter().enumerate().take(FILE_HEADER_SIZE) {
                buf[i] = *b;
            }
            let _ = FileHeader::decode(&buf);
        }

        #[test]
        fn frame_arbitrary_bytes_never_panics(bytes in proptest::collection::vec(any::<u8>(), 0..1024)) {
            let _ = Frame::decode(&bytes);
        }
    }

    #[test]
    fn file_header_roundtrip() {
        let h = FileHeader::new(123456789);
        let bytes = h.encode();
        let decoded = FileHeader::decode(&bytes).unwrap();
        assert_eq!(h, decoded);
    }

    #[test]
    fn frame_roundtrip() {
        let header = FrameHeader {
            version: 1,
            total_frame_length: 0,
            transaction_sequence: 7,
            transaction_id: Id::from(99u128),
            commit_timestamp_ms: 123,
            record_count: 0,
            payload_length: 0,
        };
        let frame = Frame::new(header, b"hello".to_vec());
        let encoded = frame.encode();
        let decoded = Frame::decode(&encoded).unwrap();
        assert_eq!(decoded.payload, b"hello");
        assert_eq!(decoded.header.transaction_sequence, 7);
    }
}
