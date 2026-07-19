use crate::codec::frame::{
    Frame, FrameHeader, FILE_HEADER_SIZE, FRAME_HEADER_SIZE, FRAME_TRAILER_SIZE,
};
use crate::storage::file::DataFile;
use crate::Error;

#[derive(Debug)]
pub struct ScanResult {
    pub frames: Vec<Frame>,
    /// Whether the file ended with an incomplete final frame that was safely ignored.
    pub tail_truncated: bool,
    /// The file offset after the last valid frame.
    pub last_valid_offset: u64,
}

/// Scan the primary data file sequentially, validating every frame boundary and checksum.
///
/// Mid-file corruption causes a hard failure. An incomplete tail frame is reported so the
/// caller can truncate to `last_valid_offset`.
pub fn scan(data_file: &mut DataFile) -> Result<ScanResult, Error> {
    let file_len = data_file.file_len();
    let mut offset = FILE_HEADER_SIZE as u64;
    let mut last_valid = offset;
    let mut frames = Vec::new();
    let mut tail_truncated = false;

    while offset < file_len {
        let header_bytes = match data_file.read_at(offset, FRAME_HEADER_SIZE) {
            Ok(bytes) => bytes,
            Err(_) => {
                // Short read before a complete header: incomplete tail.
                tail_truncated = true;
                break;
            }
        };

        let header =
            match FrameHeader::decode(header_bytes[..FRAME_HEADER_SIZE].try_into().unwrap()) {
                Ok(h) => h,
                Err(e) => {
                    if offset + FRAME_HEADER_SIZE as u64 > file_len {
                        tail_truncated = true;
                        break;
                    }
                    return Err(e);
                }
            };

        if header.total_frame_length < (FRAME_HEADER_SIZE + FRAME_TRAILER_SIZE) as u64 {
            return Err(Error::Corruption {
                message: "impossible frame length".into(),
                offset,
            });
        }

        if offset + header.total_frame_length > file_len {
            // Declared frame extends past EOF: incomplete tail.
            tail_truncated = true;
            break;
        }

        // Read the complete frame (header + payload + trailer) and decode it.
        let frame_bytes = data_file.read_at(offset, header.total_frame_length as usize)?;
        let frame = Frame::decode(&frame_bytes)?;
        frames.push(frame);
        offset += header.total_frame_length;
        last_valid = offset;
    }

    Ok(ScanResult {
        frames,
        tail_truncated,
        last_valid_offset: last_valid,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::Durability;

    #[test]
    fn empty_file_has_no_frames() {
        let tmp = std::env::temp_dir().join(format!("minisqlite_rec_{}", std::process::id()));
        let _ = std::fs::remove_file(&tmp);
        let mut file = DataFile::open_or_create(&tmp, Durability::Memory).unwrap();
        let result = scan(&mut file).unwrap();
        assert!(result.frames.is_empty());
        assert!(!result.tail_truncated);
        let _ = std::fs::remove_file(&tmp);
    }
}
