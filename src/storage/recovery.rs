use crate::codec::frame::{
    Frame, FrameHeader, FILE_HEADER_SIZE, FRAME_HEADER_SIZE, FRAME_TRAILER_SIZE, MAX_FRAME_SIZE,
};
use crate::storage::file::DataFile;
use crate::Error;

#[derive(Debug)]
pub struct ScanResult {
    /// Whether the file ended with an incomplete final frame that was safely ignored.
    pub tail_truncated: bool,
    /// The file offset after the last valid frame.
    pub last_valid_offset: u64,
}

/// Scan the primary data file sequentially, replaying each valid frame through `on_frame`.
///
/// Mid-file corruption and fully synced final-frame corruption (checksum, trailer, magic,
/// or semantic errors) cause a hard failure. Only a physically incomplete tail at EOF is
/// reported as `tail_truncated` so the caller can truncate to `last_valid_offset`.
/// Frames are not accumulated; the callback handles each frame as it is validated.
pub fn scan(
    data_file: &mut DataFile,
    mut on_frame: impl FnMut(Frame, u64) -> Result<(), Error>,
) -> Result<ScanResult, Error> {
    let file_len = data_file.file_len();
    let mut offset = FILE_HEADER_SIZE as u64;
    let mut last_valid = offset;
    let mut tail_truncated = false;

    while offset < file_len {
        let remaining = file_len
            .checked_sub(offset)
            .ok_or_else(|| Error::Corruption {
                message: "file offset exceeds file length".into(),
                offset,
            })?;
        if remaining < FRAME_HEADER_SIZE as u64 {
            tail_truncated = true;
            break;
        }

        let header_bytes = data_file.read_at(offset, FRAME_HEADER_SIZE)?;

        // A header that is exactly `FRAME_HEADER_SIZE` bytes long and fails to decode is
        // treated as an incomplete tail header: the process crashed while writing the header.
        // A header with trailing bytes after it that fails to decode is mid-file corruption.
        let header =
            match FrameHeader::decode(header_bytes[..FRAME_HEADER_SIZE].try_into().unwrap()) {
                Ok(h) => h,
                Err(_e) if remaining == FRAME_HEADER_SIZE as u64 => {
                    tail_truncated = true;
                    break;
                }
                Err(e) => return Err(with_offset(e, offset)),
            };

        if header.total_frame_length < (FRAME_HEADER_SIZE + FRAME_TRAILER_SIZE) as u64 {
            return Err(Error::Corruption {
                message: "impossible frame length".into(),
                offset,
            });
        }

        if header.total_frame_length > MAX_FRAME_SIZE as u64 {
            return Err(Error::Corruption {
                message: "frame exceeds maximum allowed size".into(),
                offset,
            });
        }

        let frame_end = offset
            .checked_add(header.total_frame_length)
            .ok_or_else(|| Error::Corruption {
                message: "frame offset overflow".into(),
                offset,
            })?;
        if frame_end > file_len {
            // Declared frame extends past EOF: incomplete tail.
            tail_truncated = true;
            break;
        }

        // Read the complete frame (header + payload + trailer) and decode it. If the frame
        // is fully present and still fails to decode, it is semantic/physical corruption.
        let frame_bytes = data_file.read_at(offset, header.total_frame_length as usize)?;
        let frame = Frame::decode(&frame_bytes).map_err(|e| with_offset(e, offset))?;
        on_frame(frame, offset)?;
        offset = frame_end;
        last_valid = offset;
    }

    Ok(ScanResult {
        tail_truncated,
        last_valid_offset: last_valid,
    })
}

fn with_offset(error: Error, offset: u64) -> Error {
    match error {
        Error::Corruption { message, .. } => Error::Corruption { message, offset },
        other => other,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::codec::checksum::crc32;
    use crate::codec::frame::{FileHeader, TRAILER_MAGIC};
    use crate::codec::record::{encode_records, EventRecord, Record};
    use crate::codec::Writer;
    use crate::config::Durability;
    use crate::id::Id;

    #[test]
    fn random_trailing_bytes_never_panics() {
        for seed in 0..128 {
            let mut rng = fastrand::Rng::with_seed(seed);
            let len = rng.usize(0..1024);
            let suffix: Vec<u8> = (0..len).map(|_| rng.u8(..)).collect();

            let tmp = std::env::temp_dir().join(format!(
                "minisqlite_recfuzz_{}_{}",
                std::process::id(),
                seed
            ));
            let _ = std::fs::remove_file(&tmp);
            {
                let header = FileHeader::new(0);
                std::fs::write(&tmp, header.encode()).unwrap();
                use std::io::Write;
                let mut f = std::fs::OpenOptions::new().append(true).open(&tmp).unwrap();
                f.write_all(&suffix).unwrap();
            }
            if let Ok(mut file) = DataFile::open_or_create(&tmp, Durability::Memory, false) {
                let _ = scan(&mut file, |_, _| Ok(()));
            }
            let _ = std::fs::remove_file(&tmp);
        }
    }

    #[test]
    fn empty_file_has_no_frames() {
        let tmp = std::env::temp_dir().join(format!("minisqlite_rec_{}", std::process::id()));
        let _ = std::fs::remove_file(&tmp);
        let mut file = DataFile::open_or_create(&tmp, Durability::Memory, false).unwrap();
        let result = scan(&mut file, |_, _| Ok(())).unwrap();
        assert_eq!(result.last_valid_offset, FILE_HEADER_SIZE as u64);
        assert!(!result.tail_truncated);
        let _ = std::fs::remove_file(&tmp);
    }

    fn event_record(sequence: u64, stream_version: u64) -> Record {
        Record::Event(EventRecord {
            global_sequence: sequence,
            stream_version,
            event_id: Id::new().unwrap(),
            stream_id: "stream".into(),
            event_type: "e".into(),
            schema_version: 1,
            occurred_at_ms: sequence as i64,
            causation_id: None,
            correlation_id: None,
            payload: b"{}".to_vec(),
            metadata: vec![],
        })
    }

    fn make_frame(sequence: u64, transaction_id: Id) -> Frame {
        let payload = encode_records(&[event_record(sequence, 1)]);
        let header = FrameHeader {
            version: 1,
            total_frame_length: 0,
            transaction_sequence: sequence,
            transaction_id,
            commit_timestamp_ms: sequence as i64,
            record_count: 1,
            payload_length: payload.len() as u32,
        };
        Frame::new(header, payload)
    }

    fn append_frame(file: &mut DataFile, sequence: u64, transaction_id: Id) {
        let frame = make_frame(sequence, transaction_id);
        let bytes = frame.encode();
        file.append_frame(&bytes, frame.header.payload_length as u64)
            .unwrap();
    }

    #[test]
    fn scans_multiple_valid_frames() {
        let tmp = std::env::temp_dir().join(format!("minisqlite_rec_multi_{}", std::process::id()));
        let _ = std::fs::remove_file(&tmp);
        let mut file = DataFile::open_or_create(&tmp, Durability::Memory, false).unwrap();
        append_frame(&mut file, 1, Id::new().unwrap());
        append_frame(&mut file, 2, Id::new().unwrap());
        let mut count = 0;
        let result = scan(&mut file, |_, _| {
            count += 1;
            Ok(())
        })
        .unwrap();
        assert_eq!(count, 2);
        assert!(!result.tail_truncated);
        let _ = std::fs::remove_file(&tmp);
    }

    #[test]
    fn truncates_incomplete_header_tail() {
        let tmp = std::env::temp_dir().join(format!("minisqlite_rec_hdr_{}", std::process::id()));
        let _ = std::fs::remove_file(&tmp);
        let mut file = DataFile::open_or_create(&tmp, Durability::Memory, false).unwrap();
        append_frame(&mut file, 1, Id::new().unwrap());
        // Append a few bytes of a second header.
        let partial = &make_frame(2, Id::new().unwrap()).encode()[..20];
        file.append_frame(partial, 0).unwrap();
        let mut count = 0;
        let result = scan(&mut file, |_, _| {
            count += 1;
            Ok(())
        })
        .unwrap();
        assert_eq!(count, 1);
        assert!(result.tail_truncated);
        let _ = std::fs::remove_file(&tmp);
    }

    #[test]
    fn truncates_incomplete_payload_tail() {
        let tmp = std::env::temp_dir().join(format!("minisqlite_rec_pay_{}", std::process::id()));
        let _ = std::fs::remove_file(&tmp);
        let mut file = DataFile::open_or_create(&tmp, Durability::Memory, false).unwrap();
        append_frame(&mut file, 1, Id::new().unwrap());
        let frame = make_frame(2, Id::new().unwrap()).encode();
        let partial = &frame[..FRAME_HEADER_SIZE + 5];
        file.append_frame(partial, 0).unwrap();
        let mut count = 0;
        let result = scan(&mut file, |_, _| {
            count += 1;
            Ok(())
        })
        .unwrap();
        assert_eq!(count, 1);
        assert!(result.tail_truncated);
        let _ = std::fs::remove_file(&tmp);
    }

    #[test]
    fn truncates_incomplete_trailer_tail() {
        let tmp = std::env::temp_dir().join(format!("minisqlite_rec_trl_{}", std::process::id()));
        let _ = std::fs::remove_file(&tmp);
        let mut file = DataFile::open_or_create(&tmp, Durability::Memory, false).unwrap();
        append_frame(&mut file, 1, Id::new().unwrap());
        let frame = make_frame(2, Id::new().unwrap()).encode();
        let payload_len = make_frame(2, Id::new().unwrap()).header.payload_length as usize;
        let partial = &frame[..FRAME_HEADER_SIZE + payload_len + 5];
        file.append_frame(partial, 0).unwrap();
        let mut count = 0;
        let result = scan(&mut file, |_, _| {
            count += 1;
            Ok(())
        })
        .unwrap();
        assert_eq!(count, 1);
        assert!(result.tail_truncated);
        let _ = std::fs::remove_file(&tmp);
    }

    #[test]
    fn corrupt_checksum_in_middle_fails_hard() {
        let tmp = std::env::temp_dir().join(format!("minisqlite_rec_mid_{}", std::process::id()));
        let _ = std::fs::remove_file(&tmp);
        let mut file = DataFile::open_or_create(&tmp, Durability::Memory, false).unwrap();
        append_frame(&mut file, 1, Id::new().unwrap());
        let frame2 = make_frame(2, Id::new().unwrap());
        let frame2_bytes = frame2.encode();
        file.append_frame(&frame2_bytes, frame2.header.payload_length as u64)
            .unwrap();
        append_frame(&mut file, 3, Id::new().unwrap());

        // Corrupt one byte in the second frame's trailer. Because a third frame follows,
        // this is mid-file corruption and must fail hard.
        let frame2_offset =
            FILE_HEADER_SIZE as u64 + make_frame(1, Id::new().unwrap()).header.total_frame_length;
        let mut bytes = file.read_all().unwrap();
        let corrupt_offset = (frame2_offset as usize)
            + FRAME_HEADER_SIZE
            + frame2.header.payload_length as usize
            + 10;
        bytes[corrupt_offset] = bytes[corrupt_offset].wrapping_add(1);
        std::fs::write(&tmp, &bytes).unwrap();

        let mut file = DataFile::open_or_create(&tmp, Durability::Memory, false).unwrap();
        assert!(scan(&mut file, |_, _| Ok(())).is_err());
        let _ = std::fs::remove_file(&tmp);
    }

    #[test]
    fn corrupt_final_frame_checksum_fails_hard() {
        let tmp = std::env::temp_dir().join(format!("minisqlite_rec_final_{}", std::process::id()));
        let _ = std::fs::remove_file(&tmp);
        let mut file = DataFile::open_or_create(&tmp, Durability::Memory, false).unwrap();
        append_frame(&mut file, 1, Id::new().unwrap());
        append_frame(&mut file, 2, Id::new().unwrap());

        // Corrupt one byte in the final frame's trailer. The frame is fully present and
        // synced, so this is bitrot/corruption and must fail without truncation.
        let frame2_offset =
            FILE_HEADER_SIZE as u64 + make_frame(1, Id::new().unwrap()).header.total_frame_length;
        let mut bytes = file.read_all().unwrap();
        let corrupt_offset = (frame2_offset as usize)
            + FRAME_HEADER_SIZE
            + make_frame(2, Id::new().unwrap()).header.payload_length as usize
            + 10;
        bytes[corrupt_offset] = bytes[corrupt_offset].wrapping_add(1);
        std::fs::write(&tmp, &bytes).unwrap();

        let mut file = DataFile::open_or_create(&tmp, Durability::Memory, false).unwrap();
        assert!(scan(&mut file, |_, _| Ok(())).is_err());
        let _ = std::fs::remove_file(&tmp);
    }

    #[test]
    fn sequence_regression_fails() {
        let tmp = std::env::temp_dir().join(format!("minisqlite_rec_seq_{}", std::process::id()));
        let _ = std::fs::remove_file(&tmp);
        let mut file = DataFile::open_or_create(&tmp, Durability::Memory, false).unwrap();
        append_frame(&mut file, 1, Id::new().unwrap());
        append_frame(&mut file, 1, Id::new().unwrap());
        // Replay (not just scan) enforces monotonic transaction sequence.
        assert!(crate::StoreBuilder::new(&tmp).open().is_err());
        let _ = std::fs::remove_file(&tmp);
    }

    #[test]
    fn duplicate_event_id_in_committed_history_fails() {
        let tmp = std::env::temp_dir().join(format!("minisqlite_rec_dup_{}", std::process::id()));
        let _ = std::fs::remove_file(&tmp);
        let mut file = DataFile::open_or_create(&tmp, Durability::Memory, false).unwrap();
        let e = event_record(1, 1);
        let payload = encode_records(std::slice::from_ref(&e));
        let header = FrameHeader {
            version: 1,
            total_frame_length: 0,
            transaction_sequence: 1,
            transaction_id: Id::new().unwrap(),
            commit_timestamp_ms: 1,
            record_count: 1,
            payload_length: payload.len() as u32,
        };
        let frame = Frame::new(header, payload);
        file.append_frame(&frame.encode(), frame.header.payload_length as u64)
            .unwrap();

        // Second frame reuses the same event ID.
        let payload2 = encode_records(&[e]);
        let header2 = FrameHeader {
            version: 1,
            total_frame_length: 0,
            transaction_sequence: 2,
            transaction_id: Id::new().unwrap(),
            commit_timestamp_ms: 2,
            record_count: 1,
            payload_length: payload2.len() as u32,
        };
        let frame2 = Frame::new(header2, payload2);
        file.append_frame(&frame2.encode(), frame2.header.payload_length as u64)
            .unwrap();

        assert!(crate::StoreBuilder::new(&tmp).open().is_err());
        let _ = std::fs::remove_file(&tmp);
    }

    #[test]
    fn rejects_frame_larger_than_max() {
        let tmp = std::env::temp_dir().join(format!("minisqlite_rec_max_{}", std::process::id()));
        let _ = std::fs::remove_file(&tmp);

        // Start with a valid file header.
        let header = FileHeader::new(0);
        let mut file = std::fs::OpenOptions::new()
            .create(true)
            .truncate(true)
            .write(true)
            .open(&tmp)
            .unwrap();
        std::io::Write::write_all(&mut file, &header.encode()).unwrap();

        // Append a frame whose header declares a size above the hard limit.
        let payload = encode_records(&[event_record(1, 1)]);
        let header = FrameHeader {
            version: 1,
            total_frame_length: (MAX_FRAME_SIZE + 1) as u64,
            transaction_sequence: 1,
            transaction_id: Id::new().unwrap(),
            commit_timestamp_ms: 1,
            record_count: 1,
            payload_length: payload.len() as u32,
        };
        let header_bytes = header.encode_without_checksum();
        let header_checksum = crc32(&[&header_bytes]);

        let mut trailer_body = Writer::with_capacity(FRAME_TRAILER_SIZE - 8);
        trailer_body.bytes.extend_from_slice(TRAILER_MAGIC);
        trailer_body.write_u64(header.transaction_sequence);
        trailer_body.write_u64(header.total_frame_length);

        let computed = crc32(&[&header_bytes, &payload, &trailer_body.bytes]);
        let trailer = crate::codec::frame::FrameTrailer {
            transaction_sequence: header.transaction_sequence,
            total_frame_length: header.total_frame_length,
            checksum: computed,
        };

        let mut frame_bytes = header_bytes.to_vec();
        frame_bytes.extend_from_slice(&header_checksum.to_le_bytes());
        frame_bytes.extend_from_slice(&payload);
        frame_bytes.extend_from_slice(&trailer.encode());

        std::io::Write::write_all(&mut file, &frame_bytes).unwrap();
        drop(file);

        let mut file = DataFile::open_or_create(&tmp, Durability::Memory, false).unwrap();
        assert!(scan(&mut file, |_, _| Ok(())).is_err());
        let _ = std::fs::remove_file(&tmp);
    }

    #[test]
    fn streaming_recovery_processes_many_frames_without_accumulating() {
        let tmp =
            std::env::temp_dir().join(format!("minisqlite_rec_stream_{}", std::process::id()));
        let _ = std::fs::remove_file(&tmp);
        let mut file = DataFile::open_or_create(&tmp, Durability::Memory, false).unwrap();
        for i in 1..=100 {
            append_frame(&mut file, i, Id::new().unwrap());
        }

        // The scan callback receives frames one at a time. `ScanResult` does not carry a
        // `frames` vector, so recovery memory use is bounded by one frame.
        let mut count = 0;
        let result = scan(&mut file, |_, _| {
            count += 1;
            Ok(())
        })
        .unwrap();
        assert_eq!(count, 100);
        assert!(!result.tail_truncated);
        let _ = std::fs::remove_file(&tmp);
    }
}
