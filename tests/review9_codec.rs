//! Review #9 codec regression tests.
//!
//! Runs frame decoding in a child process whose address space is capped with
//! RLIMIT_AS, proving that decoding valid frames at or near the 64 MiB frame
//! ceiling succeeds within a bounded memory budget instead of aborting on an
//! unbounded allocation.
#![cfg(all(target_os = "linux", feature = "fuzzing"))]

use std::process::Command;

use minisqlite::codec::frame::{
    Frame, FrameHeader, FRAME_FORMAT_VERSION, FRAME_HEADER_SIZE, FRAME_TRAILER_SIZE, MAX_FRAME_SIZE,
};
use minisqlite::codec::record::{
    decode_records, encode_records, EventRecord, Record, MAX_TRANSACTION_MEMORY,
};
use minisqlite::Id;

const CHILD_ENV: &str = "REVIEW9_CODEC_CHILD";

/// Address-space cap for the child during decode. Large enough for the frame
/// bytes plus the decoded payload copies, small enough to fail if decoding
/// regresses to unbounded or duplicated full-frame allocations.
const CHILD_RLIMIT_AS_BYTES: u64 = 1 << 30; // 1 GiB

fn event_record_with_payload(payload_len: usize) -> Record {
    Record::Event(EventRecord {
        global_sequence: 1,
        stream_version: 1,
        event_id: Id::from(7u128),
        stream_id: "stream".into(),
        event_type: "event".into(),
        schema_version: 1,
        occurred_at_ms: 0,
        causation_id: None,
        correlation_id: None,
        payload: vec![0xAB; payload_len],
        metadata: vec![],
    })
}

/// Build a valid single-record frame whose total encoded size is exactly
/// `total_size` bytes.
fn frame_of_total_size(total_size: usize) -> Vec<u8> {
    let target_payload = total_size - FRAME_HEADER_SIZE - FRAME_TRAILER_SIZE;
    // Measure the encoding overhead of an empty-payload record, then size the
    // event payload so the frame payload hits the target exactly.
    let overhead = encode_records(&[event_record_with_payload(0)])
        .unwrap()
        .len();
    let record = event_record_with_payload(target_payload - overhead);
    let payload = encode_records(std::slice::from_ref(&record)).unwrap();
    assert_eq!(payload.len(), target_payload);
    let header = FrameHeader {
        version: FRAME_FORMAT_VERSION,
        total_frame_length: 0,
        transaction_sequence: 1,
        transaction_id: Id::from(1u128),
        commit_timestamp_ms: 0,
        record_count: 1,
        payload_length: 0,
    };
    let frame = Frame::new(header, payload);
    let bytes = frame.encode();
    assert_eq!(bytes.len(), total_size);
    bytes
}

fn limit_address_space(bytes: u64) {
    let lim = libc::rlimit {
        rlim_cur: bytes,
        rlim_max: bytes,
    };
    let rc = unsafe { libc::setrlimit(libc::RLIMIT_AS, &lim) };
    assert_eq!(rc, 0, "setrlimit(RLIMIT_AS) failed");
}

fn decode_frame_and_records(bytes: &[u8]) {
    let frame = Frame::decode(bytes).expect("valid frame must decode");
    let records =
        decode_records(&frame.payload, frame.header.record_count).expect("records must decode");
    assert_eq!(records.len(), 1);
}

fn run_child(mode: &str) {
    let exe = std::env::current_exe().unwrap();
    let output = Command::new(exe)
        .env(CHILD_ENV, mode)
        .args(["--exact", "child_entry", "--ignored", "--nocapture"])
        .output()
        .expect("failed to spawn child test process");
    assert!(
        output.status.success(),
        "child decode under RLIMIT_AS failed (mode={mode}): status={:?}\nstdout: {}\nstderr: {}",
        output.status,
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr),
    );
}

/// Child body: builds the frame, then caps its own address space before
/// decoding. Only runs when spawned by the parent tests below.
#[test]
#[ignore]
fn child_entry() {
    let mode = match std::env::var(CHILD_ENV) {
        Ok(m) => m,
        Err(_) => return,
    };
    let total_size = match mode.as_str() {
        "exact" => MAX_FRAME_SIZE,
        "near" => MAX_FRAME_SIZE - 4096,
        other => panic!("unknown child mode {other}"),
    };
    let bytes = frame_of_total_size(total_size);
    limit_address_space(CHILD_RLIMIT_AS_BYTES);
    decode_frame_and_records(&bytes);
}

#[test]
fn constrained_rss_decodes_exact_cap_frame() {
    run_child("exact");
}

#[test]
fn constrained_rss_decodes_near_max_frame() {
    run_child("near");
}

#[test]
fn transaction_memory_ceiling_rejects_metadata_amplification() {
    // Each maximal ProjectionReplace record encodes ~8 MiB but costs ~48 MiB
    // of decoded tuple metadata. Enough of them exceed MAX_TRANSACTION_MEMORY
    // and must be rejected instead of ballooning replay memory.
    let per_record_cost = 1_000_000 * std::mem::size_of::<(Vec<u8>, Vec<u8>)>();
    let record_count = MAX_TRANSACTION_MEMORY / per_record_cost + 2;
    let records: Vec<Record> = (0..record_count)
        .map(|_| Record::ProjectionReplace {
            projection: "p".into(),
            new_version: 1,
            entries: vec![(vec![], vec![]); 1_000_000],
        })
        .collect();
    let payload = encode_records(&records).unwrap();
    assert!(payload.len() <= MAX_FRAME_SIZE - FRAME_HEADER_SIZE - FRAME_TRAILER_SIZE);
    let err = decode_records(&payload, record_count as u32).unwrap_err();
    assert!(
        matches!(err, minisqlite::Error::Corruption { .. }),
        "expected Corruption, got {err:?}"
    );
}

#[test]
fn frame_one_byte_over_cap_is_rejected() {
    // A frame header declaring MAX_FRAME_SIZE + 1 must be rejected before any
    // payload allocation is attempted.
    let bytes = frame_of_total_size(MAX_FRAME_SIZE);
    let mut header_bytes: [u8; FRAME_HEADER_SIZE] = bytes[..FRAME_HEADER_SIZE].try_into().unwrap();
    let too_big = (MAX_FRAME_SIZE as u64 + 1).to_le_bytes();
    header_bytes[10..18].copy_from_slice(&too_big);
    let mut tampered = bytes;
    tampered[..FRAME_HEADER_SIZE].copy_from_slice(&header_bytes);
    assert!(Frame::decode(&tampered).is_err());
}
