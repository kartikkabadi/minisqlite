#![cfg(feature = "fuzzing")]

use std::fs::OpenOptions;
use std::io::Write;

use minisqlite::codec::encode_records;
use minisqlite::codec::frame::{Frame, FrameHeader, FILE_HEADER_SIZE, FRAME_FORMAT_VERSION};
use minisqlite::codec::record::{decode_records, Record, MAX_RECORDS_PER_FRAME};
use minisqlite::config::{Durability, EffectMode, Limits};
use minisqlite::storage::file::DataFile;
use minisqlite::{ClaimRequest, CommitBatch, Event, Id, JobSpec, StoreBuilder};

mod common;

fn tmp_dir() -> common::TempDir {
    common::TempDir::new()
}

fn append_frame(path: &std::path::Path, header: FrameHeader, records: &[Record]) {
    let payload = encode_records(records);
    let mut header = header;
    header.payload_length = payload.len() as u32;
    header.record_count = records.len() as u32;
    let frame = Frame::new(header, payload);
    let mut file = OpenOptions::new().append(true).open(path).unwrap();
    file.write_all(&frame.encode()).unwrap();
}

#[test]
fn limits_rejects_max_records_above_hard_frame_ceiling() {
    let tmp = tmp_dir();
    let path = tmp.path().join("limits.mini");
    let limits = Limits {
        max_records_per_transaction: (MAX_RECORDS_PER_FRAME as usize) + 1,
        ..Default::default()
    };
    let result = StoreBuilder::new(&path).limits(limits).open();
    assert!(
        matches!(result, Err(minisqlite::Error::Validation(ref msg)) if msg.contains("hard frame record ceiling")),
        "expected Validation error for max_records above hard ceiling: {}",
        result
            .map(|_| "")
            .map_err(|e| e.to_string())
            .unwrap_or_default()
    );
    assert!(
        !path.exists(),
        "no file should be created for invalid limits"
    );
}

#[test]
fn decode_records_bounds_allocation_by_payload_geometry() {
    let empty_meta = Record::TransactionMeta {
        correlation_id: None,
        metadata: vec![],
    }
    .encode();
    let min_size = empty_meta.len();

    // Empty payload with a non-zero count is rejected before allocating.
    let err = decode_records(&[], 1).unwrap_err();
    assert!(
        matches!(err, minisqlite::Error::Corruption { .. }),
        "expected Corruption for count that cannot fit: {err:?}"
    );

    // MAX_RECORDS_PER_FRAME with an empty payload is rejected immediately.
    let err = decode_records(&[], MAX_RECORDS_PER_FRAME).unwrap_err();
    assert!(matches!(err, minisqlite::Error::Corruption { .. }));

    // A payload that could hold two records but claims three is rejected.
    let payload = empty_meta.repeat(2);
    let err = decode_records(&payload, 3).unwrap_err();
    assert!(matches!(err, minisqlite::Error::Corruption { .. }));

    // A payload that could hold exactly two records accepts two.
    let records = decode_records(&payload, 2).unwrap();
    assert_eq!(records.len(), 2);
    assert_eq!(records[0], empty_meta_record());
    assert_eq!(records[1], empty_meta_record());

    // Sanity: the hard ceiling still protects against malicious large counts.
    let mut huge = empty_meta.repeat(10);
    let err = decode_records(&huge, MAX_RECORDS_PER_FRAME).unwrap_err();
    assert!(matches!(err, minisqlite::Error::Corruption { .. }));

    // MAX_RECORDS_PER_FRAME with an exactly fitting payload succeeds.
    huge.clear();
    huge.reserve((MAX_RECORDS_PER_FRAME as usize) * min_size);
    for _ in 0..MAX_RECORDS_PER_FRAME {
        huge.extend_from_slice(&empty_meta);
    }
    let records = decode_records(&huge, MAX_RECORDS_PER_FRAME).unwrap();
    assert_eq!(records.len() as u32, MAX_RECORDS_PER_FRAME);
}

fn empty_meta_record() -> Record {
    Record::TransactionMeta {
        correlation_id: None,
        metadata: vec![],
    }
}

#[test]
fn verify_rejects_torn_tail_as_store_needs_repair() {
    let tmp = tmp_dir();
    let path = tmp.path().join("torn.mini");
    let store = StoreBuilder::new(&path)
        .durability(Durability::Memory)
        .open()
        .unwrap();
    store
        .commit(
            CommitBatch::new(Id::new().unwrap(), 0).append_event(Event::with_json_payload(
                Id::new().unwrap(),
                "s",
                "e",
                0,
                b"{}",
            )),
        )
        .unwrap();
    drop(store);

    // Truncate the last few bytes of the only frame to create a torn tail.
    let len = std::fs::metadata(&path).unwrap().len();
    let file = std::fs::OpenOptions::new().write(true).open(&path).unwrap();
    file.set_len(len - 8).unwrap();
    drop(file);

    // Verify must not modify the file and must report the torn tail.
    let size_before = std::fs::metadata(&path).unwrap().len();
    let result = StoreBuilder::new(&path).verify();
    assert!(
        matches!(result, Err(minisqlite::Error::StoreNeedsRepair)),
        "verify should report StoreNeedsRepair for torn tail: {result:?}"
    );
    assert_eq!(
        size_before,
        std::fs::metadata(&path).unwrap().len(),
        "verify must not modify a torn tail"
    );
}

#[test]
fn verify_rejects_semantic_corruption_with_frame_offset() {
    let tmp = tmp_dir();
    let path = tmp.path().join("semantic.mini");
    let store = StoreBuilder::new(&path)
        .durability(Durability::Memory)
        .open()
        .unwrap();
    store
        .commit(CommitBatch::new(Id::new().unwrap(), 0).projection_put(
            "p",
            1,
            b"k".to_vec(),
            b"v".to_vec(),
        ))
        .unwrap();
    drop(store);

    let first_frame_end = std::fs::metadata(&path).unwrap().len();

    // Append a second frame that jumps the projection version from 1 to 3.
    let header = FrameHeader {
        version: FRAME_FORMAT_VERSION,
        total_frame_length: 0,
        transaction_sequence: 2,
        transaction_id: Id::new().unwrap(),
        commit_timestamp_ms: 0,
        record_count: 1,
        payload_length: 0,
    };
    append_frame(
        &path,
        header,
        &[Record::ProjectionPut {
            projection: "p".into(),
            version: 3,
            key: b"k".to_vec(),
            value: b"v2".to_vec(),
        }],
    );

    let result = StoreBuilder::new(&path).verify();
    match result {
        Err(minisqlite::Error::Corruption { offset, message }) => {
            assert_eq!(
                offset, first_frame_end,
                "corruption must report second frame offset"
            );
            assert!(
                message.contains("version mismatch"),
                "expected version mismatch message, got: {message}"
            );
        }
        other => panic!("expected Corruption at second frame offset: {other:?}"),
    }
}

#[test]
fn replay_wraps_immutable_invariant_errors_as_corruption_with_offset() {
    let tmp = tmp_dir();
    let path = tmp.path().join("invariant.mini");
    let store = StoreBuilder::new(&path)
        .durability(Durability::Memory)
        .open()
        .unwrap();
    store
        .commit(
            CommitBatch::new(Id::new().unwrap(), 0).append_event(Event::with_json_payload(
                Id::new().unwrap(),
                "s",
                "e",
                0,
                b"{}",
            )),
        )
        .unwrap();
    drop(store);

    let first_frame_end = std::fs::metadata(&path).unwrap().len();
    let header = FrameHeader {
        version: FRAME_FORMAT_VERSION,
        total_frame_length: 0,
        transaction_sequence: 2,
        transaction_id: Id::new().unwrap(),
        commit_timestamp_ms: 0,
        record_count: 1,
        payload_length: 0,
    };
    append_frame(
        &path,
        header,
        &[Record::JobEnqueue {
            job_id: Id::new().unwrap(),
            queue: "q".into(),
            partition: "p".into(),
            payload: vec![],
            not_before_ms: 0,
            max_attempts: 0,
            effect_mode: EffectMode::Idempotent,
            idempotency_key: None,
        }],
    );

    let result = StoreBuilder::new(&path).verify();
    match result {
        Err(minisqlite::Error::Corruption { offset, message }) => {
            assert_eq!(
                offset, first_frame_end,
                "corruption must report second frame offset"
            );
            assert!(
                message.contains("max_attempts must be greater than 0"),
                "expected max_attempts error, got: {message}"
            );
        }
        other => panic!("expected Corruption at second frame offset: {other:?}"),
    }
}

#[test]
fn claim_jobs_exact_lease_fits_minimum_160_byte_frame() {
    let tmp = tmp_dir();
    let path = tmp.path().join("tiny_frame.mini");
    let limits = Limits {
        max_frame_size: 160,
        ..Default::default()
    };
    let store = StoreBuilder::new(&path)
        .durability(Durability::Memory)
        .limits(limits)
        .open()
        .unwrap();

    store
        .commit(
            CommitBatch::new(Id::new().unwrap(), 0).enqueue_job(JobSpec::new(
                Id::new().unwrap(),
                "q",
                "p",
                vec![],
            )),
        )
        .unwrap();

    let claimed = store
        .claim_jobs(ClaimRequest {
            queue: "q".into(),
            worker_id: "w".into(),
            limit: 1,
            lease_ms: 1,
            now_ms: 0,
        })
        .unwrap();
    assert_eq!(
        claimed.len(),
        1,
        "single ready job should fit in 160-byte frame"
    );
    let claimed_job = claimed.into_iter().next().unwrap();
    assert_eq!(claimed_job.partition, "p");

    // Close the store before re-reading the file; on Windows the advisory lock
    // on the primary file otherwise blocks the read.
    drop(store);

    // The lease transaction frame must be exactly the maximum frame size.
    let bytes = std::fs::read(&path).unwrap();
    let first = Frame::decode(&bytes[FILE_HEADER_SIZE..]).unwrap();
    let second_start = FILE_HEADER_SIZE + first.header.total_frame_length as usize;
    let second = Frame::decode(&bytes[second_start..]).unwrap();
    assert_eq!(
        second.header.total_frame_length, 160,
        "lease frame should be exactly 160 bytes"
    );
    let lease_record = Record::JobLease {
        job_id: claimed_job.job_id,
        lease_token: claimed_job.lease_token,
        worker_id: "w".into(),
        attempt: claimed_job.attempt,
        lease_expires_at_ms: claimed_job.lease_expires_at_ms,
        claimed_at_ms: 0,
    }
    .encode();
    assert_eq!(second.header.payload_length as usize, lease_record.len());
}

#[test]
fn claim_jobs_budgets_per_partition_and_makes_progress() {
    let tmp = tmp_dir();
    let path = tmp.path().join("fair.mini");
    let limits = Limits {
        max_records_per_transaction: 2,
        ..Default::default()
    };
    let store = StoreBuilder::new(&path)
        .durability(Durability::Memory)
        .limits(limits)
        .open()
        .unwrap();

    let a1 = Id::new().unwrap();
    let a2 = Id::new().unwrap();
    let b1 = Id::new().unwrap();
    let b2 = Id::new().unwrap();
    store
        .commit(
            CommitBatch::new(Id::new().unwrap(), 0)
                .enqueue_job(JobSpec::new(a1, "q", "a", b"a1".to_vec()).with_max_attempts(1))
                .enqueue_job(JobSpec::new(b1, "q", "b", b"b1".to_vec()).with_max_attempts(1)),
        )
        .unwrap();
    store
        .commit(
            CommitBatch::new(Id::new().unwrap(), 0)
                .enqueue_job(JobSpec::new(a2, "q", "a", b"a2".to_vec()).with_max_attempts(1))
                .enqueue_job(JobSpec::new(b2, "q", "b", b"b2".to_vec()).with_max_attempts(1)),
        )
        .unwrap();

    // First claim: lease a1 and b1.
    let first = store
        .claim_jobs(ClaimRequest {
            queue: "q".into(),
            worker_id: "w".into(),
            limit: 10,
            lease_ms: 1,
            now_ms: 0,
        })
        .unwrap();
    assert_eq!(first.len(), 2);

    // Second claim at now_ms=2: a1 and b1 leases have expired. With max_records=2,
    // per-partition progress must expire a1 and claim a2 in partition "a" before
    // being blocked by the expired job in partition "b".
    let second = store
        .claim_jobs(ClaimRequest {
            queue: "q".into(),
            worker_id: "w".into(),
            limit: 10,
            lease_ms: 1,
            now_ms: 2,
        })
        .unwrap();
    assert_eq!(second.len(), 1, "should claim a2 and stop at max_records");
    assert_eq!(second[0].partition, "a");
    assert_eq!(second[0].job_id, a2);

    // Third claim at now_ms=2 while A2's lease (expires at 3) is still active:
    // partition "a" is blocked, so the budget is free to expire b1 and claim b2.
    let third = store
        .claim_jobs(ClaimRequest {
            queue: "q".into(),
            worker_id: "w".into(),
            limit: 10,
            lease_ms: 1,
            now_ms: 2,
        })
        .unwrap();
    assert_eq!(
        third.len(),
        1,
        "should claim b2 while a-partition is blocked by active a2"
    );
    assert_eq!(third[0].partition, "b");
    assert_eq!(third[0].job_id, b2);
}

#[test]
fn backup_rejects_existing_destination() {
    let tmp = tmp_dir();
    let src = tmp.path().join("primary.mini");
    let dst = tmp.path().join("backup.mini");

    let store = StoreBuilder::new(&src)
        .durability(Durability::Memory)
        .open()
        .unwrap();
    store
        .commit(
            CommitBatch::new(Id::new().unwrap(), 0).append_event(Event::with_json_payload(
                Id::new().unwrap(),
                "s",
                "e",
                0,
                b"{}",
            )),
        )
        .unwrap();

    // Pre-create the destination.
    std::fs::write(&dst, b"do not overwrite").unwrap();
    let dst_len_before = std::fs::metadata(&dst).unwrap().len();
    let src_len_before = std::fs::metadata(&src).unwrap().len();

    let result = store.backup(&dst);
    assert!(
        result.is_err(),
        "backup must fail when destination already exists: {result:?}"
    );
    assert_eq!(
        std::fs::metadata(&dst).unwrap().len(),
        dst_len_before,
        "destination must not be overwritten"
    );
    assert_eq!(
        std::fs::metadata(&src).unwrap().len(),
        src_len_before,
        "primary must be unchanged"
    );

    // After removing the destination, backup should succeed atomically.
    std::fs::remove_file(&dst).unwrap();
    store.backup(&dst).unwrap();
    assert!(dst.exists());
    assert_eq!(
        std::fs::metadata(&dst).unwrap().len(),
        src_len_before,
        "backup should match primary size"
    );
}

#[cfg(unix)]
#[test]
fn backup_rejects_dangling_symlink_destination() {
    use std::os::unix::fs::symlink;

    let tmp = tmp_dir();
    let src = tmp.path().join("primary.mini");
    let link = tmp.path().join("backup_link.mini");

    let store = StoreBuilder::new(&src)
        .durability(Durability::Memory)
        .open()
        .unwrap();
    store
        .commit(
            CommitBatch::new(Id::new().unwrap(), 0).append_event(Event::with_json_payload(
                Id::new().unwrap(),
                "s",
                "e",
                0,
                b"{}",
            )),
        )
        .unwrap();

    // Create a dangling symlink as the destination.
    symlink("/minisqlite_nonexistent_backup", &link).unwrap();
    let src_len_before = std::fs::metadata(&src).unwrap().len();

    let result = store.backup(&link);
    assert!(
        result.is_err(),
        "backup must fail against a dangling symlink: {result:?}"
    );
    assert!(
        std::fs::symlink_metadata(&link).is_ok(),
        "dangling symlink must not be removed by failed backup"
    );
    assert_eq!(
        std::fs::metadata(&src).unwrap().len(),
        src_len_before,
        "primary must be unchanged"
    );

    std::fs::remove_file(&link).unwrap();
    store.backup(&link).unwrap();
    assert!(std::fs::metadata(&link).is_ok());
    assert!(!std::fs::symlink_metadata(&link)
        .unwrap()
        .file_type()
        .is_symlink());
}

#[test]
fn job_expire_rejects_non_idempotent_effect_mode_during_replay() {
    let tmp = tmp_dir();
    let path = tmp.path().join("expire.mini");
    let store = StoreBuilder::new(&path)
        .durability(Durability::Memory)
        .open()
        .unwrap();

    let job_id = Id::new().unwrap();
    store
        .commit(
            CommitBatch::new(Id::new().unwrap(), 0).enqueue_job(
                JobSpec::new(job_id, "q", "p", b"work".to_vec())
                    .with_max_attempts(1)
                    .with_effect_mode(EffectMode::UncertainOnLeaseExpiry),
            ),
        )
        .unwrap();

    let claimed = store
        .claim_jobs(ClaimRequest {
            queue: "q".into(),
            worker_id: "w".into(),
            limit: 1,
            lease_ms: 1,
            now_ms: 0,
        })
        .unwrap();
    assert_eq!(claimed.len(), 1);
    let claimed = &claimed[0];

    drop(store);

    // The file contains an Enqueue frame (seq 1) and a Lease frame (seq 2).
    let first_frame_end = std::fs::metadata(&path).unwrap().len();
    let header = FrameHeader {
        version: FRAME_FORMAT_VERSION,
        total_frame_length: 0,
        transaction_sequence: 3,
        transaction_id: Id::new().unwrap(),
        commit_timestamp_ms: 0,
        record_count: 1,
        payload_length: 0,
    };
    append_frame(
        &path,
        header,
        &[Record::JobExpire {
            job_id,
            lease_token: claimed.lease_token,
            attempt: claimed.attempt,
            expired_at_ms: claimed.lease_expires_at_ms,
        }],
    );

    let result = StoreBuilder::new(&path).verify();
    match result {
        Err(minisqlite::Error::Corruption { offset, message }) => {
            assert_eq!(
                offset, first_frame_end,
                "corruption must report JobExpire frame offset"
            );
            assert!(
                message.contains("cannot expire a non-idempotent effect mode"),
                "expected non-idempotent expire error, got: {message}"
            );
        }
        other => panic!("expected Corruption for non-idempotent JobExpire: {other:?}"),
    }
}

#[test]
#[cfg(feature = "failpoint")]
fn truncate_reports_repair_outcome_uncertain_after_set_len_before_sync() {
    use std::sync::Mutex;

    static LOCK: Mutex<()> = Mutex::new(());

    let tmp = tmp_dir();
    let path = tmp.path().join("truncate.mini");

    let _guard = LOCK.lock().unwrap();
    std::env::set_var("MINISQLITE_FAILPOINT", "truncate-before-sync");
    let result = std::panic::catch_unwind(|| {
        let mut df = DataFile::open_or_create(&path, Durability::Memory, false).unwrap();
        df.truncate(64).unwrap_err()
    });
    std::env::remove_var("MINISQLITE_FAILPOINT");
    drop(_guard);

    let err = result.expect("test closure should not panic");
    match err {
        minisqlite::Error::RepairOutcomeUncertain { requested, actual } => {
            assert_eq!(requested, 64);
            assert_eq!(
                actual, 64,
                "set_len succeeded, so actual length must match request"
            );
        }
        other => panic!("expected RepairOutcomeUncertain: {other:?}"),
    }

    // A second truncate without the failpoint should succeed and sync.
    let mut df = DataFile::open_or_create(&path, Durability::Memory, false).unwrap();
    df.truncate(64).unwrap();
    assert_eq!(df.file_len(), 64);
}

// Ensure the constant used in the 160-byte claim test is still the default ceiling.
#[test]
fn max_frame_size_default_is_at_least_160() {
    assert!(Limits::default().max_frame_size >= 160);
}
