#![cfg(feature = "fuzzing")]

use std::io::Write;
use std::time::{SystemTime, UNIX_EPOCH};

use minisqlite::codec::frame::{FileHeader, Frame, FrameHeader, FRAME_FORMAT_VERSION};
use minisqlite::codec::record::{encode_records, Record};
use minisqlite::{EffectMode, Id, StoreBuilder};

mod common;

fn now_ms() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis() as i64)
        .unwrap_or(0)
}

fn fmt_result<T>(r: Result<T, minisqlite::Error>) -> Result<(), String> {
    r.map(|_| ()).map_err(|e| e.to_string())
}

fn write_frame(
    path: &std::path::Path,
    transaction_id: Id,
    records: &[Record],
    record_count_override: u32,
) {
    let _ = std::fs::remove_file(path);
    let mut file = std::fs::OpenOptions::new()
        .create(true)
        .truncate(true)
        .write(true)
        .open(path)
        .unwrap();
    file.write_all(&FileHeader::new(0).encode()).unwrap();

    let payload = encode_records(records);
    let header = FrameHeader {
        version: FRAME_FORMAT_VERSION,
        total_frame_length: 0,
        transaction_sequence: 1,
        transaction_id,
        commit_timestamp_ms: now_ms(),
        record_count: record_count_override,
        payload_length: payload.len() as u32,
    };
    let frame = Frame::new(header, payload);
    file.write_all(&frame.encode()).unwrap();
    drop(file);
}

#[test]
fn replay_rejects_zero_transaction_id() {
    let tmp = common::TempDir::new();
    let path = tmp.path().join("zero_tx_id.mini");

    let records = [Record::TransactionMeta {
        correlation_id: None,
        metadata: Vec::new(),
    }];
    write_frame(&path, Id::ZERO, &records, 1);

    let result = StoreBuilder::new(&path).open();
    assert!(
        result.is_err(),
        "replay must reject a zero transaction_id: {result:?}",
        result = fmt_result(result),
    );
}

#[test]
fn replay_rejects_zero_job_id() {
    let tmp = common::TempDir::new();
    let path = tmp.path().join("zero_job_id.mini");

    let records = [Record::JobEnqueue {
        job_id: Id::ZERO,
        queue: "q".into(),
        partition: "p".into(),
        payload: b"payload".to_vec(),
        not_before_ms: 0,
        max_attempts: 1,
        effect_mode: EffectMode::Idempotent,
        idempotency_key: None,
    }];
    write_frame(&path, Id::new().unwrap(), &records, 1);

    let result = StoreBuilder::new(&path).open();
    assert!(
        result.is_err(),
        "replay must reject a zero job id: {result:?}",
        result = fmt_result(result)
    );
}

#[test]
fn replay_rejects_zero_max_attempts() {
    let tmp = common::TempDir::new();
    let path = tmp.path().join("zero_max_attempts.mini");

    let records = [Record::JobEnqueue {
        job_id: Id::new().unwrap(),
        queue: "q".into(),
        partition: "p".into(),
        payload: b"payload".to_vec(),
        not_before_ms: 0,
        max_attempts: 0,
        effect_mode: EffectMode::Idempotent,
        idempotency_key: None,
    }];
    write_frame(&path, Id::new().unwrap(), &records, 1);

    let result = StoreBuilder::new(&path).open();
    assert!(
        result.is_err(),
        "replay must reject max_attempts == 0: {result:?}",
        result = fmt_result(result),
    );
}

#[test]
fn replay_rejects_zero_lease_token() {
    let tmp = common::TempDir::new();
    let path = tmp.path().join("zero_lease_token.mini");

    let job_id = Id::new().unwrap();
    let records = [
        Record::JobEnqueue {
            job_id,
            queue: "q".into(),
            partition: "p".into(),
            payload: b"payload".to_vec(),
            not_before_ms: 0,
            max_attempts: 3,
            effect_mode: EffectMode::Idempotent,
            idempotency_key: None,
        },
        Record::JobLease {
            job_id,
            lease_token: Id::ZERO,
            worker_id: "w".into(),
            attempt: 1,
            lease_expires_at_ms: 100,
            claimed_at_ms: 0,
        },
    ];
    write_frame(&path, Id::new().unwrap(), &records, 2);

    let result = StoreBuilder::new(&path).open();
    assert!(
        result.is_err(),
        "replay must reject a zero lease token: {result:?}",
        result = fmt_result(result),
    );
}

#[test]
fn replay_rejects_non_sequential_attempt() {
    let tmp = common::TempDir::new();
    let path = tmp.path().join("bad_attempt.mini");

    let job_id = Id::new().unwrap();
    let records = [
        Record::JobEnqueue {
            job_id,
            queue: "q".into(),
            partition: "p".into(),
            payload: b"payload".to_vec(),
            not_before_ms: 0,
            max_attempts: 3,
            effect_mode: EffectMode::Idempotent,
            idempotency_key: None,
        },
        Record::JobLease {
            job_id,
            lease_token: Id::new().unwrap(),
            worker_id: "w".into(),
            attempt: 7, // not previous + 1
            lease_expires_at_ms: 100,
            claimed_at_ms: 0,
        },
    ];
    write_frame(&path, Id::new().unwrap(), &records, 2);

    let result = StoreBuilder::new(&path).open();
    assert!(
        result.is_err(),
        "replay must reject a non-sequential attempt number: {result:?}",
        result = fmt_result(result),
    );
}

#[test]
fn replay_rejects_attempt_above_max_attempts() {
    let tmp = common::TempDir::new();
    let path = tmp.path().join("attempt_ceiling.mini");

    let job_id = Id::new().unwrap();
    let records = [
        Record::JobEnqueue {
            job_id,
            queue: "q".into(),
            partition: "p".into(),
            payload: b"payload".to_vec(),
            not_before_ms: 0,
            max_attempts: 1,
            effect_mode: EffectMode::Idempotent,
            idempotency_key: None,
        },
        Record::JobLease {
            job_id,
            lease_token: Id::new().unwrap(),
            worker_id: "w".into(),
            attempt: 1,
            lease_expires_at_ms: 100,
            claimed_at_ms: 0,
        },
        Record::JobLease {
            job_id,
            lease_token: Id::new().unwrap(),
            worker_id: "w".into(),
            attempt: 2, // exceeds max_attempts
            lease_expires_at_ms: 200,
            claimed_at_ms: 100,
        },
    ];
    write_frame(&path, Id::new().unwrap(), &records, 3);

    let result = StoreBuilder::new(&path).open();
    assert!(
        result.is_err(),
        "replay must reject attempt > max_attempts: {result:?}",
        result = fmt_result(result),
    );
}

#[test]
fn replay_rejects_lease_expiry_not_after_claim_time() {
    let tmp = common::TempDir::new();
    let path = tmp.path().join("lease_order.mini");

    let job_id = Id::new().unwrap();
    let records = [
        Record::JobEnqueue {
            job_id,
            queue: "q".into(),
            partition: "p".into(),
            payload: b"payload".to_vec(),
            not_before_ms: 0,
            max_attempts: 3,
            effect_mode: EffectMode::Idempotent,
            idempotency_key: None,
        },
        Record::JobLease {
            job_id,
            lease_token: Id::new().unwrap(),
            worker_id: "w".into(),
            attempt: 1,
            lease_expires_at_ms: 0, // not > claimed_at_ms
            claimed_at_ms: 0,
        },
    ];
    write_frame(&path, Id::new().unwrap(), &records, 2);

    let result = StoreBuilder::new(&path).open();
    assert!(
        result.is_err(),
        "replay must reject lease_expires_at_ms <= claimed_at_ms: {result:?}",
        result = fmt_result(result),
    );
}

#[test]
fn replay_rejects_duplicate_job_id_with_different_spec() {
    let tmp = common::TempDir::new();
    let path = tmp.path().join("dup_job.mini");

    let job_id = Id::new().unwrap();
    let records = [
        Record::JobEnqueue {
            job_id,
            queue: "q".into(),
            partition: "p".into(),
            payload: b"first".to_vec(),
            not_before_ms: 0,
            max_attempts: 3,
            effect_mode: EffectMode::Idempotent,
            idempotency_key: None,
        },
        Record::JobEnqueue {
            job_id,
            queue: "q".into(),
            partition: "p".into(),
            payload: b"second".to_vec(),
            not_before_ms: 0,
            max_attempts: 3,
            effect_mode: EffectMode::Idempotent,
            idempotency_key: None,
        },
    ];
    write_frame(&path, Id::new().unwrap(), &records, 2);

    let result = StoreBuilder::new(&path).open();
    assert!(
        result.is_err(),
        "replay must reject duplicate job id with different spec: {result:?}",
        result = fmt_result(result),
    );
}

#[test]
fn decode_records_rejects_huge_record_count_without_oom() {
    let tmp = common::TempDir::new();
    let path = tmp.path().join("huge_count.mini");

    let records = [];
    write_frame(&path, Id::new().unwrap(), &records, u32::MAX);

    let result = StoreBuilder::new(&path).open();
    assert!(
        result.is_err(),
        "huge record count must be rejected before allocation: {result:?}",
        result = fmt_result(result),
    );
}
