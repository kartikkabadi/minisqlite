use std::io::{BufRead, Write};
use std::path::PathBuf;
use std::process::{Command, Stdio};
use std::time::{SystemTime, UNIX_EPOCH};

use minisqlite::{
    ClaimRequest, CommitBatch, Durability, EffectMode, Event, Id, JobSpec, Resolution, StoreBuilder,
};

mod common;

fn lock_holder_bin() -> PathBuf {
    std::env::var("CARGO_BIN_EXE_lock_holder")
        .map(PathBuf::from)
        .unwrap_or_else(|_| PathBuf::from("target/debug/lock_holder"))
}

fn now_ms() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis() as i64)
        .unwrap_or(0)
}

#[test]
fn same_process_second_open_is_rejected() {
    let tmp = common::TempDir::new();
    let path = tmp.path().join("locked.mini");
    let _store = StoreBuilder::new(&path)
        .durability(Durability::Memory)
        .open()
        .unwrap();

    let result = StoreBuilder::new(&path).open();
    assert!(
        matches!(result, Err(minisqlite::Error::AlreadyOpen)),
        "second open in the same process should be rejected"
    );
}

#[test]
fn second_process_open_is_rejected() {
    let tmp = common::TempDir::new();
    let path = tmp.path().join("cross_process.mini");

    let mut child = Command::new(lock_holder_bin())
        .arg(&path)
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .spawn()
        .expect("lock holder failed to start");

    let stdout = child.stdout.take().unwrap();
    let mut reader = std::io::BufReader::new(stdout);
    let mut line = String::new();
    reader
        .read_line(&mut line)
        .expect("lock holder did not report lock");
    assert_eq!(line.trim(), "LOCKED");

    let result = StoreBuilder::new(&path).open();
    assert!(
        matches!(result, Err(minisqlite::Error::AlreadyOpen)),
        "cross-process second open should be rejected"
    );

    let mut stdin = child.stdin.take().unwrap();
    stdin.write_all(b"\n").unwrap();
    let status = child.wait().expect("lock holder did not exit");
    assert!(status.success());
}

#[test]
fn max_attempts_caps_expired_idempotent_lease() {
    let tmp = common::TempDir::new();
    let path = tmp.path().join("max_attempts.mini");
    let store = StoreBuilder::new(&path)
        .durability(Durability::Memory)
        .open()
        .unwrap();

    let job_id = Id::from(1u128);
    let job = JobSpec::new(job_id, "q", "p", b"payload".to_vec()).with_max_attempts(1);
    store
        .commit(CommitBatch::new(Id::new(), now_ms()).enqueue_job(job))
        .unwrap();

    let now = now_ms();
    let claimed = store
        .claim_jobs(ClaimRequest {
            queue: "q".into(),
            worker_id: "w1".into(),
            now_ms: now,
            lease_ms: 1000,
            limit: 1,
        })
        .unwrap();
    assert_eq!(claimed.len(), 1);
    let token = claimed[0].lease_token;

    store
        .commit(CommitBatch::new(Id::new(), now + 1).fail_job(job_id, token, "boom", None))
        .unwrap();

    let later = now + 10_000;
    let re_claimed = store
        .claim_jobs(ClaimRequest {
            queue: "q".into(),
            worker_id: "w2".into(),
            now_ms: later,
            lease_ms: 1000,
            limit: 1,
        })
        .unwrap();
    assert!(
        re_claimed.is_empty(),
        "expired idempotent lease must not be retried past max_attempts"
    );
}

#[test]
fn terminal_job_fail_is_idempotent_across_reopen() {
    let tmp = common::TempDir::new();
    let path = tmp.path().join("terminal_fail_idempotent.mini");
    let store = StoreBuilder::new(&path)
        .durability(Durability::Strict)
        .open()
        .unwrap();

    let job_id = Id::from(2u128);
    let job = JobSpec::new(job_id, "q", "p", b"payload".to_vec()).with_max_attempts(1);
    store
        .commit(CommitBatch::new(Id::new(), now_ms()).enqueue_job(job))
        .unwrap();

    let now = now_ms();
    let claimed = store
        .claim_jobs(ClaimRequest {
            queue: "q".into(),
            worker_id: "w".into(),
            now_ms: now,
            lease_ms: 1000,
            limit: 1,
        })
        .unwrap();
    let token = claimed[0].lease_token;

    let fail_batch = CommitBatch::new(Id::new(), now + 1).fail_job(job_id, token, "boom", None);
    store.commit(fail_batch.clone()).unwrap();
    assert_eq!(
        store.job_state(job_id, now + 100).unwrap(),
        minisqlite::JobState::Dead
    );

    drop(store);
    let store = StoreBuilder::new(&path)
        .durability(Durability::Strict)
        .open()
        .unwrap();
    store.commit(fail_batch).unwrap();
    assert_eq!(
        store.job_state(job_id, now + 100).unwrap(),
        minisqlite::JobState::Dead
    );
}

#[test]
fn stream_versions_in_receipt_are_deterministically_sorted() {
    let tmp = common::TempDir::new();
    let path = tmp.path().join("sorted_streams.mini");
    let store = StoreBuilder::new(&path)
        .durability(Durability::Memory)
        .open()
        .unwrap();

    let now = now_ms();
    let batch = CommitBatch::new(Id::new(), now)
        .append_event(Event::with_json_payload(Id::new(), "zebra", "e", now, b"z"))
        .append_event(Event::with_json_payload(Id::new(), "apple", "e", now, b"a"))
        .append_event(Event::with_json_payload(Id::new(), "mango", "e", now, b"m"));
    let receipt = store.commit(batch).unwrap();
    let ids: Vec<_> = receipt
        .stream_versions
        .iter()
        .map(|sv| sv.stream_id.as_str())
        .collect();
    assert_eq!(ids, vec!["apple", "mango", "zebra"]);

    drop(store);
    let store = StoreBuilder::new(&path).open().unwrap();
    let recovered = store.get_transaction(receipt.transaction_id).unwrap();
    let ids2: Vec<_> = recovered
        .stream_versions
        .iter()
        .map(|sv| sv.stream_id.as_str())
        .collect();
    assert_eq!(ids2, vec!["apple", "mango", "zebra"]);
}

#[test]
fn zero_id_is_rejected() {
    let tmp = common::TempDir::new();
    let path = tmp.path().join("zero_id.mini");
    let store = StoreBuilder::new(&path).open().unwrap();

    let event = Event::with_json_payload(Id::ZERO, "s", "e", now_ms(), b"p");
    let batch = CommitBatch::new(Id::new(), now_ms()).append_event(event);
    assert!(
        store.commit(batch).is_err(),
        "zero event id must be rejected"
    );

    let batch = CommitBatch::new(Id::ZERO, now_ms()).append_event(Event::with_json_payload(
        Id::new(),
        "s",
        "e",
        now_ms(),
        b"p",
    ));
    assert!(
        store.commit(batch).is_err(),
        "zero transaction id must be rejected"
    );

    let batch = CommitBatch::new(Id::new(), now_ms()).enqueue_job(JobSpec::new(
        Id::ZERO,
        "q",
        "p",
        b"".to_vec(),
    ));
    assert!(store.commit(batch).is_err(), "zero job id must be rejected");
}

#[test]
fn file_header_semantics_are_enforced() {
    let tmp = common::TempDir::new();
    let path = tmp.path().join("header_semantics.mini");

    let store = StoreBuilder::new(&path).open().unwrap();
    store
        .commit(
            CommitBatch::new(Id::new(), now_ms()).append_event(Event::with_json_payload(
                Id::new(),
                "s",
                "e",
                now_ms(),
                b"p",
            )),
        )
        .unwrap();
    drop(store);

    let mut bytes = std::fs::read(&path).unwrap();
    // File header layout: magic 0..8, major 8..10, minor 10..12, header_length 12..14,
    // created_at_ms 14..22, flags 22..26, reserved 26..60, header CRC32 60..64.
    let flags_offset = 22usize;
    if flags_offset + 4 <= bytes.len() {
        bytes[flags_offset..flags_offset + 4].copy_from_slice(&[0xff; 4]);
        let checksum = crc32fast::hash(&bytes[0..60]);
        bytes[60..64].copy_from_slice(&checksum.to_le_bytes());
        std::fs::write(&path, &bytes).unwrap();
    }

    assert!(
        StoreBuilder::new(&path).open().is_err(),
        "unsupported file header flags must fail"
    );
}

#[test]
fn stale_worker_and_lease_metadata_cleared_after_terminal_fail() {
    let tmp = common::TempDir::new();
    let path = tmp.path().join("stale_metadata.mini");
    let store = StoreBuilder::new(&path).open().unwrap();

    let job_id = Id::from(3u128);
    let job = JobSpec::new(job_id, "q", "p", b"payload".to_vec()).with_max_attempts(2);
    store
        .commit(CommitBatch::new(Id::new(), now_ms()).enqueue_job(job))
        .unwrap();

    let now = now_ms();
    let mut attempt = 1;
    loop {
        let claimed = store
            .claim_jobs(ClaimRequest {
                queue: "q".into(),
                worker_id: format!("w{attempt}"),
                now_ms: now + (attempt - 1) * 2000,
                lease_ms: 10_000,
                limit: 1,
            })
            .unwrap();
        assert_eq!(claimed.len(), 1);
        let token = claimed[0].lease_token;

        store
            .commit(
                CommitBatch::new(Id::new(), now + (attempt - 1) * 2000 + 1000)
                    .fail_job(job_id, token, "boom", None),
            )
            .unwrap();

        let state = store
            .job_state(job_id, now + (attempt - 1) * 2000 + 1050)
            .unwrap();
        if state == minisqlite::JobState::Dead {
            break;
        }
        attempt += 1;
    }

    let jobs = store.jobs(now + 100_000, None, Some(minisqlite::JobState::Dead));
    assert_eq!(jobs.len(), 1);
    assert_eq!(jobs[0].worker_id, None);
    assert_eq!(jobs[0].lease_expires_at_ms, None);
    assert_eq!(jobs[0].retry_after_ms, None);
    assert_eq!(jobs[0].error_summary.as_deref(), Some("boom"));
}

#[test]
fn backup_and_reopen_restores_state() {
    let tmp = common::TempDir::new();
    let path = tmp.path().join("backup_src.mini");
    let dest_dir = common::TempDir::new();
    let dest = dest_dir.path().join("backup_dest.mini");

    let store = StoreBuilder::new(&path).open().unwrap();
    let event = Event::with_json_payload(Id::new(), "s", "e", now_ms(), b"p");
    store
        .commit(CommitBatch::new(Id::new(), now_ms()).append_event(event))
        .unwrap();
    store.backup(&dest).unwrap();

    let backup = StoreBuilder::new(&dest).open().unwrap();
    assert_eq!(backup.stats().event_count, 1);
}

#[test]
fn projection_scan_preserves_binary_keys() {
    let tmp = common::TempDir::new();
    let path = tmp.path().join("projection_binary.mini");
    let store = StoreBuilder::new(&path).open().unwrap();

    store
        .commit(CommitBatch::new(Id::new(), now_ms()).projection_put(
            "p",
            1,
            b"\xff\xfe".to_vec(),
            b"value".to_vec(),
        ))
        .unwrap();

    let entries = store.scan_projection_prefix("p", &[]).unwrap();
    assert_eq!(entries.len(), 1);
    assert_eq!(entries[0].key, b"\xff\xfe");
}

#[test]
fn uncertain_job_can_be_resolved_after_reopen() {
    let tmp = common::TempDir::new();
    let path = tmp.path().join("uncertain_resolve.mini");
    let store = StoreBuilder::new(&path)
        .durability(Durability::Strict)
        .open()
        .unwrap();

    let job_id = Id::from(4u128);
    let job = JobSpec::new(job_id, "q", "p", b"payload".to_vec())
        .with_effect_mode(EffectMode::UncertainOnLeaseExpiry);
    store
        .commit(CommitBatch::new(Id::new(), now_ms()).enqueue_job(job))
        .unwrap();

    let now = now_ms();
    let claimed = store
        .claim_jobs(ClaimRequest {
            queue: "q".into(),
            worker_id: "w".into(),
            now_ms: now,
            lease_ms: 100,
            limit: 1,
        })
        .unwrap();
    let _token = claimed[0].lease_token;

    drop(store);
    let store = StoreBuilder::new(&path).open().unwrap();
    assert_eq!(
        store.job_state(job_id, now + 200).unwrap(),
        minisqlite::JobState::Uncertain
    );

    store
        .commit(
            CommitBatch::new(Id::new(), now + 201).resolve_uncertain_job(job_id, Resolution::Retry),
        )
        .unwrap();
    assert_eq!(
        store.job_state(job_id, now + 300).unwrap(),
        minisqlite::JobState::RetryWait
    );
}

#[test]
fn multi_stream_receipt_is_stable_across_reopen() {
    let tmp = common::TempDir::new();
    let path = tmp.path().join("multi_stream_receipt.mini");
    let store = StoreBuilder::new(&path)
        .durability(Durability::Strict)
        .open()
        .unwrap();

    let now = now_ms();
    let batch = CommitBatch::new(Id::new(), now)
        .append_event(Event::with_json_payload(Id::new(), "b", "e", now, b""))
        .append_event(Event::with_json_payload(Id::new(), "a", "e", now, b""));
    let receipt = store.commit(batch).unwrap();
    let first = receipt.stream_versions.clone();

    drop(store);
    let store = StoreBuilder::new(&path).open().unwrap();
    let second = store
        .get_transaction(receipt.transaction_id)
        .unwrap()
        .stream_versions;
    assert_eq!(first, second);
}
