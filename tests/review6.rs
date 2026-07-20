use std::time::{SystemTime, UNIX_EPOCH};

use minisqlite::{
    ClaimRequest, CommitBatch, Durability, EffectMode, Event, Id, JobSpec, Limits, StoreBuilder,
};

mod common;

fn now_ms() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis() as i64)
        .unwrap_or(0)
}

#[test]
fn expired_job_maintenance_is_independent_of_summary_and_frame_limits() {
    // Maintenance for expired final-attempt jobs uses a fixed-size JobExpire record.
    // It must succeed even when max_summary_len is zero and max_frame_size is at the
    // minimum that still holds one JobExpire.
    let tmp = common::TempDir::new();
    let path = tmp.path().join("expire_summary.mini");

    let limits = Limits {
        max_summary_len: 0,
        max_frame_size: 200,
        ..Limits::default()
    };
    let store = StoreBuilder::new(&path)
        .durability(Durability::Memory)
        .limits(limits)
        .open()
        .unwrap();

    let job = JobSpec::new(Id::new().unwrap(), "q", "p", b"payload".to_vec())
        .with_max_attempts(1)
        .with_effect_mode(EffectMode::Idempotent);
    store
        .commit(CommitBatch::new(Id::new().unwrap(), now_ms()).enqueue_job(job))
        .unwrap();

    let start = now_ms();
    let claimed = store
        .claim_jobs(ClaimRequest {
            queue: "q".into(),
            worker_id: "w".into(),
            now_ms: start,
            lease_ms: 1,
            limit: 1,
        })
        .unwrap();
    assert_eq!(claimed.len(), 1);

    // Let the lease expire and claim again. The internal JobExpire must fit in the
    // tiny frame and not depend on max_summary_len.
    let result = store.claim_jobs(ClaimRequest {
        queue: "q".into(),
        worker_id: "w".into(),
        now_ms: start + 10,
        lease_ms: 1000,
        limit: 1,
    });
    assert!(
        result.is_ok(),
        "JobExpire maintenance must fit tiny limits: {result:?}"
    );
}

#[test]
fn claim_jobs_budgets_records_and_frame_size() {
    // With max_records_per_transaction == 1, each claim_jobs call may only durably
    // commit one operation, but it must never create a partial durable state.
    let tmp = common::TempDir::new();
    let path = tmp.path().join("claim_budget.mini");

    let limits = Limits {
        max_records_per_transaction: 1,
        ..Limits::default()
    };
    let store = StoreBuilder::new(&path)
        .durability(Durability::Memory)
        .limits(limits)
        .open()
        .unwrap();

    let expired1 = JobSpec::new(Id::new().unwrap(), "q", "p1", b"a".to_vec())
        .with_max_attempts(1)
        .with_effect_mode(EffectMode::Idempotent);
    let expired2 = JobSpec::new(Id::new().unwrap(), "q", "p2", b"b".to_vec())
        .with_max_attempts(1)
        .with_effect_mode(EffectMode::Idempotent);
    let fresh = JobSpec::new(Id::new().unwrap(), "q", "p3", b"c".to_vec())
        .with_max_attempts(3)
        .with_effect_mode(EffectMode::Idempotent);

    for job in [expired1, expired2, fresh] {
        store
            .commit(CommitBatch::new(Id::new().unwrap(), now_ms()).enqueue_job(job))
            .unwrap();
    }

    let start = now_ms();
    for _ in 0..3 {
        store
            .claim_jobs(ClaimRequest {
                queue: "q".into(),
                worker_id: "w".into(),
                now_ms: start,
                lease_ms: 1,
                limit: 1,
            })
            .unwrap();
    }

    let mut seen = vec![];
    for _ in 0..10 {
        let c = store
            .claim_jobs(ClaimRequest {
                queue: "q".into(),
                worker_id: "w".into(),
                now_ms: start + 10,
                lease_ms: 1000,
                limit: 1,
            })
            .unwrap();
        seen.extend(c.into_iter().map(|j| j.job_id));
    }
    assert_eq!(
        seen.len(),
        1,
        "fresh job must be claimable after bounded maintenance"
    );

    // Reopen and prove the queue is still consistent.
    drop(store);
    let store = StoreBuilder::new(&path)
        .durability(Durability::Memory)
        .limits(limits)
        .open()
        .unwrap();
    let final_claim = store
        .claim_jobs(ClaimRequest {
            queue: "q".into(),
            worker_id: "w".into(),
            now_ms: start + 10,
            lease_ms: 1000,
            limit: 1,
        })
        .unwrap();
    assert!(final_claim.is_empty());
}

#[test]
fn open_existing_zero_byte_file_is_not_created_or_repaired() {
    let tmp = common::TempDir::new();
    let path = tmp.path().join("zero.mini");
    std::fs::write(&path, b"").unwrap();
    let size_before = std::fs::metadata(&path).unwrap().len();

    let result = StoreBuilder::new(&path).open_existing();
    assert!(
        matches!(result, Err(minisqlite::Error::NotMiniSQLite)),
        "open_existing on an empty file must fail: {result:?}",
        result = result.map(|_| ()).map_err(|e| e.to_string()),
    );

    let size_after = std::fs::metadata(&path).unwrap().len();
    assert_eq!(
        size_before, size_after,
        "open_existing must not modify a zero-byte file"
    );

    let result = StoreBuilder::new(&path).verify();
    assert!(
        matches!(result, Err(minisqlite::Error::NotMiniSQLite)),
        "verify on an empty file must fail: {result:?}",
        result = result.map(|_| ()).map_err(|e| e.to_string()),
    );
}

#[test]
fn verify_and_open_existing_are_non_mutating_on_torn_tail() {
    let tmp = common::TempDir::new();
    let path = tmp.path().join("torn.mini");

    let store = StoreBuilder::new(&path)
        .durability(Durability::Strict)
        .open()
        .unwrap();
    store
        .commit(
            CommitBatch::new(Id::new().unwrap(), now_ms()).append_event(Event::new(
                Id::new().unwrap(),
                "s",
                "e",
                1,
                now_ms(),
                None,
                None,
                b"",
                b"",
            )),
        )
        .unwrap();
    drop(store);

    let size_before = std::fs::metadata(&path).unwrap().len();
    // Truncate in the middle of the second frame header.
    let file = std::fs::OpenOptions::new().write(true).open(&path).unwrap();
    file.set_len(size_before - 8).unwrap();
    drop(file);
    let size_torn = std::fs::metadata(&path).unwrap().len();

    // Read-only verify must not modify the file.
    let verify_result = StoreBuilder::new(&path).verify();
    assert!(
        verify_result.is_ok(),
        "verify must report valid prefix: {verify_result:?}",
        verify_result = verify_result.map(|_| ()).map_err(|e| e.to_string())
    );
    let size_after_verify = std::fs::metadata(&path).unwrap().len();
    assert_eq!(
        size_torn, size_after_verify,
        "verify must not modify the torn tail"
    );

    // open_existing must not repair, but must be readable and block writes.
    let store = StoreBuilder::new(&path).open_existing().unwrap();
    let result = store.commit(CommitBatch::new(Id::new().unwrap(), now_ms()).append_event(
        Event::new(
            Id::new().unwrap(),
            "s",
            "e",
            2,
            now_ms(),
            None,
            None,
            b"",
            b"",
        ),
    ));
    assert!(
        matches!(result, Err(minisqlite::Error::StoreNeedsRepair)),
        "writes must be blocked until explicit repair: {result:?}",
        result = result.map(|_| ()).map_err(|e| e.to_string()),
    );
    assert_eq!(
        size_after_verify,
        std::fs::metadata(&path).unwrap().len(),
        "open_existing must not truncate the torn tail"
    );

    // Explicit repair makes the store writable again.
    store.repair().unwrap();
    let size_after_repair = std::fs::metadata(&path).unwrap().len();
    assert!(
        size_after_repair < size_torn,
        "repair must truncate the torn tail"
    );

    store
        .commit(
            CommitBatch::new(Id::new().unwrap(), now_ms()).append_event(Event::new(
                Id::new().unwrap(),
                "s",
                "e",
                2,
                now_ms(),
                None,
                None,
                b"",
                b"",
            )),
        )
        .unwrap();
}

#[test]
fn backup_refuses_existing_destination() {
    let tmp = common::TempDir::new();
    let src = tmp.path().join("src.mini");
    let dest = tmp.path().join("dest.mini");

    let store = StoreBuilder::new(&src)
        .durability(Durability::Memory)
        .open()
        .unwrap();
    store
        .commit(
            CommitBatch::new(Id::new().unwrap(), now_ms()).append_event(Event::new(
                Id::new().unwrap(),
                "s",
                "e",
                1,
                now_ms(),
                None,
                None,
                b"",
                b"",
            )),
        )
        .unwrap();

    std::fs::write(&dest, b"pre-existing").unwrap();
    let result = store.backup(&dest);
    assert!(
        matches!(result, Err(minisqlite::Error::Validation(ref s)) if s.contains("already exists")),
        "backup must refuse to overwrite an existing destination: {result:?}",
        result = result.map(|_| ()).map_err(|e| e.to_string()),
    );
    assert_eq!(
        std::fs::read_to_string(&dest).unwrap(),
        "pre-existing",
        "existing destination must not be touched"
    );
}

#[test]
fn limits_minimum_frame_size_covers_internal_records() {
    let tmp = common::TempDir::new();
    let path = tmp.path().join("limits.mini");

    let limits = Limits {
        max_frame_size: 95, // below header + trailer + one fixed-size record
        ..Limits::default()
    };
    let result = StoreBuilder::new(&path)
        .durability(Durability::Memory)
        .limits(limits)
        .open();
    assert!(
        result.is_err(),
        "limits must reject a max_frame_size that cannot hold internal records: {result:?}",
        result = result.map(|_| ()).map_err(|e| e.to_string()),
    );
}
