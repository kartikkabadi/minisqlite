use std::time::{SystemTime, UNIX_EPOCH};

use minisqlite::{
    ClaimRequest, CommitBatch, Durability, EffectMode, Event, Id, JobSpec, Limits, Resolution,
    StoreBuilder,
};

mod common;

fn now_ms() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis() as i64)
        .unwrap_or(0)
}

#[test]
fn expired_maintenance_makes_progress_with_tiny_transaction_limit() {
    let tmp = common::TempDir::new();
    let path = tmp.path().join("wedge.mini");

    let limits = Limits {
        max_records_per_transaction: 1,
        ..Limits::default()
    };

    let store = StoreBuilder::new(&path)
        .durability(Durability::Memory)
        .limits(limits)
        .open()
        .unwrap();

    // Enqueue three idempotent jobs that expire at the attempt ceiling, plus a
    // fresh idempotent job with more attempts.
    let job1 = JobSpec::new(Id::new().unwrap(), "q", "p1", b"a".to_vec())
        .with_max_attempts(1)
        .with_effect_mode(EffectMode::Idempotent);
    let job2 = JobSpec::new(Id::new().unwrap(), "q", "p2", b"b".to_vec())
        .with_max_attempts(1)
        .with_effect_mode(EffectMode::Idempotent);
    let job3 = JobSpec::new(Id::new().unwrap(), "q", "p3", b"c".to_vec())
        .with_max_attempts(1)
        .with_effect_mode(EffectMode::Idempotent);
    let fresh = JobSpec::new(Id::new().unwrap(), "q", "p4", b"d".to_vec())
        .with_max_attempts(3)
        .with_effect_mode(EffectMode::Idempotent);

    store
        .commit(CommitBatch::new(Id::new().unwrap(), now_ms()).enqueue_job(job1))
        .unwrap();
    store
        .commit(CommitBatch::new(Id::new().unwrap(), now_ms()).enqueue_job(job2))
        .unwrap();
    store
        .commit(CommitBatch::new(Id::new().unwrap(), now_ms()).enqueue_job(job3))
        .unwrap();
    store
        .commit(CommitBatch::new(Id::new().unwrap(), now_ms()).enqueue_job(fresh))
        .unwrap();

    // Claim all four with a 1ms lease, one at a time (max_records_per_transaction == 1).
    let start = now_ms();
    for _ in 0..4 {
        let c = store
            .claim_jobs(ClaimRequest {
                queue: "q".into(),
                worker_id: "w".into(),
                now_ms: start,
                lease_ms: 1,
                limit: 1,
            })
            .unwrap();
        assert_eq!(c.len(), 1, "each claim should return exactly one job");
    }

    // Let the leases expire. With max_records_per_transaction == 1, the old
    // single-batch maintenance would have failed. Each call should durably
    // clean the expired final-attempt jobs and then claim the ready fresh job.
    // With `max_records_per_transaction == 1` the maintenance for the three expired
    // final-attempt jobs and the fresh claim cannot fit in a single atomic batch.
    // `claim_jobs` therefore makes progress by cleaning one expired job per call and,
    // once the queue is clear, returns the ready fresh job.
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
        seen.extend(c.claims().iter().map(|j| j.job_id));
    }
    assert_eq!(
        seen.len(),
        1,
        "exactly one later ready job should be claimable"
    );

    // Reopen and prove the queue is still not wedged.
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
    assert!(final_claim.is_empty(), "no remaining jobs after cleanup");
}

#[test]
fn final_frame_corruption_fails_open_and_verify_does_not_truncate() {
    let tmp = common::TempDir::new();
    let path = tmp.path().join("final_corrupt.mini");

    let store = StoreBuilder::new(&path)
        .durability(Durability::Strict)
        .open()
        .unwrap();
    store
        .commit(CommitBatch::new(Id::new().unwrap(), now_ms()).append_event(
            Event::with_json_payload(Id::new().unwrap(), "s", "e", now_ms(), b"{}"),
        ))
        .unwrap();
    let len_before = std::fs::metadata(&path).unwrap().len();
    drop(store);

    // Flip a payload byte in the final frame (after the 64-byte file header).
    let mut bytes = std::fs::read(&path).unwrap();
    assert!(bytes.len() > 65);
    bytes[65] ^= 0xFF;
    std::fs::write(&path, &bytes).unwrap();

    let len_after_write = std::fs::metadata(&path).unwrap().len();
    assert_eq!(len_before, len_after_write);

    // Reopen must fail; the file length must not change.
    assert!(
        StoreBuilder::new(&path).open().is_err(),
        "corrupt final frame must fail open"
    );
    assert_eq!(
        std::fs::metadata(&path).unwrap().len(),
        len_before,
        "open must not truncate a corrupt final frame"
    );

    // `verify` must also fail and not truncate.
    let result = minisqlite::StoreBuilder::new(&path).open_existing();
    assert!(
        result.is_err(),
        "verify/doctor path must fail on corruption"
    );
    assert_eq!(
        std::fs::metadata(&path).unwrap().len(),
        len_before,
        "verify must not truncate a corrupt final frame"
    );
}

#[test]
fn backup_rejects_live_path_after_working_directory_change() {
    let tmp = common::TempDir::new();
    let db = tmp.path().join("db.mini");

    // Open through a relative path.
    let original_cwd = std::env::current_dir().unwrap();
    std::env::set_current_dir(tmp.path()).unwrap();
    let store = StoreBuilder::new("db.mini")
        .durability(Durability::Memory)
        .open()
        .unwrap();

    store
        .commit(CommitBatch::new(Id::new().unwrap(), now_ms()).append_event(
            Event::with_json_payload(Id::new().unwrap(), "s", "e", now_ms(), b"1"),
        ))
        .unwrap();

    // Change working directory and attempt to back up to the absolute path
    // that is actually the live, open file.
    std::env::set_current_dir(&original_cwd).unwrap();
    let abs = db.canonicalize().unwrap();
    let result = store.backup(&abs);
    assert!(
        result.is_err(),
        "backup to the live file must fail after a cwd change"
    );

    // The store must remain writable and survive reopen.
    store
        .commit(CommitBatch::new(Id::new().unwrap(), now_ms()).append_event(
            Event::with_json_payload(Id::new().unwrap(), "s", "e2", now_ms(), b"2"),
        ))
        .unwrap();
    assert_eq!(store.high_water_sequence(), 2);

    drop(store);
    std::env::set_current_dir(tmp.path()).unwrap();
    let store = StoreBuilder::new("db.mini")
        .durability(Durability::Memory)
        .open()
        .unwrap();
    assert_eq!(store.high_water_sequence(), 2);
    std::env::set_current_dir(&original_cwd).unwrap();
}

#[test]
fn expired_uncertain_job_cannot_be_failed() {
    let tmp = common::TempDir::new();
    let path = tmp.path().join("uncertain.mini");

    let store = StoreBuilder::new(&path)
        .durability(Durability::Memory)
        .open()
        .unwrap();

    let job = JobSpec::new(Id::new().unwrap(), "q", "p", b"call-api".to_vec())
        .with_effect_mode(EffectMode::UncertainOnLeaseExpiry)
        .with_max_attempts(1);
    let job_id = job.job_id;
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
    let token = claimed.claims()[0].lease_token;

    // Expire the lease and attempt to fail the job at the attempt ceiling.
    let fail_result = store.fail_job(job_id, token, "boom", None, start + 10);
    assert!(
        matches!(fail_result, Err(minisqlite::Error::InvalidLease { .. })),
        "expired uncertain job must not be silently marked dead: {fail_result:?}"
    );

    // State must remain Uncertain, and only resolve can finalize it.
    assert!(matches!(
        store.job_state(job_id, start + 10).unwrap(),
        minisqlite::JobState::Uncertain
    ));
    store
        .resolve_uncertain_job(job_id, Resolution::MarkDead, start + 10)
        .unwrap();
    assert!(matches!(
        store.job_state(job_id, start + 10).unwrap(),
        minisqlite::JobState::Dead
    ));

    // The same behavior survives reopen.
    drop(store);
    let store = StoreBuilder::new(&path)
        .durability(Durability::Memory)
        .open()
        .unwrap();
    let second_fail = store.fail_job(job_id, token, "boom", None, start + 10);
    assert!(
        matches!(second_fail, Err(minisqlite::Error::InvalidLease { .. })),
        "reopened uncertain job must still reject fail_job"
    );
}

#[test]
fn duplicate_projection_replace_keys_are_canonicalized_and_versioned() {
    let tmp = common::TempDir::new();
    let path = tmp.path().join("dup_replace.mini");

    let store = StoreBuilder::new(&path)
        .durability(Durability::Memory)
        .open()
        .unwrap();

    // Same-version duplicate-key replace that would change the canonical value must fail.
    store
        .commit(
            CommitBatch::new(Id::new().unwrap(), now_ms()).projection_put(
                "p",
                1,
                b"a".to_vec(),
                b"1".to_vec(),
            ),
        )
        .unwrap();
    let same_version = store.commit(
        CommitBatch::new(Id::new().unwrap(), now_ms()).projection_replace(
            "p",
            1,
            vec![
                minisqlite::ProjectionEntry::new(b"a".to_vec(), b"1".to_vec()),
                minisqlite::ProjectionEntry::new(b"a".to_vec(), b"2".to_vec()),
            ],
        ),
    );
    assert!(
        same_version.is_err(),
        "same-version duplicate-key replace must fail: {same_version:?}"
    );
    assert_eq!(
        store.get_projection("p", b"a").unwrap().as_deref(),
        Some(b"1".as_slice()),
        "same-version duplicate-key replace must not mutate state"
    );

    // Bumped-version duplicate-key replace applies last-wins and advances version.
    store
        .commit(
            CommitBatch::new(Id::new().unwrap(), now_ms()).projection_replace(
                "p",
                2,
                vec![
                    minisqlite::ProjectionEntry::new(b"a".to_vec(), b"first".to_vec()),
                    minisqlite::ProjectionEntry::new(b"a".to_vec(), b"last".to_vec()),
                ],
            ),
        )
        .unwrap();
    assert_eq!(
        store.get_projection("p", b"a").unwrap().as_deref(),
        Some(b"last".as_slice())
    );
    assert_eq!(store.projection_version("p").unwrap(), 2);
}

#[test]
fn projection_clear_name_length_is_validated() {
    let tmp = common::TempDir::new();
    let path = tmp.path().join("clear_name.mini");

    let limits = Limits {
        max_string_len: 4,
        ..Limits::default()
    };
    let store = StoreBuilder::new(&path)
        .durability(Durability::Memory)
        .limits(limits)
        .open()
        .unwrap();

    let result = store.commit(
        CommitBatch::new(Id::new().unwrap(), now_ms()).projection_clear("longer-than-four", 1),
    );
    assert!(
        result.is_err(),
        "ProjectionClear must validate the projection name length: {result:?}"
    );
}

#[test]
fn projection_version_overflow_is_rejected() {
    // Directly exercise the checked version arithmetic at the boundary.
    let tmp = common::TempDir::new();
    let path = tmp.path().join("version_overflow.mini");

    let store = StoreBuilder::new(&path)
        .durability(Durability::Memory)
        .open()
        .unwrap();

    // An impossible target version is rejected before any arithmetic can overflow.
    let result = store.commit(
        CommitBatch::new(Id::new().unwrap(), now_ms()).projection_put(
            "p",
            u64::MAX,
            b"a".to_vec(),
            b"v".to_vec(),
        ),
    );
    assert!(
        result.is_err(),
        "projection version overflow must be rejected: {result:?}"
    );
}
