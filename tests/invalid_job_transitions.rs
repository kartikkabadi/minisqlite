use minisqlite::{
    ClaimRequest, CommitBatch, Durability, Id, JobSpec, Resolution, Store, StoreBuilder,
};

mod common;

fn store() -> (common::TempDir, Store) {
    let tmp = common::TempDir::new();
    let store = StoreBuilder::new(tmp.path().join("jobs.mini"))
        .durability(Durability::Memory)
        .open()
        .unwrap();
    (tmp, store)
}

#[test]
fn ack_with_wrong_token_fails() {
    let (_tmp, store) = store();
    let job_id = Id::new();
    let job = JobSpec::new(job_id, "q", "p", b"work".to_vec());
    store
        .commit(CommitBatch::new(Id::new(), 0).enqueue_job(job))
        .unwrap();

    let claimed = store
        .claim_jobs(ClaimRequest {
            queue: "q".into(),
            worker_id: "w".into(),
            now_ms: 0,
            lease_ms: 1000,
            limit: 1,
        })
        .unwrap();
    assert_eq!(claimed.len(), 1);

    let wrong_token = Id::new();
    let result = store.ack_job(job_id, wrong_token, None, 0);
    assert!(result.is_err(), "ack with wrong token must fail");
}

#[test]
fn ack_after_lease_expiry_fails() {
    let (_tmp, store) = store();
    let job_id = Id::new();
    store
        .commit(CommitBatch::new(Id::new(), 0).enqueue_job(JobSpec::new(
            job_id,
            "q",
            "p",
            b"work".to_vec(),
        )))
        .unwrap();

    let claimed = store
        .claim_jobs(ClaimRequest {
            queue: "q".into(),
            worker_id: "w".into(),
            now_ms: 0,
            lease_ms: 1000,
            limit: 1,
        })
        .unwrap();
    let token = claimed[0].lease_token;

    let result = store.ack_job(job_id, token, None, 2000);
    assert!(result.is_err(), "ack after lease expiry must fail");
}

#[test]
fn fail_with_wrong_token_fails() {
    let (_tmp, store) = store();
    let job_id = Id::new();
    store
        .commit(CommitBatch::new(Id::new(), 0).enqueue_job(JobSpec::new(
            job_id,
            "q",
            "p",
            b"work".to_vec(),
        )))
        .unwrap();

    store
        .claim_jobs(ClaimRequest {
            queue: "q".into(),
            worker_id: "w".into(),
            now_ms: 0,
            lease_ms: 1000,
            limit: 1,
        })
        .unwrap();

    let result = store.fail_job(job_id, Id::new(), "boom", None, 0);
    assert!(result.is_err(), "fail with wrong token must fail");
}

#[test]
fn cancel_terminal_job_fails() {
    let (_tmp, store) = store();
    let job_id = Id::new();
    let job = JobSpec::new(job_id, "q", "p", b"work".to_vec()).with_max_attempts(1);
    store
        .commit(CommitBatch::new(Id::new(), 0).enqueue_job(job))
        .unwrap();

    let claimed = store
        .claim_jobs(ClaimRequest {
            queue: "q".into(),
            worker_id: "w".into(),
            now_ms: 0,
            lease_ms: 1000,
            limit: 1,
        })
        .unwrap();
    let token = claimed[0].lease_token;

    store.fail_job(job_id, token, "boom", None, 0).unwrap();
    let result = store.cancel_job(job_id, None, 0);
    assert!(result.is_err(), "cancel on dead job must fail");
}

#[test]
fn resolve_non_uncertain_job_fails() {
    let (_tmp, store) = store();
    let job_id = Id::new();
    store
        .commit(CommitBatch::new(Id::new(), 0).enqueue_job(JobSpec::new(
            job_id,
            "q",
            "p",
            b"work".to_vec(),
        )))
        .unwrap();

    let result = store.resolve_uncertain_job(job_id, Resolution::MarkSucceeded, 0);
    assert!(result.is_err(), "resolving a non-uncertain job must fail");
}

#[test]
fn double_claim_without_expiry_fails() {
    let (_tmp, store) = store();
    let job_id = Id::new();
    store
        .commit(CommitBatch::new(Id::new(), 0).enqueue_job(JobSpec::new(
            job_id,
            "q",
            "p",
            b"work".to_vec(),
        )))
        .unwrap();

    store
        .claim_jobs(ClaimRequest {
            queue: "q".into(),
            worker_id: "w1".into(),
            now_ms: 0,
            lease_ms: 1000,
            limit: 1,
        })
        .unwrap();

    let second = store
        .claim_jobs(ClaimRequest {
            queue: "q".into(),
            worker_id: "w2".into(),
            now_ms: 0,
            lease_ms: 1000,
            limit: 1,
        })
        .unwrap();
    assert!(
        second.is_empty(),
        "claiming an already leased job must not duplicate the lease"
    );
}

#[test]
fn claim_before_not_before_fails() {
    let (_tmp, store) = store();
    let job_id = Id::new();
    let job = JobSpec::new(job_id, "q", "p", b"work".to_vec()).with_not_before_ms(1000);
    store
        .commit(CommitBatch::new(Id::new(), 0).enqueue_job(job))
        .unwrap();

    let claimed = store
        .claim_jobs(ClaimRequest {
            queue: "q".into(),
            worker_id: "w".into(),
            now_ms: 0,
            lease_ms: 1000,
            limit: 1,
        })
        .unwrap();
    assert!(
        claimed.is_empty(),
        "job must not be claimable before not_before_ms"
    );
}

#[test]
fn stale_token_cannot_fail_newer_lease() {
    let (_tmp, store) = store();
    let job_id = Id::new();
    let job = JobSpec::new(job_id, "q", "p", b"work".to_vec());
    store
        .commit(CommitBatch::new(Id::new(), 0).enqueue_job(job))
        .unwrap();

    let first = store
        .claim_jobs(ClaimRequest {
            queue: "q".into(),
            worker_id: "w".into(),
            now_ms: 0,
            lease_ms: 100,
            limit: 1,
        })
        .unwrap();
    let old_token = first[0].lease_token;

    let second = store
        .claim_jobs(ClaimRequest {
            queue: "q".into(),
            worker_id: "w".into(),
            now_ms: 200,
            lease_ms: 100,
            limit: 1,
        })
        .unwrap();
    assert_eq!(second[0].job_id, job_id);
    assert_ne!(second[0].lease_token, old_token);

    let result = store.fail_job(job_id, old_token, "boom", None, 200);
    assert!(result.is_err(), "stale token must not fail the newer lease");
}

#[test]
fn claim_jobs_rejects_non_positive_lease() {
    let (_tmp, store) = store();
    let job_id = Id::new();
    store
        .commit(CommitBatch::new(Id::new(), 0).enqueue_job(JobSpec::new(
            job_id,
            "q",
            "p",
            b"work".to_vec(),
        )))
        .unwrap();

    let result = store.claim_jobs(ClaimRequest {
        queue: "q".into(),
        worker_id: "w".into(),
        now_ms: 0,
        lease_ms: 0,
        limit: 1,
    });
    assert!(result.is_err(), "zero lease_ms must be rejected");

    let result = store.claim_jobs(ClaimRequest {
        queue: "q".into(),
        worker_id: "w".into(),
        now_ms: 0,
        lease_ms: -1,
        limit: 1,
    });
    assert!(result.is_err(), "negative lease_ms must be rejected");
}

#[test]
fn claim_jobs_rejects_lease_arithmetic_overflow() {
    let (_tmp, store) = store();
    let job_id = Id::new();
    store
        .commit(CommitBatch::new(Id::new(), 0).enqueue_job(JobSpec::new(
            job_id,
            "q",
            "p",
            b"work".to_vec(),
        )))
        .unwrap();

    let result = store.claim_jobs(ClaimRequest {
        queue: "q".into(),
        worker_id: "w".into(),
        now_ms: i64::MAX,
        lease_ms: i64::MAX,
        limit: 1,
    });
    assert!(result.is_err(), "lease expiry overflow must be rejected");
}
