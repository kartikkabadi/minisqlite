//! Structure-aware fuzzing of the public store API (spec Part B, Layer 7).
//!
//! The input bytes drive a sequence of commits, claims, acknowledgements,
//! failures, cancellations, lease extensions, and uncertain resolutions
//! against a fresh store. The store must never panic, and `verify` must
//! report a consistent database after every input.
//!
//! Run with: `cargo +nightly fuzz run fuzz_ops` (from the repo root).

#![no_main]

use libfuzzer_sys::fuzz_target;
use minisqlite::{
    ClaimOutcome, ClaimRequest, CommitBatch, ControlPlaneStore, Event, Id, JobSpec,
    ProjectionMutation, ProjectionPatch, Resolution,
};

struct Bytes<'a> {
    data: &'a [u8],
    pos: usize,
}

impl<'a> Bytes<'a> {
    fn u8(&mut self) -> u8 {
        let b = self.data.get(self.pos).copied().unwrap_or(0);
        self.pos += 1;
        b
    }

    fn done(&self) -> bool {
        self.pos >= self.data.len()
    }
}

fuzz_target!(|data: &[u8]| {
    let dir = tempfile::tempdir().unwrap();
    let store = ControlPlaneStore::open(dir.path().join("fuzz.db")).unwrap();
    let mut input = Bytes { data, pos: 0 };
    let mut next_id: u128 = 1;
    let mut leases: Vec<(Id, Id)> = Vec::new();
    let mut uncertain: Vec<Id> = Vec::new();

    while !input.done() {
        let now = 1_000 + input.pos as i64;
        let txn = {
            next_id += 1;
            Id::from(next_id)
        };
        match input.u8() % 8 {
            0 => {
                next_id += 1;
                let event_id = Id::from(next_id);
                let stream = format!("s{}", input.u8() % 4);
                let _ = store.commit(
                    &CommitBatch::new(txn, now)
                        .append_event(Event::with_json_payload(event_id, &stream, "t", now, b"{}")),
                );
            }
            1 => {
                next_id += 1;
                let job_id = Id::from(next_id);
                let queue = format!("q{}", input.u8() % 2);
                let part = format!("p{}", input.u8() % 4);
                let _ = store.commit(&CommitBatch::new(txn, now).enqueue_job(
                    JobSpec::reconcilable(job_id, &queue, &part, vec![input.u8()]),
                ));
            }
            2 => {
                let queue = format!("q{}", input.u8() % 2);
                match store.claim_jobs(&ClaimRequest {
                    queue,
                    worker_id: "w".into(),
                    now_ms: now,
                    lease_ms: 1 + i64::from(input.u8()),
                    limit: 1 + usize::from(input.u8() % 4),
                }) {
                    Ok(ClaimOutcome::Committed(claims)) => {
                        for job in claims.into_jobs() {
                            leases.push((job.job_id, job.lease_token));
                        }
                    }
                    Ok(_) | Err(_) => {}
                }
            }
            3 => {
                if let Some((job_id, token)) = pick(&mut leases, input.u8()) {
                    let _ = store
                        .commit(&CommitBatch::new(txn, now).acknowledge_job(job_id, token, None));
                }
            }
            4 => {
                if let Some((job_id, token)) = pick(&mut leases, input.u8()) {
                    let retry = if input.u8() % 2 == 0 {
                        Some(now + i64::from(input.u8()))
                    } else {
                        None
                    };
                    let _ = store
                        .commit(&CommitBatch::new(txn, now).fail_job(job_id, token, "e", retry));
                }
            }
            5 => {
                if let Some((job_id, token)) = pick(&mut leases, input.u8()) {
                    let _ = store.extend_lease(job_id, token, now + i64::from(input.u8()), now);
                    leases.push((job_id, token));
                }
            }
            6 => {
                for job in store
                    .jobs(None, Some(minisqlite::JobState::Uncertain), 4)
                    .unwrap()
                {
                    uncertain.push(job.job_id);
                }
                if let Some(job_id) = uncertain.pop() {
                    let resolution = match input.u8() % 3 {
                        0 => Resolution::Retry,
                        1 => Resolution::MarkSucceeded,
                        _ => Resolution::MarkDead,
                    };
                    let _ = store.commit(
                        &CommitBatch::new(txn, now).resolve_uncertain_job(job_id, resolution),
                    );
                }
            }
            _ => {
                let projection = format!("proj{}", input.u8() % 2);
                let version = store.projection_version(&projection).unwrap();
                let _ = store.commit(&CommitBatch::new(txn, now).apply_projection_patch(
                    ProjectionPatch {
                        projection,
                        expected_version: version,
                        new_version: version + 1,
                        mutations: vec![ProjectionMutation::Put {
                            key: vec![input.u8()],
                            value: vec![input.u8()],
                        }],
                    },
                ));
            }
        }
    }

    let report = store.verify().unwrap();
    assert!(report.is_ok(), "verify findings: {:?}", report.findings);
});

fn pick(leases: &mut Vec<(Id, Id)>, byte: u8) -> Option<(Id, Id)> {
    if leases.is_empty() {
        return None;
    }
    let idx = usize::from(byte) % leases.len();
    Some(leases.swap_remove(idx))
}
