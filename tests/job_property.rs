use std::collections::HashSet;

use minisqlite::{ClaimRequest, CommitBatch, Durability, EffectMode, Id, JobSpec, StoreBuilder};

mod common;

fn now_ms() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_millis() as i64)
        .unwrap_or(0)
}

#[derive(Debug, Clone)]
struct EnqueuedJob {
    queue: String,
    partition: String,
    payload: Vec<u8>,
    not_before_ms: i64,
    max_attempts: u32,
    effect_mode: EffectMode,
}

fn rand_string(rng: &mut fastrand::Rng) -> String {
    let len = rng.usize(1..=8);
    let chars: Vec<char> = (0..len)
        .map(|_| {
            const CHARSET: &[u8] = b"abcdefghijklmnopqrstuvwxyz";
            CHARSET[rng.usize(..CHARSET.len())] as char
        })
        .collect();
    chars.into_iter().collect()
}

fn rand_job(rng: &mut fastrand::Rng) -> EnqueuedJob {
    EnqueuedJob {
        queue: rand_string(rng),
        partition: rand_string(rng),
        payload: (0..rng.usize(0..64)).map(|_| rng.u8(..)).collect(),
        not_before_ms: rng.i64(0..100),
        max_attempts: rng.u32(1..5),
        effect_mode: if rng.bool() {
            EffectMode::Idempotent
        } else {
            EffectMode::UncertainOnLeaseExpiry
        },
    }
}

#[test]
fn claimed_jobs_can_be_acknowledged() {
    for seed in 0..32 {
        let mut rng = fastrand::Rng::with_seed(seed);
        let job_count = rng.usize(1..20);
        let jobs: Vec<EnqueuedJob> = (0..job_count).map(|_| rand_job(&mut rng)).collect();

        let tmp = common::TempDir::new();
        let path = tmp.path().join("prop.mini");
        let store = StoreBuilder::new(&path)
            .durability(Durability::Memory)
            .open()
            .unwrap();

        let mut ids = Vec::new();
        let mut max_not_before = 0i64;
        for job in &jobs {
            max_not_before = max_not_before.max(job.not_before_ms);
            let spec = JobSpec::new(Id::new(), &job.queue, &job.partition, job.payload.clone())
                .with_not_before_ms(job.not_before_ms)
                .with_max_attempts(job.max_attempts)
                .with_effect_mode(job.effect_mode);
            ids.push(spec.job_id);
            store
                .commit(CommitBatch::new(Id::new(), now_ms()).enqueue_job(spec))
                .unwrap();
        }

        let t = max_not_before + 1;

        let queues: HashSet<String> = jobs.iter().map(|j| j.queue.clone()).collect();
        let mut claimed = std::collections::HashMap::new();
        for queue in queues {
            for job in store
                .claim_jobs(ClaimRequest {
                    queue: queue.clone(),
                    worker_id: "worker".into(),
                    now_ms: t,
                    lease_ms: 10_000,
                    limit: 100,
                })
                .unwrap()
            {
                claimed.insert(job.job_id, (job.lease_token, job.attempt));
            }
        }

        assert!(claimed.len() <= ids.len());

        for (&job_id, &(lease_token, _)) in &claimed {
            store
                .commit(CommitBatch::new(Id::new(), t).acknowledge_job(
                    job_id,
                    lease_token,
                    Some(b"ok".to_vec()),
                ))
                .unwrap();
        }

        for job_id in ids {
            if claimed.contains_key(&job_id) {
                assert_eq!(
                    store.job_state(job_id, t).unwrap(),
                    minisqlite::JobState::Succeeded
                );
            }
        }

        if let Some((&job_id, &(lease_token, _))) = claimed.iter().next() {
            assert!(store
                .commit(CommitBatch::new(Id::new(), t).acknowledge_job(job_id, lease_token, None),)
                .is_err());
        }
    }
}
