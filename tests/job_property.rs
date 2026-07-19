use minisqlite::{ClaimRequest, CommitBatch, Durability, EffectMode, Id, JobSpec, StoreBuilder};
use proptest::prelude::*;

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

fn arb_job() -> impl Strategy<Value = EnqueuedJob> {
    (
        "[a-z]{1,8}",
        "[a-z]{1,8}",
        proptest::collection::vec(any::<u8>(), 0..64),
        0i64..100i64,
        1u32..5u32,
        prop_oneof![
            Just(EffectMode::Idempotent),
            Just(EffectMode::UncertainOnLeaseExpiry)
        ],
    )
        .prop_map(
            |(queue, partition, payload, not_before_ms, max_attempts, effect_mode)| EnqueuedJob {
                queue,
                partition,
                payload,
                not_before_ms,
                max_attempts,
                effect_mode,
            },
        )
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(32))]

    #[test]
    fn claimed_jobs_can_be_acknowledged(jobs in proptest::collection::vec(arb_job(), 1..20)) {
        let tmp = tempfile::tempdir().unwrap();
        let path = tmp.path().join("prop.mini");
        let store = StoreBuilder::new(&path)
            .durability(Durability::Memory)
            .open()
            .unwrap();

        let mut ids = Vec::new();
        let mut max_not_before = 0i64;
        for job in &jobs {
            max_not_before = max_not_before.max(job.not_before_ms);
            let spec = JobSpec::new(
                Id::new(),
                &job.queue,
                &job.partition,
                job.payload.clone(),
            )
            .with_not_before_ms(job.not_before_ms)
            .with_max_attempts(job.max_attempts)
            .with_effect_mode(job.effect_mode);
            ids.push(spec.job_id);
            store
                .commit(CommitBatch::new(Id::new(), now_ms()).enqueue_job(spec))
                .unwrap();
        }

        // Advance time past all `not_before` values.
        let t = max_not_before + 1;

        // Claim everything that is ready, queue by queue.
        let queues: std::collections::HashSet<String> = jobs.iter().map(|j| j.queue.clone()).collect();
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

        // Every non-terminal, ready job should have been claimed at most once.
        assert!(claimed.len() <= ids.len());

        // Acknowledge each claimed job.
        for (&job_id, &(lease_token, _)) in &claimed {
            store
                .commit(
                    CommitBatch::new(Id::new(), t)
                        .acknowledge_job(job_id, lease_token, Some(b"ok".to_vec())),
                )
                .unwrap();
        }

        // All acknowledged jobs are succeeded.
        for job_id in ids {
            if claimed.contains_key(&job_id) {
                assert_eq!(store.job_state(job_id, t).unwrap(), minisqlite::JobState::Succeeded);
            }
        }

        // A stale lease token cannot ack a job that no longer has a lease.
        if let Some((&job_id, &(lease_token, _))) = claimed.iter().next() {
            assert!(store
                .commit(
                    CommitBatch::new(Id::new(), t)
                        .acknowledge_job(job_id, lease_token, None),
                )
                .is_err());
        }
    }
}
