use std::collections::{BTreeMap, HashSet};
use std::time::{SystemTime, UNIX_EPOCH};

use minisqlite::{
    ClaimRequest, CommitBatch, Durability, EffectMode, Event, Id, JobInfo, JobSpec, JobState,
    Resolution, StoreBuilder,
};

mod common;

fn now_ms() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis() as i64)
        .unwrap_or(0)
}

#[derive(Debug, Clone)]
enum Op {
    AppendEvent {
        stream: String,
        payload: Vec<u8>,
    },
    ProjectionPutNext {
        name: String,
        key: Vec<u8>,
        value: Vec<u8>,
    },
    EnqueueJob {
        queue: String,
        partition: String,
        payload: Vec<u8>,
        effect_mode: EffectMode,
        max_attempts: u32,
    },
    ClaimJobs {
        queue: String,
        worker_id: String,
        lease_ms: i64,
        limit: usize,
    },
    AckJob {
        job_id: Id,
        token: Id,
        result_digest: Vec<u8>,
    },
    FailJob {
        job_id: Id,
        token: Id,
        retry_after_ms: Option<i64>,
    },
    CancelJob {
        job_id: Id,
        token: Option<Id>,
    },
    ResolveJob {
        job_id: Id,
        resolution: Resolution,
    },
    Reopen,
}

fn rand_string(rng: &mut fastrand::Rng, max_len: usize) -> String {
    let len = rng.usize(1..=max_len);
    const CHARSET: &[u8] = b"abcdefghijklmnopqrstuvwxyz0123456789";
    (0..len)
        .map(|_| CHARSET[rng.usize(..CHARSET.len())] as char)
        .collect()
}

fn rand_bytes(rng: &mut fastrand::Rng, max: usize) -> Vec<u8> {
    let len = rng.usize(0..max);
    (0..len).map(|_| rng.u8(..)).collect()
}

fn rand_valid_op(rng: &mut fastrand::Rng, model: &Model, now: i64) -> Op {
    if model.jobs.is_empty() {
        return Op::EnqueueJob {
            queue: rand_string(rng, 8),
            partition: rand_string(rng, 8),
            payload: rand_bytes(rng, 64),
            effect_mode: if rng.bool() {
                EffectMode::Idempotent
            } else {
                EffectMode::UncertainOnLeaseExpiry
            },
            max_attempts: rng.u32(1..=3),
        };
    }

    let ack_candidates: Vec<Id> = model
        .jobs
        .iter()
        .filter(|(_, j)| {
            matches!(j.internal_state, InternalState::Leased) && now < j.lease_expires_at_ms
        })
        .map(|(id, _)| *id)
        .collect();

    // `fail_job` succeeds when the lease is still active, or when the lease has expired
    // and the job is at the attempt ceiling and idempotent. Uncertain expired jobs can
    // only be resolved, not failed.
    let fail_candidates: Vec<Id> = model
        .jobs
        .iter()
        .filter(|(_, j)| {
            matches!(j.internal_state, InternalState::Leased)
                && (now < j.lease_expires_at_ms
                    || (j.attempt >= j.spec.max_attempts
                        && j.spec.effect_mode == EffectMode::Idempotent))
        })
        .map(|(id, _)| *id)
        .collect();

    let cancel_candidates: Vec<Id> = model
        .jobs
        .iter()
        .filter(|(_, j)| match j.internal_state {
            InternalState::Leased => now < j.lease_expires_at_ms,
            InternalState::Pending | InternalState::RetryWait => true,
            _ => false,
        })
        .map(|(id, _)| *id)
        .collect();

    let resolve_candidates: Vec<Id> = model
        .jobs
        .iter()
        .filter(|(_, j)| {
            matches!(j.internal_state, InternalState::Leased)
                && now >= j.lease_expires_at_ms
                && j.spec.effect_mode == EffectMode::UncertainOnLeaseExpiry
        })
        .map(|(id, _)| *id)
        .collect();

    match rng.usize(0..10) {
        0 => Op::AppendEvent {
            stream: rand_string(rng, 12),
            payload: rand_bytes(rng, 128),
        },
        1 => Op::ProjectionPutNext {
            name: rand_string(rng, 12),
            key: rand_bytes(rng, 32),
            value: rand_bytes(rng, 128),
        },
        2 => Op::EnqueueJob {
            queue: rand_string(rng, 8),
            partition: rand_string(rng, 8),
            payload: rand_bytes(rng, 64),
            effect_mode: if rng.bool() {
                EffectMode::Idempotent
            } else {
                EffectMode::UncertainOnLeaseExpiry
            },
            max_attempts: rng.u32(1..=3),
        },
        3 => Op::ClaimJobs {
            queue: model
                .queue_partitions
                .keys()
                .map(|(q, _)| q.clone())
                .next()
                .unwrap_or_else(|| rand_string(rng, 8)),
            worker_id: rand_string(rng, 8),
            lease_ms: rng.u64(1..=100) as i64,
            limit: rng.usize(1..=3),
        },
        4 | 5 if !ack_candidates.is_empty() => {
            let id = ack_candidates[rng.usize(..ack_candidates.len())];
            let token = model.jobs[&id].lease_token.unwrap();
            Op::AckJob {
                job_id: id,
                token,
                result_digest: rand_bytes(rng, 32),
            }
        }
        6 | 7 if !fail_candidates.is_empty() => {
            let id = fail_candidates[rng.usize(..fail_candidates.len())];
            let token = model.jobs[&id].lease_token.unwrap();
            Op::FailJob {
                job_id: id,
                token,
                retry_after_ms: if rng.bool() {
                    None
                } else {
                    Some(now + rng.u64(100..=1000) as i64)
                },
            }
        }
        8 if !cancel_candidates.is_empty() => {
            let id = cancel_candidates[rng.usize(..cancel_candidates.len())];
            let job = &model.jobs[&id];
            let token = if matches!(job.internal_state, InternalState::Leased)
                && now < job.lease_expires_at_ms
            {
                job.lease_token
            } else {
                None
            };
            Op::CancelJob { job_id: id, token }
        }
        9 if !resolve_candidates.is_empty() => {
            let id = resolve_candidates[rng.usize(..resolve_candidates.len())];
            let resolution = match rng.usize(0..3) {
                0 => Resolution::Retry,
                1 => Resolution::MarkSucceeded,
                _ => Resolution::MarkDead,
            };
            Op::ResolveJob {
                job_id: id,
                resolution,
            }
        }
        9 => Op::Reopen,
        _ => Op::EnqueueJob {
            queue: rand_string(rng, 8),
            partition: rand_string(rng, 8),
            payload: rand_bytes(rng, 64),
            effect_mode: if rng.bool() {
                EffectMode::Idempotent
            } else {
                EffectMode::UncertainOnLeaseExpiry
            },
            max_attempts: rng.u32(1..=3),
        },
    }
}

#[derive(Debug, Default)]
struct Model {
    streams: BTreeMap<String, u64>,
    projections: BTreeMap<String, BTreeMap<Vec<u8>, Vec<u8>>>,
    projection_versions: BTreeMap<String, u64>,
    queue_partitions: BTreeMap<(String, String), Vec<Id>>,
    jobs: BTreeMap<Id, ModelJob>,
    committed_transaction_ids: HashSet<Id>,
}

#[derive(Debug, Clone)]
struct ModelJob {
    spec: JobSpec,
    internal_state: InternalState,
    lease_token: Option<Id>,
    worker_id: Option<String>,
    attempt: u32,
    lease_expires_at_ms: i64,
    retry_after_ms: i64,
    terminal_at_ms: Option<i64>,
    result_digest: Option<Vec<u8>>,
    error_summary: Option<String>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum InternalState {
    Pending,
    Leased,
    RetryWait,
    Succeeded,
    Dead,
    Cancelled,
}

impl ModelJob {
    fn is_terminal(&self) -> bool {
        matches!(
            self.internal_state,
            InternalState::Succeeded | InternalState::Dead | InternalState::Cancelled
        )
    }

    fn state_at(&self, now_ms: i64) -> JobState {
        match self.internal_state {
            InternalState::Pending => JobState::Pending,
            InternalState::Leased => {
                if now_ms >= self.lease_expires_at_ms
                    && self.spec.effect_mode == EffectMode::UncertainOnLeaseExpiry
                {
                    JobState::Uncertain
                } else {
                    JobState::Leased
                }
            }
            InternalState::RetryWait => {
                if now_ms < self.retry_after_ms {
                    JobState::RetryWait
                } else {
                    JobState::Pending
                }
            }
            InternalState::Succeeded => JobState::Succeeded,
            InternalState::Dead => JobState::Dead,
            InternalState::Cancelled => JobState::Cancelled,
        }
    }

    fn is_ready_at(&self, now_ms: i64) -> bool {
        if self.is_terminal() || self.attempt >= self.spec.max_attempts {
            return false;
        }
        match self.internal_state {
            InternalState::Pending => now_ms >= self.spec.not_before_ms,
            InternalState::RetryWait => now_ms >= self.retry_after_ms,
            InternalState::Leased => {
                now_ms >= self.lease_expires_at_ms
                    && self.spec.effect_mode == EffectMode::Idempotent
            }
            _ => false,
        }
    }

    fn is_expired_at_attempt_limit(&self, now_ms: i64) -> bool {
        matches!(self.internal_state, InternalState::Leased)
            && now_ms >= self.lease_expires_at_ms
            && self.spec.effect_mode == EffectMode::Idempotent
            && self.attempt >= self.spec.max_attempts
    }
}

#[test]
fn store_matches_reference_model_through_reopen() {
    for seed in 0..128 {
        let mut rng = fastrand::Rng::with_seed(seed);
        let op_count = rng.usize(5..80);

        let tmp = common::TempDir::new();
        let path = tmp.path().join("prop.mini");

        let mut store = StoreBuilder::new(&path)
            .durability(Durability::Memory)
            .open()
            .unwrap();

        let mut model = Model::default();
        let mut now = now_ms();

        for _ in 0..op_count {
            now += rng.u64(1..=100) as i64;
            let op = rand_valid_op(&mut rng, &model, now);

            store = execute_op(store, &mut model, &op, now, &path);
            assert_model(&store, &model, now, &path);
        }
    }
}

fn execute_op(
    store: minisqlite::Store,
    model: &mut Model,
    op: &Op,
    now: i64,
    path: &std::path::Path,
) -> minisqlite::Store {
    match op.clone() {
        Op::AppendEvent { stream, payload } => {
            let tx = Id::new().unwrap();
            let event = Event::with_json_payload(Id::new().unwrap(), &stream, "e", now, &payload);
            let batch = CommitBatch::new(tx, now).append_event(event);
            let receipt = store.commit(batch).unwrap();
            model.committed_transaction_ids.insert(tx);
            assert_eq!(receipt.transaction_id, tx);
            *model.streams.entry(stream).or_insert(0) += 1;
        }
        Op::ProjectionPutNext { name, key, value } => {
            let tx = Id::new().unwrap();
            let current = *model.projection_versions.get(&name).unwrap_or(&0);
            let batch = CommitBatch::new(tx, now).projection_put(
                &name,
                current + 1,
                key.clone(),
                value.clone(),
            );
            let receipt = store.commit(batch).unwrap();
            model.committed_transaction_ids.insert(tx);
            assert_eq!(receipt.transaction_id, tx);
            *model.projection_versions.entry(name.clone()).or_insert(0) += 1;
            model
                .projections
                .entry(name)
                .or_default()
                .insert(key, value);
        }
        Op::EnqueueJob {
            queue,
            partition,
            payload,
            effect_mode,
            max_attempts,
        } => {
            let tx = Id::new().unwrap();
            let job_id = Id::new().unwrap();
            let job = JobSpec::new(job_id, &queue, &partition, payload)
                .with_effect_mode(effect_mode)
                .with_max_attempts(max_attempts);
            let batch = CommitBatch::new(tx, now).enqueue_job(job.clone());
            let receipt = store.commit(batch).unwrap();
            model.committed_transaction_ids.insert(tx);
            assert_eq!(receipt.transaction_id, tx);
            model
                .queue_partitions
                .entry((queue, partition))
                .or_default()
                .push(job_id);
            model.jobs.insert(
                job_id,
                ModelJob {
                    spec: job,
                    internal_state: InternalState::Pending,
                    lease_token: None,
                    worker_id: None,
                    attempt: 0,
                    lease_expires_at_ms: 0,
                    retry_after_ms: 0,
                    terminal_at_ms: None,
                    result_digest: None,
                    error_summary: None,
                },
            );
        }
        Op::ClaimJobs {
            queue,
            worker_id,
            lease_ms,
            limit,
        } => {
            let expected = model_claimed_ids(model, &queue, now, limit);
            let claimed = store
                .claim_jobs(ClaimRequest {
                    queue: queue.clone(),
                    worker_id: worker_id.clone(),
                    now_ms: now,
                    lease_ms,
                    limit,
                })
                .unwrap();
            let claimed_ids: Vec<Id> = claimed.iter().map(|c| c.job_id).collect();
            assert_eq!(
                claimed_ids, expected,
                "claim_jobs did not match the reference model"
            );
            for c in claimed {
                let job = model.jobs.get_mut(&c.job_id).unwrap();
                job.internal_state = InternalState::Leased;
                job.lease_token = Some(c.lease_token);
                job.worker_id = Some(worker_id.clone());
                job.attempt = c.attempt;
                job.lease_expires_at_ms = c.lease_expires_at_ms;
            }
        }
        Op::AckJob {
            job_id,
            token,
            result_digest,
        } => {
            let receipt = store
                .ack_job(job_id, token, Some(result_digest.clone()), now)
                .unwrap();
            model
                .committed_transaction_ids
                .insert(receipt.transaction_id);
            let job = model.jobs.get_mut(&job_id).unwrap();
            job.internal_state = InternalState::Succeeded;
            job.terminal_at_ms = Some(now);
            job.result_digest = Some(result_digest);
            job.lease_token = None;
            job.worker_id = None;
            job.lease_expires_at_ms = 0;
        }
        Op::FailJob {
            job_id,
            token,
            retry_after_ms,
        } => {
            let receipt = store
                .fail_job(job_id, token, "boom", retry_after_ms, now)
                .unwrap();
            model
                .committed_transaction_ids
                .insert(receipt.transaction_id);
            let job = model.jobs.get_mut(&job_id).unwrap();
            let terminal = job.attempt >= job.spec.max_attempts;
            job.error_summary = Some("boom".into());
            job.lease_token = None;
            job.worker_id = None;
            job.lease_expires_at_ms = 0;
            if terminal {
                job.internal_state = InternalState::Dead;
                job.terminal_at_ms = Some(now);
                job.retry_after_ms = 0;
            } else {
                job.internal_state = InternalState::RetryWait;
                job.retry_after_ms = retry_after_ms.unwrap_or(now + 1000);
            }
        }
        Op::CancelJob { job_id, token } => {
            let receipt = store.cancel_job(job_id, token, now).unwrap();
            model
                .committed_transaction_ids
                .insert(receipt.transaction_id);
            let job = model.jobs.get_mut(&job_id).unwrap();
            job.internal_state = InternalState::Cancelled;
            job.terminal_at_ms = Some(now);
            job.lease_token = None;
            job.worker_id = None;
            job.lease_expires_at_ms = 0;
            job.retry_after_ms = 0;
        }
        Op::ResolveJob { job_id, resolution } => {
            let receipt = store
                .resolve_uncertain_job(job_id, resolution, now)
                .unwrap();
            model
                .committed_transaction_ids
                .insert(receipt.transaction_id);
            let job = model.jobs.get_mut(&job_id).unwrap();
            job.lease_token = None;
            job.worker_id = None;
            job.lease_expires_at_ms = 0;
            match resolution {
                Resolution::Retry => {
                    job.attempt = job.attempt.checked_sub(1).unwrap();
                    job.internal_state = InternalState::RetryWait;
                    job.retry_after_ms = now + 1000;
                    job.terminal_at_ms = None;
                }
                Resolution::MarkSucceeded => {
                    job.internal_state = InternalState::Succeeded;
                    job.terminal_at_ms = Some(now);
                    job.retry_after_ms = 0;
                }
                Resolution::MarkDead => {
                    job.internal_state = InternalState::Dead;
                    job.terminal_at_ms = Some(now);
                    job.retry_after_ms = 0;
                }
            }
        }
        Op::Reopen => {
            drop(store);
            return StoreBuilder::new(path).open().unwrap();
        }
    }
    store
}

fn model_claimed_ids(model: &mut Model, queue: &str, now: i64, limit: usize) -> Vec<Id> {
    let mut claimed = Vec::new();
    for ((q, _p), ids) in &model.queue_partitions {
        if q != queue || claimed.len() >= limit {
            continue;
        }
        for job_id in ids {
            let job = model.jobs.get_mut(job_id).expect("missing job in model");
            if job.is_terminal() {
                continue;
            }
            if job.is_expired_at_attempt_limit(now) {
                job.internal_state = InternalState::Dead;
                job.terminal_at_ms = Some(now);
                // Internal maintenance uses a fixed-size `JobExpire` record with no summary.
                job.error_summary = None;
                job.lease_token = None;
                job.worker_id = None;
                job.lease_expires_at_ms = 0;
                job.retry_after_ms = 0;
                continue;
            }
            if job.is_ready_at(now) {
                claimed.push(*job_id);
                break;
            } else {
                break;
            }
        }
    }
    claimed
}

fn assert_model(store: &minisqlite::Store, model: &Model, now: i64, path: &std::path::Path) {
    for (stream, expected) in &model.streams {
        assert_eq!(
            store.stream_version(stream),
            Some(*expected),
            "stream version mismatch for {stream}"
        );
    }

    for (name, expected_version) in &model.projection_versions {
        assert_eq!(
            store.projection_version(name).ok(),
            Some(*expected_version),
            "projection version mismatch for {name}"
        );
    }

    for (name, entries) in &model.projections {
        for (key, expected_value) in entries {
            assert_eq!(
                store.get_projection(name, key).unwrap().as_deref(),
                Some(expected_value.as_slice()),
                "projection {name} key {key:?} mismatch"
            );
        }
    }

    let jobs = store.jobs(now, None, None);
    let job_map: BTreeMap<Id, JobInfo> = jobs.into_iter().map(|j| (j.job_id, j)).collect();

    for (job_id, expected) in &model.jobs {
        let actual = job_map
            .get(job_id)
            .unwrap_or_else(|| panic!("job {job_id} missing in store at {}", path.display()));
        let expected_state = expected.state_at(now);
        assert_eq!(
            actual.state, expected_state,
            "job state mismatch for {job_id}"
        );
        assert_eq!(
            actual.attempt, expected.attempt,
            "attempt mismatch for {job_id}"
        );
        assert_eq!(
            actual.lease_expires_at_ms,
            if expected.is_terminal() || expected.lease_expires_at_ms == 0 {
                None
            } else {
                Some(expected.lease_expires_at_ms)
            },
            "lease expiry mismatch for {job_id}"
        );
        assert_eq!(
            actual.worker_id,
            if expected.is_terminal() {
                None
            } else {
                expected.worker_id.clone()
            },
            "worker_id mismatch for {job_id}"
        );
        assert_eq!(
            actual.retry_after_ms,
            if expected.is_terminal() || expected.retry_after_ms == 0 {
                None
            } else {
                Some(expected.retry_after_ms)
            },
            "retry_after mismatch for {job_id}"
        );
        assert_eq!(
            actual.terminal_at_ms, expected.terminal_at_ms,
            "terminal_at mismatch for {job_id}"
        );
        assert_eq!(
            actual.result_digest, expected.result_digest,
            "result_digest mismatch for {job_id}"
        );
        assert_eq!(
            actual.error_summary, expected.error_summary,
            "error_summary mismatch for {job_id}"
        );
        assert_eq!(actual.spec, expected.spec, "spec mismatch for {job_id}");
    }

    assert_eq!(
        store.stats().job_count,
        model.jobs.len() as u64,
        "job count mismatch"
    );

    for tx in &model.committed_transaction_ids {
        assert!(
            store.get_transaction(*tx).is_ok(),
            "committed transaction {tx} not found after reopen in {}",
            path.display()
        );
    }
}
