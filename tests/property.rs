use std::collections::{BTreeMap, HashMap};
use std::time::{SystemTime, UNIX_EPOCH};

use minisqlite::{CommitBatch, Durability, Event, Id, JobSpec, StoreBuilder};

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
    ProjectionPut {
        name: String,
        version: u64,
        key: Vec<u8>,
        value: Vec<u8>,
    },
    EnqueueJob {
        queue: String,
        partition: String,
        payload: Vec<u8>,
    },
    Reopen,
}

fn rand_string(rng: &mut fastrand::Rng) -> String {
    let len = rng.usize(1..=16);
    let chars: Vec<char> = (0..len)
        .map(|_| {
            const CHARSET: &[u8] = b"abcdefghijklmnopqrstuvwxyz0123456789";
            CHARSET[rng.usize(..CHARSET.len())] as char
        })
        .collect();
    chars.into_iter().collect()
}

fn rand_payload(rng: &mut fastrand::Rng, max: usize) -> Vec<u8> {
    let len = rng.usize(0..max);
    (0..len).map(|_| rng.u8(..)).collect()
}

fn rand_op(rng: &mut fastrand::Rng) -> Op {
    match rng.usize(0..4) {
        0 => Op::AppendEvent {
            stream: rand_string(rng),
            payload: rand_payload(rng, 256),
        },
        1 => Op::ProjectionPut {
            name: rand_string(rng),
            version: rng.u64(..),
            key: rand_payload(rng, 64),
            value: rand_payload(rng, 64),
        },
        2 => Op::EnqueueJob {
            queue: rand_string(rng),
            partition: rand_string(rng),
            payload: rand_payload(rng, 64),
        },
        _ => Op::Reopen,
    }
}

#[test]
fn store_matches_reference_model() {
    for seed in 0..32 {
        let mut rng = fastrand::Rng::with_seed(seed);
        let op_count = rng.usize(1..30);
        let ops: Vec<Op> = (0..op_count).map(|_| rand_op(&mut rng)).collect();

        let tmp = common::TempDir::new();
        let path = tmp.path().join("prop.mini");

        let mut store = StoreBuilder::new(&path)
            .durability(Durability::Memory)
            .open()
            .unwrap();

        let mut model_streams: HashMap<String, u64> = HashMap::new();
        let mut model_projection_versions: HashMap<String, u64> = HashMap::new();
        let mut model_projections: HashMap<String, BTreeMap<Vec<u8>, Vec<u8>>> = HashMap::new();
        let mut model_job_count: u64 = 0;

        for op in ops {
            match op {
                Op::AppendEvent { stream, payload } => {
                    let event =
                        Event::with_json_payload(Id::new(), &stream, "e", now_ms(), &payload);
                    store
                        .commit(CommitBatch::new(Id::new(), now_ms()).append_event(event))
                        .unwrap();
                    *model_streams.entry(stream).or_insert(0) += 1;
                }
                Op::ProjectionPut {
                    name,
                    version,
                    key,
                    value,
                } => {
                    if version == 0 {
                        continue;
                    }
                    let current = *model_projection_versions.get(&name).unwrap_or(&0);
                    if version != current + 1 {
                        assert!(store
                            .commit(CommitBatch::new(Id::new(), now_ms()).projection_put(
                                &name,
                                version,
                                key.clone(),
                                value.clone()
                            ),)
                            .is_err());
                        continue;
                    }
                    store
                        .commit(CommitBatch::new(Id::new(), now_ms()).projection_put(
                            &name,
                            version,
                            key.clone(),
                            value.clone(),
                        ))
                        .unwrap();
                    model_projection_versions.insert(name.clone(), version);
                    model_projections
                        .entry(name)
                        .or_default()
                        .insert(key, value);
                }
                Op::EnqueueJob {
                    queue,
                    partition,
                    payload,
                } => {
                    let job = JobSpec::new(Id::new(), &queue, &partition, payload);
                    store
                        .commit(CommitBatch::new(Id::new(), now_ms()).enqueue_job(job))
                        .unwrap();
                    model_job_count += 1;
                }
                Op::Reopen => {
                    drop(store);
                    store = StoreBuilder::new(&path)
                        .durability(Durability::Memory)
                        .open()
                        .unwrap();
                }
            }

            for (stream, expected) in &model_streams {
                assert_eq!(
                    store.stream_version(stream),
                    Some(*expected),
                    "stream version for {}",
                    stream
                );
            }
            for (name, entries) in &model_projections {
                let expected_version = model_projection_versions[name];
                assert_eq!(store.projection_version(name).ok(), Some(expected_version));
                for (key, value) in entries {
                    assert_eq!(
                        store.get_projection(name, key).unwrap().as_deref(),
                        Some(value.as_slice()),
                        "projection {} key {:?}",
                        name,
                        key
                    );
                }
            }
            assert_eq!(store.stats().job_count, model_job_count);
        }
    }
}
