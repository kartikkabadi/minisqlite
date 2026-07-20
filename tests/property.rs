use std::collections::{BTreeMap, HashSet};
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
    ProjectionPutNext {
        name: String,
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

fn rand_op(rng: &mut fastrand::Rng) -> Op {
    match rng.usize(0..5) {
        0 => Op::AppendEvent {
            stream: rand_string(rng, 12),
            payload: rand_bytes(rng, 128),
        },
        1 => Op::ProjectionPut {
            name: rand_string(rng, 12),
            version: rng.u64(0..=5),
            key: rand_bytes(rng, 32),
            value: rand_bytes(rng, 128),
        },
        2 => Op::ProjectionPutNext {
            name: rand_string(rng, 12),
            key: rand_bytes(rng, 32),
            value: rand_bytes(rng, 128),
        },
        3 => Op::EnqueueJob {
            queue: rand_string(rng, 8),
            partition: rand_string(rng, 8),
            payload: rand_bytes(rng, 64),
        },
        _ => Op::Reopen,
    }
}

#[derive(Debug, Default)]
struct Model {
    streams: BTreeMap<String, u64>,
    projections: BTreeMap<String, BTreeMap<Vec<u8>, Vec<u8>>>,
    projection_versions: BTreeMap<String, u64>,
    job_ids: HashSet<Id>,
    committed_transaction_ids: HashSet<Id>,
}

fn append_event_model(model: &mut Model, stream: &str) {
    *model.streams.entry(stream.to_string()).or_insert(0) += 1;
}

fn projection_put_model(model: &mut Model, name: &str, key: &[u8], value: &[u8]) {
    *model
        .projection_versions
        .entry(name.to_string())
        .or_insert(0) += 1;
    model
        .projections
        .entry(name.to_string())
        .or_default()
        .insert(key.to_vec(), value.to_vec());
}

#[test]
#[allow(clippy::explicit_counter_loop)]
fn store_matches_reference_model_through_reopen() {
    for seed in 0..128 {
        let mut rng = fastrand::Rng::with_seed(seed);
        let op_count = rng.usize(5..80);
        let ops: Vec<Op> = (0..op_count).map(|_| rand_op(&mut rng)).collect();

        let tmp = common::TempDir::new();
        let path = tmp.path().join("prop.mini");

        let mut store = StoreBuilder::new(&path)
            .durability(Durability::Memory)
            .open()
            .unwrap();

        let mut model = Model::default();
        let mut now = now_ms();

        for op in ops {
            match op {
                Op::AppendEvent { stream, payload } => {
                    let tx = Id::new();
                    let event = Event::with_json_payload(Id::new(), &stream, "e", now, &payload);
                    let batch = CommitBatch::new(tx, now)
                        .with_correlation_id(Id::new())
                        .with_metadata(rand_bytes(&mut rng, 32))
                        .append_event(event);
                    if let Ok(receipt) = store.commit(batch) {
                        model.committed_transaction_ids.insert(tx);
                        append_event_model(&mut model, &stream);
                        assert_eq!(receipt.transaction_id, tx);
                        let roundtrip = store.get_transaction(tx).unwrap();
                        assert_eq!(roundtrip.transaction_id, tx);
                        assert_eq!(roundtrip.stream_versions, receipt.stream_versions);
                    }
                }
                Op::ProjectionPut {
                    name,
                    version,
                    key,
                    value,
                } => {
                    let tx = Id::new();
                    let current = *model.projection_versions.get(&name).unwrap_or(&0);
                    let batch = CommitBatch::new(tx, now).projection_put(
                        &name,
                        version,
                        key.clone(),
                        value.clone(),
                    );
                    if version == current + 1 {
                        if let Ok(receipt) = store.commit(batch) {
                            model.committed_transaction_ids.insert(tx);
                            projection_put_model(&mut model, &name, &key, &value);
                            assert_eq!(receipt.transaction_id, tx);
                        }
                    } else {
                        // Version mismatch is a conflict; this branch also covers version zero.
                        assert!(store.commit(batch).is_err());
                    }
                }
                Op::ProjectionPutNext { name, key, value } => {
                    let tx = Id::new();
                    let current = *model.projection_versions.get(&name).unwrap_or(&0);
                    let batch = CommitBatch::new(tx, now).projection_put(
                        &name,
                        current + 1,
                        key.clone(),
                        value.clone(),
                    );
                    if let Ok(receipt) = store.commit(batch) {
                        model.committed_transaction_ids.insert(tx);
                        projection_put_model(&mut model, &name, &key, &value);
                        assert_eq!(receipt.transaction_id, tx);
                    }
                }
                Op::EnqueueJob {
                    queue,
                    partition,
                    payload,
                } => {
                    let tx = Id::new();
                    let job_id = Id::new();
                    let job = JobSpec::new(job_id, &queue, &partition, payload);
                    let batch = CommitBatch::new(tx, now).enqueue_job(job);
                    if let Ok(receipt) = store.commit(batch) {
                        model.committed_transaction_ids.insert(tx);
                        model.job_ids.insert(job_id);
                        assert_eq!(receipt.transaction_id, tx);
                        assert!(store.get_transaction(tx).is_ok());
                    }
                }
                Op::Reopen => {
                    drop(store);
                    store = StoreBuilder::new(&path)
                        .durability(Durability::Memory)
                        .open()
                        .unwrap();
                }
            }

            now += 1;
            assert_model(&store, &model, &path);
        }
    }
}

fn assert_model(store: &minisqlite::Store, model: &Model, path: &std::path::Path) {
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

    assert_eq!(store.stats().job_count, model.job_ids.len() as u64);

    for tx in &model.committed_transaction_ids {
        assert!(
            store.get_transaction(*tx).is_ok(),
            "committed transaction {tx} not found after reopen in {}",
            path.display()
        );
    }
}
