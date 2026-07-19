use std::collections::{BTreeMap, HashMap};
use std::time::{SystemTime, UNIX_EPOCH};

use minisqlite::{CommitBatch, Durability, Event, Id, JobSpec, StoreBuilder};
use proptest::prelude::*;

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

fn arb_string() -> impl Strategy<Value = String> {
    "[a-z0-9]{1,16}"
}

fn arb_payload() -> impl Strategy<Value = Vec<u8>> {
    proptest::collection::vec(any::<u8>(), 0..256)
}

fn arb_op() -> impl Strategy<Value = Op> {
    prop_oneof![
        (arb_string(), arb_payload())
            .prop_map(|(stream, payload)| Op::AppendEvent { stream, payload }),
        (arb_string(), any::<u64>(), arb_payload(), arb_payload()).prop_map(
            |(name, version, key, value)| Op::ProjectionPut {
                name,
                version,
                key,
                value,
            }
        ),
        (arb_string(), arb_string(), arb_payload()).prop_map(|(queue, partition, payload)| {
            Op::EnqueueJob {
                queue,
                partition,
                payload,
            }
        }),
        Just(Op::Reopen),
    ]
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(32))]

    #[test]
    fn store_matches_reference_model(ops in proptest::collection::vec(arb_op(), 1..30)) {
        let tmp = tempfile::tempdir().unwrap();
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
                    let event = Event::with_json_payload(Id::new(), &stream, "e", now_ms(), &payload);
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
                            .commit(
                                CommitBatch::new(Id::new(), now_ms())
                                    .projection_put(&name, version, key.clone(), value.clone()),
                            )
                            .is_err());
                        continue;
                    }
                    store
                        .commit(
                            CommitBatch::new(Id::new(), now_ms())
                                .projection_put(&name, version, key.clone(), value.clone()),
                        )
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
