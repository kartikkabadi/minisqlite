use std::sync::{mpsc, Arc};
mod common;
use std::thread;

use minisqlite::{ClaimRequest, CommitBatch, Durability, Event, Id, JobSpec, StoreBuilder};

#[test]
fn concurrent_commits_serialize() {
    let tmp = common::TempDir::new();
    let store = Arc::new(
        StoreBuilder::new(tmp.path().join("c.mini"))
            .durability(Durability::Memory)
            .open()
            .unwrap(),
    );

    let mut handles = Vec::new();
    for i in 0..20 {
        let s = store.clone();
        handles.push(thread::spawn(move || {
            let event = Event::with_json_payload(
                Id::new().unwrap(),
                "concurrent",
                "e",
                i as i64,
                format!("{{\"i\":{i}}}").as_bytes(),
            );
            s.commit(CommitBatch::new(Id::new().unwrap(), i as i64).append_event(event))
                .unwrap();
        }));
    }
    for h in handles {
        h.join().unwrap();
    }

    assert_eq!(store.high_water_sequence(), 20);
    let events = store.events_after(0, 100);
    assert_eq!(events.len(), 20);
}

#[test]
fn concurrent_stream_conflict_is_explicit() {
    let tmp = common::TempDir::new();
    let store = Arc::new(
        StoreBuilder::new(tmp.path().join("c.mini"))
            .durability(Durability::Memory)
            .open()
            .unwrap(),
    );

    // Pre-condition: stream at version 0.
    let mut senders = Vec::new();
    let mut handles = Vec::new();
    for _ in 0..5 {
        let (tx, rx) = mpsc::channel();
        senders.push(tx);
        let s = store.clone();
        handles.push(thread::spawn(move || {
            let event = Event::with_json_payload(Id::new().unwrap(), "stream", "e", 0, b"{}");
            let result = s.commit(
                CommitBatch::new(Id::new().unwrap(), 0)
                    .expect_stream_version("stream", 0)
                    .append_event(event),
            );
            let _ = rx.recv();
            result.is_ok()
        }));
    }

    // Release all threads at once to race.
    for tx in senders {
        tx.send(()).unwrap();
    }
    let results: Vec<bool> = handles.into_iter().map(|h| h.join().unwrap()).collect();
    assert_eq!(results.iter().filter(|&&b| b).count(), 1);
    assert_eq!(store.stream_version("stream"), Some(1));
}

#[test]
fn concurrent_reads_never_observe_half_commit() {
    let tmp = common::TempDir::new();
    let store = Arc::new(
        StoreBuilder::new(tmp.path().join("c.mini"))
            .durability(Durability::Memory)
            .open()
            .unwrap(),
    );

    let writer = store.clone();
    let reader = store.clone();

    let writer = thread::spawn(move || {
        for i in 0..100 {
            writer
                .commit(
                    CommitBatch::new(Id::new().unwrap(), i as i64)
                        .append_event(Event::with_json_payload(
                            Id::new().unwrap(),
                            "x",
                            "e",
                            i as i64,
                            b"{}",
                        ))
                        .projection_put(
                            "p",
                            i as u64 + 1,
                            b"k".to_vec(),
                            i.to_string().into_bytes(),
                        ),
                )
                .unwrap();
        }
    });

    // Reader observes a sequence of monotonically non-decreasing version values.
    let reader = thread::spawn(move || {
        let mut last_version = 0u64;
        for _ in 0..1000 {
            if let Ok(Some(v)) = reader
                .get_projection("p", b"k")
                .map(|o| o.map(|b| String::from_utf8_lossy(&b).parse().unwrap_or(0)))
            {
                assert!(
                    v >= last_version,
                    "projection version regressed: {last_version} -> {v}"
                );
                last_version = v;
            }
        }
    });

    writer.join().unwrap();
    reader.join().unwrap();
}

#[test]
fn concurrent_job_claims_do_not_duplicate_lease() {
    let tmp = common::TempDir::new();
    let store = Arc::new(
        StoreBuilder::new(tmp.path().join("c.mini"))
            .durability(Durability::Memory)
            .open()
            .unwrap(),
    );

    store
        .commit(
            CommitBatch::new(Id::new().unwrap(), 0).enqueue_job(JobSpec::new(
                Id::new().unwrap(),
                "q",
                "p",
                b"work".to_vec(),
            )),
        )
        .unwrap();

    let mut handles = Vec::new();
    for i in 0..10 {
        let s = store.clone();
        handles.push(thread::spawn(move || {
            s.claim_jobs(ClaimRequest {
                queue: "q".into(),
                worker_id: format!("worker-{i}"),
                now_ms: 0,
                lease_ms: 1000,
                limit: 1,
            })
        }));
    }

    let mut tokens = Vec::new();
    for h in handles {
        let claimed = h.join().unwrap().unwrap();
        for c in claimed {
            tokens.push(c.lease_token);
        }
    }

    // Only one lease token should have been issued for the one job.
    assert_eq!(tokens.len(), 1, "more than one concurrent claim succeeded");
}

#[test]
fn partition_ordering_is_stable_under_concurrent_claims() {
    let tmp = common::TempDir::new();
    let store = Arc::new(
        StoreBuilder::new(tmp.path().join("c.mini"))
            .durability(Durability::Memory)
            .open()
            .unwrap(),
    );

    let mut batch = CommitBatch::new(Id::new().unwrap(), 0);
    for partition in ["c", "a", "b"] {
        batch = batch.enqueue_job(JobSpec::new(
            Id::new().unwrap(),
            "q",
            partition,
            b"work".to_vec(),
        ));
    }
    store.commit(batch).unwrap();

    let mut claimed = store
        .claim_jobs(ClaimRequest {
            queue: "q".into(),
            worker_id: "w".into(),
            now_ms: 0,
            lease_ms: 1000,
            limit: 3,
        })
        .unwrap();

    assert_eq!(claimed.len(), 3);
    let partitions: Vec<_> = claimed.drain(..).map(|c| c.partition).collect();
    assert_eq!(partitions, vec!["a", "b", "c"]);
}
