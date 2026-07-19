use std::time::{SystemTime, UNIX_EPOCH};

use minisqlite::{
    ClaimRequest, CommitBatch, Durability, EffectMode, Event, Id, JobSpec, JobState,
    ProjectionEntry, Resolution, StoreBuilder, StreamVersion,
};

fn tmp_path(name: &str) -> std::path::PathBuf {
    let dir = std::env::temp_dir().join(format!("minisqlite_int_{}", std::process::id()));
    std::fs::create_dir_all(&dir).unwrap();
    dir.join(name)
}

fn now_ms() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis() as i64)
        .unwrap_or(0)
}

#[test]
fn roundtrip_event_and_projection() {
    let path = tmp_path("roundtrip.mini");
    let _ = std::fs::remove_file(&path);

    let store = StoreBuilder::new(&path)
        .durability(Durability::Memory)
        .open()
        .unwrap();

    let tx = minisqlite::Id::new();
    let event = Event::new(
        minisqlite::Id::new(),
        "user:1",
        "user.created",
        1,
        now_ms(),
        None,
        None,
        b"{}",
        b"",
    );
    let receipt = store
        .commit(
            CommitBatch::new(tx, now_ms())
                .append_event(event.clone())
                .projection_put("users", 1, b"user:1".to_vec(), b"{}".to_vec()),
        )
        .unwrap();

    assert_eq!(receipt.first_event_sequence, Some(1));
    assert_eq!(receipt.last_event_sequence, Some(1));
    assert_eq!(
        receipt.stream_versions,
        vec![StreamVersion::new("user:1", 1)]
    );
    assert_eq!(store.high_water_sequence(), 1);
    assert_eq!(store.stream_version("user:1"), Some(1));
    assert_eq!(store.projection_version("users").unwrap(), 1);
    assert_eq!(
        store.get_projection("users", b"user:1").unwrap(),
        Some(b"{}".to_vec())
    );

    drop(store);

    let store = StoreBuilder::new(&path)
        .durability(Durability::Memory)
        .open()
        .unwrap();
    assert_eq!(store.high_water_sequence(), 1);
    assert_eq!(
        store.get_projection("users", b"user:1").unwrap(),
        Some(b"{}".to_vec())
    );
    let events = store.events_after(0, 10);
    assert_eq!(events.len(), 1);
    assert_eq!(events[0].event.event_id, event.event_id);

    let _ = std::fs::remove_file(&path);
}

#[test]
fn stream_version_conflict() {
    let path = tmp_path("conflict.mini");
    let _ = std::fs::remove_file(&path);
    let store = StoreBuilder::new(&path)
        .durability(Durability::Memory)
        .open()
        .unwrap();

    let event = Event::new(
        minisqlite::Id::new(),
        "stream",
        "a",
        1,
        now_ms(),
        None,
        None,
        b"",
        b"",
    );
    store
        .commit(CommitBatch::new(minisqlite::Id::new(), now_ms()).append_event(event))
        .unwrap();

    let bad = Event::new(
        minisqlite::Id::new(),
        "stream",
        "b",
        1,
        now_ms(),
        None,
        None,
        b"",
        b"",
    );
    let result = store.commit(
        CommitBatch::new(minisqlite::Id::new(), now_ms())
            .expect_stream_version("stream", 0)
            .append_event(bad),
    );
    assert!(matches!(result, Err(minisqlite::Error::Conflict { .. })));
    let _ = std::fs::remove_file(&path);
}

#[test]
fn job_lifecycle() {
    let path = tmp_path("jobs.mini");
    let _ = std::fs::remove_file(&path);
    let store = StoreBuilder::new(&path)
        .durability(Durability::Memory)
        .open()
        .unwrap();

    let job = JobSpec::new(
        minisqlite::Id::new(),
        "queue",
        "part-a",
        b"payload".to_vec(),
    );
    let job_id = job.job_id;
    let receipt = store
        .commit(CommitBatch::new(minisqlite::Id::new(), now_ms()).enqueue_job(job))
        .unwrap();
    assert_eq!(receipt.job_ids, vec![job_id]);

    let mut request = minisqlite::ClaimRequest {
        queue: "queue".into(),
        worker_id: "worker-1".into(),
        lease_ms: 60_000,
        limit: 1,
        now_ms: now_ms(),
    };
    let claimed = store.claim_jobs(request.clone()).unwrap();
    assert_eq!(claimed.len(), 1);
    assert_eq!(claimed[0].job_id, job_id);
    assert_eq!(claimed[0].attempt, 1);

    // No double lease.
    request.now_ms += 1;
    let claimed2 = store.claim_jobs(request).unwrap();
    assert!(claimed2.is_empty());

    // Acknowledge.
    store
        .ack_job(job_id, claimed[0].lease_token, None, now_ms())
        .unwrap();
    assert!(matches!(
        store.job_state(job_id, now_ms()).unwrap(),
        minisqlite::JobState::Succeeded
    ));

    drop(store);
    let store = StoreBuilder::new(&path)
        .durability(Durability::Memory)
        .open()
        .unwrap();
    assert!(matches!(
        store.job_state(job_id, now_ms()).unwrap(),
        minisqlite::JobState::Succeeded
    ));

    let _ = std::fs::remove_file(&path);
}

#[test]
fn projection_version_mismatch() {
    let path = tmp_path("proj.mini");
    let _ = std::fs::remove_file(&path);
    let store = StoreBuilder::new(&path)
        .durability(Durability::Memory)
        .open()
        .unwrap();

    store
        .commit(
            CommitBatch::new(minisqlite::Id::new(), now_ms()).projection_put(
                "p",
                1,
                b"k".to_vec(),
                b"v".to_vec(),
            ),
        )
        .unwrap();

    let result = store.commit(
        CommitBatch::new(minisqlite::Id::new(), now_ms()).projection_put(
            "p",
            3,
            b"k".to_vec(),
            b"v2".to_vec(),
        ),
    );
    assert!(matches!(
        result,
        Err(minisqlite::Error::ProjectionVersionMismatch { .. })
    ));
    let _ = std::fs::remove_file(&path);
}

#[test]
fn backup_and_verify() {
    let path = tmp_path("backup_src.mini");
    let dest = tmp_path("backup_dest.mini");
    let _ = std::fs::remove_file(&path);
    let _ = std::fs::remove_file(&dest);

    let store = StoreBuilder::new(&path)
        .durability(Durability::Strict)
        .open()
        .unwrap();
    store
        .commit(
            CommitBatch::new(minisqlite::Id::new(), now_ms())
                .append_event(Event::new(
                    minisqlite::Id::new(),
                    "s",
                    "e",
                    1,
                    now_ms(),
                    None,
                    None,
                    b"",
                    b"",
                ))
                .projection_replace("p", 1, [ProjectionEntry::new(b"k".to_vec(), b"v".to_vec())]),
        )
        .unwrap();
    store.backup(&dest).unwrap();

    let backup = StoreBuilder::new(&dest)
        .durability(Durability::Memory)
        .open()
        .unwrap();
    assert_eq!(backup.high_water_sequence(), 1);
    assert_eq!(backup.projection_version("p").unwrap(), 1);

    let _ = std::fs::remove_file(&path);
    let _ = std::fs::remove_file(&dest);
}

#[test]
fn reopen_recovers_multiple_frames() {
    let path = tmp_path("reopen.mini");
    let _ = std::fs::remove_file(&path);
    let store = StoreBuilder::new(&path)
        .durability(Durability::Memory)
        .open()
        .unwrap();

    for i in 0..5 {
        let event = Event::new(
            minisqlite::Id::new(),
            "s",
            "e",
            1,
            now_ms() + i,
            None,
            None,
            &i.to_le_bytes(),
            b"",
        );
        store
            .commit(CommitBatch::new(minisqlite::Id::new(), now_ms() + i).append_event(event))
            .unwrap();
    }

    drop(store);
    let store = StoreBuilder::new(&path)
        .durability(Durability::Memory)
        .open()
        .unwrap();
    assert_eq!(store.high_water_sequence(), 5);
    let events = store.events_after(0, 10);
    assert_eq!(events.len(), 5);

    let _ = std::fs::remove_file(&path);
}

#[test]
fn recovery_truncates_incomplete_tail() {
    let path = tmp_path("tail.mini");
    let _ = std::fs::remove_file(&path);
    let store = StoreBuilder::new(&path)
        .durability(Durability::Memory)
        .open()
        .unwrap();
    store
        .commit(
            CommitBatch::new(minisqlite::Id::new(), now_ms()).append_event(Event::new(
                minisqlite::Id::new(),
                "s",
                "e",
                1,
                now_ms(),
                None,
                None,
                b"",
                b"",
            )),
        )
        .unwrap();
    drop(store);

    // Append some garbage to the end of the file.
    let mut f = std::fs::OpenOptions::new()
        .append(true)
        .open(&path)
        .unwrap();
    use std::io::Write;
    f.write_all(b"garbage").unwrap();
    drop(f);

    let store = StoreBuilder::new(&path)
        .durability(Durability::Memory)
        .open()
        .unwrap();
    assert_eq!(store.high_water_sequence(), 1);
    let stats = store.stats();
    assert!(stats.recovered_tail);

    let _ = std::fs::remove_file(&path);
}

#[test]
fn synara_shaped_flows() {
    let path = tmp_path("synara.mini");
    let _ = std::fs::remove_file(&path);
    let store = StoreBuilder::new(&path)
        .durability(Durability::Strict)
        .open()
        .unwrap();

    let thread_id = Id::new();
    let stream = format!("thread:{thread_id}");

    // Flow A.
    let created = Event::with_json_payload(
        Id::new(),
        &stream,
        "thread.created",
        now_ms(),
        br#"{"title":"hello"}"#,
    );
    let receipt = store
        .commit(
            CommitBatch::new(Id::new(), now_ms())
                .append_event(created)
                .projection_put(
                    "threads",
                    1,
                    thread_id.to_string().into_bytes(),
                    br#"{"status":"idle"}"#.to_vec(),
                ),
        )
        .unwrap();
    assert_eq!(receipt.first_event_sequence, Some(1));
    assert_eq!(store.stream_version(&stream), Some(1));

    // Flow B.
    let requested =
        Event::with_json_payload(Id::new(), &stream, "thread.turn-requested", now_ms(), b"");
    let job = JobSpec::new(
        Id::new(),
        "provider",
        stream.clone(),
        thread_id.to_string().into_bytes(),
    );
    let job_id = job.job_id;
    store
        .commit(
            CommitBatch::new(Id::new(), now_ms())
                .expect_stream_version(&stream, 1)
                .append_event(requested)
                .projection_put(
                    "threads",
                    2,
                    thread_id.to_string().into_bytes(),
                    br#"{"status":"queued"}"#.to_vec(),
                )
                .enqueue_job(job),
        )
        .unwrap();
    assert_eq!(store.stream_version(&stream), Some(2));
    let val = store
        .get_projection("threads", &thread_id.to_string().into_bytes())
        .unwrap()
        .unwrap();
    assert!(val.windows(6).any(|w| w == b"queued"));

    // Flow C.
    let claim = ClaimRequest {
        queue: "provider".into(),
        worker_id: "worker-1".into(),
        now_ms: now_ms(),
        lease_ms: 60_000,
        limit: 1,
    };
    let claimed = store.claim_jobs(claim).unwrap();
    assert_eq!(claimed.len(), 1);
    let token = claimed[0].lease_token;

    let completed =
        Event::with_json_payload(Id::new(), &stream, "thread.turn-completed", now_ms(), b"");
    store
        .commit(
            CommitBatch::new(Id::new(), now_ms())
                .expect_stream_version(&stream, 2)
                .append_event(completed)
                .projection_put(
                    "threads",
                    3,
                    thread_id.to_string().into_bytes(),
                    br#"{"status":"idle"}"#.to_vec(),
                )
                .acknowledge_job(job_id, token, None),
        )
        .unwrap();
    assert!(matches!(
        store.job_state(job_id, now_ms()).unwrap(),
        JobState::Succeeded
    ));
    assert!(store.ack_job(job_id, token, None, now_ms()).is_err());

    // Flow D.
    let uncertain_job = JobSpec::new(Id::new(), "provider", "partition-2", b"call-api".to_vec())
        .with_effect_mode(EffectMode::UncertainOnLeaseExpiry)
        .with_max_attempts(1);
    let uncertain_id = uncertain_job.job_id;
    store
        .commit(CommitBatch::new(Id::new(), now_ms()).enqueue_job(uncertain_job))
        .unwrap();
    let claim2 = ClaimRequest {
        queue: "provider".into(),
        worker_id: "worker-2".into(),
        now_ms: now_ms(),
        lease_ms: 1000,
        limit: 1,
    };
    assert_eq!(store.claim_jobs(claim2.clone()).unwrap().len(), 1);
    let mut later = claim2;
    later.now_ms += 2000;
    assert!(store.claim_jobs(later).unwrap().is_empty());
    assert!(matches!(
        store.job_state(uncertain_id, now_ms() + 2000).unwrap(),
        JobState::Uncertain
    ));
    store
        .resolve_uncertain_job(uncertain_id, Resolution::MarkSucceeded, now_ms() + 2000)
        .unwrap();

    // Flow E.
    let loop_id = Id::new();
    let loop_stream = format!("loop:{loop_id}");
    let iteration =
        Event::with_json_payload(Id::new(), &loop_stream, "loop.iteration", now_ms(), b"1");
    let next_job = JobSpec::new(Id::new(), "loop", loop_id.to_string(), b"next".to_vec())
        .with_not_before_ms(now_ms() + 10_000);
    let next_id = next_job.job_id;
    store
        .commit(
            CommitBatch::new(Id::new(), now_ms())
                .append_event(iteration)
                .projection_put(
                    "loops",
                    1,
                    loop_id.to_string().into_bytes(),
                    b"iterating".to_vec(),
                )
                .enqueue_job(next_job),
        )
        .unwrap();

    drop(store);
    let store = StoreBuilder::new(&path)
        .durability(Durability::Strict)
        .open()
        .unwrap();

    let early = ClaimRequest {
        queue: "loop".into(),
        worker_id: "worker-loop".into(),
        now_ms: now_ms() + 1_000,
        lease_ms: 60_000,
        limit: 1,
    };
    assert!(store.claim_jobs(early).unwrap().is_empty());
    let late = ClaimRequest {
        queue: "loop".into(),
        worker_id: "worker-loop".into(),
        now_ms: now_ms() + 11_000,
        lease_ms: 60_000,
        limit: 1,
    };
    let claimed_loop = store.claim_jobs(late).unwrap();
    assert_eq!(claimed_loop.len(), 1);
    assert_eq!(claimed_loop[0].job_id, next_id);

    // Flow F.
    let events = store.stream_events(&stream, 0, 100);
    let mut rebuilt = Vec::new();
    for ev in &events {
        let status = if ev.event.event_type == "thread.turn-requested" {
            b"queued".to_vec()
        } else {
            b"idle".to_vec()
        };
        let key = ev
            .event
            .stream_id
            .strip_prefix("thread:")
            .unwrap_or("")
            .as_bytes();
        rebuilt.push(ProjectionEntry::new(key.to_vec(), status));
    }
    store
        .commit(CommitBatch::new(Id::new(), now_ms()).projection_replace("threads", 4, rebuilt))
        .unwrap();
    assert!(!store
        .scan_projection_prefix("threads", b"")
        .unwrap()
        .is_empty());

    let _ = std::fs::remove_file(&path);
}

#[test]
fn transaction_id_is_idempotent_across_reopen() {
    let path = tmp_path("idempotency.mini");
    let _ = std::fs::remove_file(&path);

    let tx = Id::new();
    let event = Event::new(
        Id::new(),
        "stream",
        "e",
        1,
        now_ms(),
        None,
        None,
        b"{}",
        b"",
    );
    let batch = CommitBatch::new(tx, now_ms()).append_event(event.clone());

    let store = StoreBuilder::new(&path)
        .durability(Durability::Memory)
        .open()
        .unwrap();
    let receipt1 = store.commit(batch.clone()).unwrap();
    let receipt2 = store.commit(batch.clone()).unwrap();
    assert_eq!(receipt1, receipt2);

    drop(store);
    let store = StoreBuilder::new(&path)
        .durability(Durability::Memory)
        .open()
        .unwrap();
    let receipt3 = store.commit(batch).unwrap();
    assert_eq!(receipt1, receipt3);
    assert_eq!(store.high_water_sequence(), 1);

    let _ = std::fs::remove_file(&path);
}

#[test]
fn transaction_id_conflicts_on_different_content() {
    let path = tmp_path("idempotency_conflict.mini");
    let _ = std::fs::remove_file(&path);

    let tx = Id::new();
    let event1 = Event::new(Id::new(), "s", "e", 1, now_ms(), None, None, b"1", b"");
    let event2 = Event::new(Id::new(), "s", "e", 1, now_ms(), None, None, b"2", b"");

    let store = StoreBuilder::new(&path)
        .durability(Durability::Memory)
        .open()
        .unwrap();
    store
        .commit(CommitBatch::new(tx, now_ms()).append_event(event1))
        .unwrap();
    let result = store.commit(CommitBatch::new(tx, now_ms()).append_event(event2));
    assert!(matches!(
        result,
        Err(minisqlite::Error::DuplicateIdWithDifferentContent { .. })
    ));

    let _ = std::fs::remove_file(&path);
}

#[test]
fn projection_prefix_with_all_ff_bytes() {
    let path = tmp_path("prefix_ff.mini");
    let _ = std::fs::remove_file(&path);

    let store = StoreBuilder::new(&path)
        .durability(Durability::Memory)
        .open()
        .unwrap();

    let key1 = vec![0xff, 0x01];
    let key2 = vec![0xff, 0xff];
    let key3 = vec![0xff, 0xff, 0x00];
    store
        .commit(
            CommitBatch::new(Id::new(), now_ms())
                .projection_put("p", 1, key1.clone(), b"1".to_vec())
                .projection_put("p", 2, key2.clone(), b"2".to_vec())
                .projection_put("p", 3, key3.clone(), b"3".to_vec()),
        )
        .unwrap();

    let prefix = vec![0xff];
    let found = store.scan_projection_prefix("p", &prefix).unwrap();
    assert_eq!(found.len(), 3);

    let _ = std::fs::remove_file(&path);
}

#[test]
fn claimed_job_includes_worker_id() {
    let path = tmp_path("worker_id.mini");
    let _ = std::fs::remove_file(&path);

    let store = StoreBuilder::new(&path)
        .durability(Durability::Memory)
        .open()
        .unwrap();

    let job = JobSpec::new(Id::new(), "q", "p", b"work".to_vec());
    let job_id = job.job_id;
    store
        .commit(CommitBatch::new(Id::new(), now_ms()).enqueue_job(job))
        .unwrap();

    let claimed = store
        .claim_jobs(ClaimRequest {
            queue: "q".into(),
            worker_id: "worker-42".into(),
            now_ms: now_ms(),
            lease_ms: 60_000,
            limit: 1,
        })
        .unwrap();
    assert_eq!(claimed.len(), 1);
    assert_eq!(claimed[0].job_id, job_id);
    assert_eq!(claimed[0].worker_id, "worker-42");

    let _ = std::fs::remove_file(&path);
}

#[test]
fn transaction_correlation_id_and_metadata_roundtrip() {
    let path = tmp_path("tx_meta.mini");
    let _ = std::fs::remove_file(&path);

    let store = StoreBuilder::new(&path)
        .durability(Durability::Memory)
        .open()
        .unwrap();

    let correlation = Id::new();
    let metadata = b"causal context".to_vec();
    let tx = Id::new();
    let event = Event::new(
        Id::new(),
        "tx-meta",
        "meta.test",
        1,
        now_ms(),
        None,
        None,
        b"{}",
        b"",
    );
    let receipt = store
        .commit(
            CommitBatch::new(tx, now_ms())
                .with_correlation_id(correlation)
                .with_metadata(metadata.clone())
                .append_event(event),
        )
        .unwrap();

    assert_eq!(receipt.correlation_id, Some(correlation));
    assert_eq!(receipt.metadata, metadata);

    let fetched = store.get_transaction(tx).unwrap();
    assert_eq!(fetched.correlation_id, Some(correlation));
    assert_eq!(fetched.metadata, metadata);

    drop(store);

    let store = StoreBuilder::new(&path)
        .durability(Durability::Memory)
        .open()
        .unwrap();
    let fetched = store.get_transaction(tx).unwrap();
    assert_eq!(fetched.correlation_id, Some(correlation));
    assert_eq!(fetched.metadata, metadata);

    let _ = std::fs::remove_file(&path);
}
