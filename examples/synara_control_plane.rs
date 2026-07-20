use std::time::{SystemTime, UNIX_EPOCH};

use minisqlite::{
    ClaimRequest, CommitBatch, Durability, EffectMode, Event, Id, JobSpec, JobState,
    ProjectionEntry, Resolution, StoreBuilder,
};

fn now_ms() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis() as i64)
        .unwrap_or(0)
}

fn main() {
    let path: std::path::PathBuf = std::env::args()
        .nth(1)
        .map(Into::into)
        .unwrap_or_else(|| std::env::temp_dir().join("synara_control_plane.mini"));
    let delete_after = std::env::args().len() < 2;
    if delete_after {
        let _ = std::fs::remove_file(&path);
    }
    let store = StoreBuilder::new(&path)
        .durability(Durability::Strict)
        .open()
        .unwrap();

    let thread_id = Id::new();
    let stream = format!("thread:{thread_id}");

    // Flow A: Create a thread.
    let created = Event::new(
        Id::new(),
        &stream,
        "thread.created",
        1,
        now_ms(),
        None,
        None,
        br#"{"title":"hello"}"#,
        b"",
    );
    let receipt = store
        .commit(
            CommitBatch::new(Id::new(), now_ms())
                .append_event(created.clone())
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
    println!(
        "Flow A: created thread {thread_id} at sequence {}",
        receipt.first_event_sequence.unwrap()
    );

    // Flow B: Request a provider turn.
    let requested = Event::new(
        Id::new(),
        &stream,
        "thread.turn-requested",
        1,
        now_ms(),
        Some(created.event_id),
        None,
        b"",
        b"",
    );
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
    println!("Flow B: requested provider turn and enqueued job {job_id}");

    // Flow C: Claim and complete provider work.
    let claim = ClaimRequest {
        queue: "provider".into(),
        worker_id: "worker-1".into(),
        now_ms: now_ms(),
        lease_ms: 60_000,
        limit: 1,
    };
    let claimed = store.claim_jobs(claim.clone()).unwrap();
    assert_eq!(claimed.len(), 1);
    let token = claimed[0].lease_token;
    println!("Flow C: claimed job {job_id} with token {token}");

    let completed = Event::new(
        Id::new(),
        &stream,
        "thread.turn-completed",
        1,
        now_ms(),
        None,
        None,
        b"",
        b"",
    );
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
    println!("Flow C: completed provider work");

    // Stale lease token cannot ack.
    let bad_ack = store.ack_job(job_id, token, None, now_ms());
    assert!(bad_ack.is_err());

    // Flow D part 1: Idempotent effect can be reclaimed after lease expiry.
    let idempotent_job = JobSpec::new(
        Id::new(),
        "provider",
        "partition-idempotent",
        b"idempotent-call".to_vec(),
    )
    .with_effect_mode(EffectMode::Idempotent)
    .with_idempotency_key("idempotent-key-1");
    let idempotent_id = idempotent_job.job_id;
    store
        .commit(CommitBatch::new(Id::new(), now_ms()).enqueue_job(idempotent_job))
        .unwrap();

    let mut idem_claim = ClaimRequest {
        queue: "provider".into(),
        worker_id: "worker-idem".into(),
        now_ms: now_ms(),
        lease_ms: 100,
        limit: 1,
    };
    let idem_first = store.claim_jobs(idem_claim.clone()).unwrap();
    assert_eq!(idem_first.len(), 1);
    assert_eq!(idem_first[0].job_id, idempotent_id);

    idem_claim.now_ms += 200;
    let idem_second = store.claim_jobs(idem_claim).unwrap();
    assert_eq!(idem_second.len(), 1);
    assert_eq!(idem_second[0].job_id, idempotent_id);
    assert_ne!(idem_second[0].lease_token, idem_first[0].lease_token);
    store
        .ack_job(
            idempotent_id,
            idem_second[0].lease_token,
            None,
            now_ms() + 200,
        )
        .unwrap();
    println!("Flow D: idempotent job reclaimed and acknowledged after expiry");

    // Flow D part 2: Non-idempotent effect becomes uncertain after expiry.
    let uncertain_job = JobSpec::new(Id::new(), "provider", "partition-2", b"call-api".to_vec())
        .with_effect_mode(EffectMode::UncertainOnLeaseExpiry)
        .with_max_attempts(1);
    let uncertain_id = uncertain_job.job_id;
    store
        .commit(CommitBatch::new(Id::new(), now_ms()).enqueue_job(uncertain_job))
        .unwrap();

    let mut claim2 = ClaimRequest {
        queue: "provider".into(),
        worker_id: "worker-2".into(),
        now_ms: now_ms(),
        lease_ms: 1000,
        limit: 1,
    };
    let claimed2 = store.claim_jobs(claim2.clone()).unwrap();
    assert_eq!(claimed2.len(), 1);
    assert_eq!(claimed2[0].job_id, uncertain_id);

    // Simulate time passing after lease expiry.
    claim2.now_ms += 2000;
    let reclaims = store.claim_jobs(claim2).unwrap();
    assert!(
        reclaims.is_empty(),
        "uncertain job must not be silently retried"
    );
    assert!(matches!(
        store.job_state(uncertain_id, now_ms() + 2000).unwrap(),
        JobState::Uncertain
    ));

    // Explicitly resolve as succeeded.
    store
        .resolve_uncertain_job(uncertain_id, Resolution::MarkSucceeded, now_ms() + 2000)
        .unwrap();
    assert!(matches!(
        store.job_state(uncertain_id, now_ms() + 2000).unwrap(),
        JobState::Succeeded
    ));
    println!("Flow D: uncertain job resolved");

    // Flow E: Durable loop scheduling with a future job.
    let loop_id = Id::new();
    let loop_stream = format!("loop:{loop_id}");
    let iteration = Event::new(
        Id::new(),
        &loop_stream,
        "loop.iteration",
        1,
        now_ms(),
        None,
        None,
        b"1",
        b"",
    );
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

    // Restart the process by reopening the store.
    drop(store);
    let store = StoreBuilder::new(&path)
        .durability(Durability::Strict)
        .open()
        .unwrap();

    // The future job is not yet claimable.
    let early_claim = ClaimRequest {
        queue: "loop".into(),
        worker_id: "worker-loop".into(),
        now_ms: now_ms() + 1_000,
        lease_ms: 60_000,
        limit: 1,
    };
    assert!(store.claim_jobs(early_claim).unwrap().is_empty());

    // After not_before, it is claimable.
    let late_claim = ClaimRequest {
        queue: "loop".into(),
        worker_id: "worker-loop".into(),
        now_ms: now_ms() + 11_000,
        lease_ms: 60_000,
        limit: 1,
    };
    let claimed_loop = store.claim_jobs(late_claim).unwrap();
    assert_eq!(claimed_loop.len(), 1);
    assert_eq!(claimed_loop[0].job_id, next_id);
    println!("Flow E: durable loop scheduling recovered after restart");

    // Flow F: Rebuild a projection from event history and atomically replace it.
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
    let threads = store.scan_projection_prefix("threads", b"").unwrap();
    assert!(!threads.is_empty());
    println!(
        "Flow F: rebuilt threads projection from {} events ({} entries)",
        events.len(),
        threads.len()
    );

    println!("All Synara-shaped flows completed.");
    drop(store);
    if delete_after {
        let _ = std::fs::remove_file(&path);
    }
}
