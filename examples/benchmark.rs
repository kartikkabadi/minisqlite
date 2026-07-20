use std::time::Instant;

use minisqlite::{
    ClaimRequest, CommitBatch, Durability, Event, Id, JobSpec, ProjectionEntry, StoreBuilder,
};

fn main() {
    let base = std::env::temp_dir().join(format!("minisqlite_bench_{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&base);
    std::fs::create_dir_all(&base).unwrap();

    for durability in [Durability::Memory, Durability::Strict] {
        println!("\n=== {durability:?} durability ===");
        let counts = [1_000, 10_000, 100_000, 1_000_000];
        for &n in &counts {
            let path = base.join(format!("{durability:?}_{n}.mini").to_lowercase());

            let store = StoreBuilder::new(&path)
                .durability(durability)
                .open()
                .unwrap();

            let start = Instant::now();
            for i in 0..n {
                let event = Event::with_json_payload(
                    Id::new().unwrap(),
                    "bench",
                    "event",
                    i as i64,
                    br#"{"i":0}"#,
                );
                store
                    .commit(CommitBatch::new(Id::new().unwrap(), i as i64).append_event(event))
                    .unwrap();
            }
            let elapsed = start.elapsed();
            let per_ms = n as f64 / elapsed.as_millis().max(1) as f64;
            println!(
                "committed {n} events in {:?} ({:.2} events/ms)",
                elapsed, per_ms
            );

            drop(store);
            let reopen_start = Instant::now();
            let store = StoreBuilder::new(&path)
                .durability(durability)
                .open()
                .unwrap();
            let reopen_elapsed = reopen_start.elapsed();
            println!("reopen after {n} events: {:?}", reopen_elapsed);

            let meta = std::fs::metadata(&path).unwrap();
            println!("file size after {n} events: {} bytes", meta.len());

            drop(store);
        }
    }

    println!("\n=== projection operations (Strict) ===");
    let proj_path = base.join("projection.mini");
    let store = StoreBuilder::new(&proj_path)
        .durability(Durability::Strict)
        .open()
        .unwrap();

    let mut entries = Vec::new();
    for i in 0..10_000u32 {
        let key = format!("key-{i:05}").into_bytes();
        let value = format!("value-{i:05}").into_bytes();
        entries.push(ProjectionEntry::new(key, value));
    }
    let start = Instant::now();
    store
        .commit(CommitBatch::new(Id::new().unwrap(), 0).projection_replace("kv", 1, entries))
        .unwrap();
    println!(
        "replaced 10,000 projection entries in {:?}",
        start.elapsed()
    );

    let start = Instant::now();
    let prefix = store.scan_projection_prefix("kv", b"key-050").unwrap();
    println!(
        "prefix scan returned {} entries in {:?}",
        prefix.len(),
        start.elapsed()
    );

    drop(store);

    println!("\n=== durable jobs (Strict) ===");
    let job_path = base.join("jobs.mini");
    let store = StoreBuilder::new(&job_path)
        .durability(Durability::Strict)
        .open()
        .unwrap();

    let start = Instant::now();
    for i in 0..10_000u32 {
        let job = JobSpec::new(
            Id::new().unwrap(),
            "provider",
            "partition",
            format!("work-{i}").into_bytes(),
        );
        store
            .commit(CommitBatch::new(Id::new().unwrap(), i as i64).enqueue_job(job))
            .unwrap();
    }
    println!("enqueued 10,000 jobs in {:?}", start.elapsed());

    let now = 60_000i64;
    let start = Instant::now();
    let mut claimed = 0usize;
    loop {
        let batch = store
            .claim_jobs(ClaimRequest {
                queue: "provider".into(),
                worker_id: "worker-1".into(),
                now_ms: now,
                lease_ms: 30_000,
                limit: 100,
            })
            .unwrap();
        if batch.is_empty() {
            break;
        }
        for c in batch {
            store.ack_job(c.job_id, c.lease_token, None, now).unwrap();
            claimed += 1;
        }
    }
    println!(
        "claimed and acknowledged {} jobs in {:?}",
        claimed,
        start.elapsed()
    );

    drop(store);

    let _ = std::fs::remove_dir_all(&base);
    println!("\nAt these scales, replay remains acceptable. Snapshots are not required yet.");
}
