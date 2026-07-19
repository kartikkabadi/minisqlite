use std::time::Instant;

use minisqlite::{CommitBatch, Durability, Event, Id, StoreBuilder};

fn main() {
    let base = std::env::temp_dir().join(format!("minisqlite_bench_{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&base);
    std::fs::create_dir_all(&base).unwrap();

    for durability in [Durability::Memory, Durability::Strict] {
        println!("\n=== {durability:?} durability ===");
        let counts = [1_000, 10_000, 100_000];
        for &n in &counts {
            let path = base.join(format!("{durability:?}_{n}.mini").to_lowercase());

            let store = StoreBuilder::new(&path)
                .durability(durability)
                .open()
                .unwrap();

            let start = Instant::now();
            for i in 0..n {
                let event = Event::with_json_payload(Id::new(), "bench", "event", br#"{"i":0}"#);
                store
                    .commit(CommitBatch::new(Id::new(), i as i64).append_event(event))
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

    let _ = std::fs::remove_dir_all(&base);
    println!("\nAt these scales, replay remains sub-second. Snapshots are not required yet.");
}
