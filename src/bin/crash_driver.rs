use std::env;
use std::time::{SystemTime, UNIX_EPOCH};

use minisqlite::{CommitBatch, Durability, EffectMode, Event, Id, JobSpec, StoreBuilder};

fn now_ms() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis() as i64)
        .unwrap_or(0)
}

fn main() {
    let args: Vec<String> = env::args().collect();
    if args.len() != 3 {
        eprintln!("usage: crash_driver <path> <failpoint>");
        std::process::exit(2);
    }
    let path = &args[1];
    let failpoint = &args[2];

    // Initial successful commit.
    let store = StoreBuilder::new(path)
        .durability(Durability::Strict)
        .open()
        .unwrap();
    let event = Event::with_json_payload(Id::new(), "stream", "init", b"{}");
    store
        .commit(
            CommitBatch::new(Id::new(), now_ms())
                .append_event(event)
                .projection_put("state", 1, b"key".to_vec(), b"first".to_vec())
                .enqueue_job(JobSpec::new(Id::new(), "q", "p", b"work".to_vec())),
        )
        .unwrap();

    // Set the failpoint for the next commit.
    env::set_var("MINISQLITE_FAILPOINT", failpoint);

    let second = CommitBatch::new(Id::new(), now_ms())
        .append_event(Event::with_json_payload(
            Id::new(),
            "stream",
            "second",
            b"{}",
        ))
        .projection_put("state", 2, b"key".to_vec(), b"second".to_vec())
        .enqueue_job(
            JobSpec::new(Id::new(), "q", "p", b"more".to_vec())
                .with_effect_mode(EffectMode::UncertainOnLeaseExpiry),
        );

    // This call may abort the process depending on the failpoint.
    let _ = store.commit(second);
}
