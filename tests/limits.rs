use std::path::PathBuf;

use minisqlite::{CommitBatch, Durability, Event, Id, Limits, StoreBuilder};

fn invalid_path() -> PathBuf {
    std::env::temp_dir().join("minisqlite_limits_should_not_be_created.mini")
}

#[test]
fn max_frame_size_cannot_exceed_hard_limit() {
    let mut limits = Limits::new();
    limits.max_frame_size = 128 << 20; // larger than hard 64 MiB
    match StoreBuilder::new(invalid_path())
        .durability(Durability::Memory)
        .limits(limits)
        .open()
    {
        Ok(_) => panic!("expected validation error"),
        Err(e) => assert!(e.to_string().contains("hard limit")),
    }
}

#[test]
fn max_frame_size_must_cover_overhead() {
    let mut limits = Limits::new();
    limits.max_frame_size = 80; // below header + trailer
    match StoreBuilder::new(invalid_path())
        .durability(Durability::Memory)
        .limits(limits)
        .open()
    {
        Ok(_) => panic!("expected validation error"),
        Err(e) => assert!(e.to_string().contains("overhead")),
    }
}

#[test]
fn event_payload_above_limit_is_rejected() {
    let tmp = tempfile::tempdir().unwrap();
    let mut limits = Limits::new();
    limits.max_event_payload = 4;
    let store = StoreBuilder::new(tmp.path().join("limits.mini"))
        .durability(Durability::Memory)
        .limits(limits)
        .open()
        .unwrap();
    let event = Event::with_json_payload(Id::new(), "s", "e", 0, b"hello");
    let err = store
        .commit(CommitBatch::new(Id::new(), 0).append_event(event))
        .unwrap_err();
    assert!(err.to_string().contains("event payload"));
}

#[test]
fn projection_value_above_limit_is_rejected() {
    let tmp = tempfile::tempdir().unwrap();
    let mut limits = Limits::new();
    limits.max_projection_value = 2;
    let store = StoreBuilder::new(tmp.path().join("limits.mini"))
        .durability(Durability::Memory)
        .limits(limits)
        .open()
        .unwrap();
    let err = store
        .commit(CommitBatch::new(Id::new(), 0).projection_put(
            "p",
            1,
            b"k".to_vec(),
            b"value".to_vec(),
        ))
        .unwrap_err();
    assert!(err.to_string().contains("projection value"));
}

#[test]
fn job_payload_above_limit_is_rejected() {
    let tmp = tempfile::tempdir().unwrap();
    let mut limits = Limits::new();
    limits.max_job_payload = 2;
    let store = StoreBuilder::new(tmp.path().join("limits.mini"))
        .durability(Durability::Memory)
        .limits(limits)
        .open()
        .unwrap();
    let job = minisqlite::JobSpec::new(Id::new(), "q", "p", b"payload".to_vec());
    let err = store
        .commit(CommitBatch::new(Id::new(), 0).enqueue_job(job))
        .unwrap_err();
    assert!(err.to_string().contains("job payload"));
}

#[test]
fn too_many_records_per_transaction_is_rejected() {
    let tmp = tempfile::tempdir().unwrap();
    let mut limits = Limits::new();
    limits.max_records_per_transaction = 2;
    let store = StoreBuilder::new(tmp.path().join("limits.mini"))
        .durability(Durability::Memory)
        .limits(limits)
        .open()
        .unwrap();
    let batch = CommitBatch::new(Id::new(), 0)
        .append_event(Event::with_json_payload(Id::new(), "s", "e", 0, b""))
        .append_event(Event::with_json_payload(Id::new(), "s", "e", 0, b""))
        .append_event(Event::with_json_payload(Id::new(), "s", "e", 0, b""));
    let err = store.commit(batch).unwrap_err();
    assert!(err.to_string().contains("records"));
}
