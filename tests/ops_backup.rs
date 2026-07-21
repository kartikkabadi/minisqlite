use minisqlite::{CommitBatch, ControlPlaneStore, Error, Event, Id};

fn seeded_store(path: &std::path::Path) -> ControlPlaneStore {
    let store = ControlPlaneStore::open(path).unwrap();
    let batch = CommitBatch::new(Id::from(1u128), 2_000)
        .append_event(Event::with_json_payload(
            Id::from(10u128),
            "s1",
            "created",
            1_000,
            b"{\"a\":1}",
        ))
        .append_event(Event::with_json_payload(
            Id::from(11u128),
            "s2",
            "created",
            1_001,
            b"{}",
        ));
    store.commit(&batch).unwrap();
    store
}

#[test]
fn backup_to_fresh_destination_succeeds() {
    let dir = tempfile::tempdir().unwrap();
    let store = seeded_store(&dir.path().join("db"));
    let dest = dir.path().join("backup.db");
    store.backup(&dest, false).unwrap();
    assert!(dest.exists());
}

#[test]
fn backup_refuses_existing_destination_without_overwrite() {
    let dir = tempfile::tempdir().unwrap();
    let store = seeded_store(&dir.path().join("db"));
    let dest = dir.path().join("backup.db");
    std::fs::write(&dest, b"existing").unwrap();
    assert!(matches!(
        store.backup(&dest, false).unwrap_err(),
        Error::Validation(_)
    ));
    // Destination untouched.
    assert_eq!(std::fs::read(&dest).unwrap(), b"existing");
}

#[test]
fn backup_overwrites_existing_destination_when_asked() {
    let dir = tempfile::tempdir().unwrap();
    let store = seeded_store(&dir.path().join("db"));
    let dest = dir.path().join("backup.db");
    store.backup(&dest, false).unwrap();
    store.backup(&dest, true).unwrap();
    let restored = ControlPlaneStore::open(&dest).unwrap();
    assert_eq!(restored.events_after(0, 100).unwrap().len(), 2);
}

#[test]
fn backup_restore_is_equivalent() {
    let dir = tempfile::tempdir().unwrap();
    let store = seeded_store(&dir.path().join("db"));
    let dest = dir.path().join("backup.db");
    store.backup(&dest, false).unwrap();

    let restored = ControlPlaneStore::open(&dest).unwrap();
    assert_eq!(
        restored.events_after(0, 100).unwrap(),
        store.events_after(0, 100).unwrap()
    );
    assert_eq!(restored.stream_version("s1").unwrap(), 1);
    assert_eq!(restored.stream_version("s2").unwrap(), 1);
    assert!(restored.verify().unwrap().is_ok());
    // The restored store accepts further commits.
    let more = CommitBatch::new(Id::from(2u128), 3_000).append_event(Event::with_json_payload(
        Id::from(12u128),
        "s1",
        "updated",
        2_500,
        b"{}",
    ));
    restored.commit(&more).unwrap();
    assert_eq!(restored.stream_version("s1").unwrap(), 2);
}
