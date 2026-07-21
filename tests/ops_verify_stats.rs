use minisqlite::{CommitBatch, ControlPlaneStore, Event, Id};

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
            "s1",
            "updated",
            1_001,
            b"{}",
        ))
        .append_event(Event::with_json_payload(
            Id::from(12u128),
            "s2",
            "created",
            1_002,
            b"{}",
        ));
    store.commit(&batch).unwrap();
    store
}

#[test]
fn verify_reports_ok_on_healthy_store() {
    let dir = tempfile::tempdir().unwrap();
    let store = seeded_store(&dir.path().join("db"));
    let report = store.verify().unwrap();
    assert!(report.is_ok(), "unexpected findings: {:?}", report.findings);
}

#[test]
fn verify_reports_ok_on_empty_store() {
    let dir = tempfile::tempdir().unwrap();
    let store = ControlPlaneStore::open(dir.path().join("db")).unwrap();
    assert!(store.verify().unwrap().is_ok());
}

#[test]
fn stats_counts_are_correct() {
    let dir = tempfile::tempdir().unwrap();
    let store = seeded_store(&dir.path().join("db"));
    let stats = store.stats().unwrap();
    assert_eq!(stats.transactions, 1);
    assert_eq!(stats.events, 3);
    assert_eq!(stats.streams, 2);
    assert_eq!(stats.projections, 0);
    assert_eq!(stats.projection_entries, 0);
    assert!(stats.jobs_by_state.is_empty());
    assert_eq!(stats.active_partitions, 0);
    assert_eq!(stats.migration_version, 1);
    assert!(stats.file_size_bytes > 0);
    assert_eq!(stats.oldest_active_lease_ms, None);
    assert_eq!(stats.oldest_uncertain_job_ms, None);
}

#[test]
fn diagnostic_export_redacts_payloads_by_default() {
    let dir = tempfile::tempdir().unwrap();
    let store = seeded_store(&dir.path().join("db"));
    let export = store.diagnostic_export().unwrap();
    let first = export.lines().next().unwrap();
    assert!(first.contains("\"kind\":\"header\""));
    assert!(first.contains("\"restorable\":false"));
    assert!(first.contains("\"schema_version\":1"));
    assert!(first.contains("\"payloads_included\":false"));
    assert!(export.contains("\"payload_len\":7"));
    assert!(!export.contains("payload_hex"));
    assert!(!export.contains("lease_token"));
    // One line per record: header, stats, 1 transaction, 3 events, 2 streams.
    assert_eq!(export.lines().count(), 8);
}

#[test]
fn diagnostic_export_can_include_payloads_but_never_lease_tokens() {
    let dir = tempfile::tempdir().unwrap();
    let store = seeded_store(&dir.path().join("db"));
    let export = store.diagnostic_export_with(true).unwrap();
    assert!(export.contains("\"payloads_included\":true"));
    // b"{\"a\":1}" as hex.
    assert!(export.contains("\"payload_hex\":\"7b2261223a317d\""));
    assert!(!export.contains("lease_token"));
}
