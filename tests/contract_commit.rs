//! Contract tests for the commit pipeline: transaction idempotency, stream
//! versioning, event reads, projection patches, durability modes, and reopen.

mod common;

use common::{db_path, event, id, open, open_in, single_event_batch, temp_dir};
use minisqlite::{
    CommitBatch, CommitError, Conflict, ControlPlaneStore, Durability, Error, ProjectionPatch,
    TransactionRecovery,
};

// ----- transaction idempotency -----

#[test]
fn duplicate_transaction_id_with_identical_content_returns_original_receipt() {
    let dir = temp_dir();
    let store = open_in(&dir);
    let batch = single_event_batch(1, 10, "s1");
    let first = store.commit(&batch).unwrap();
    let second = store.commit(&batch).unwrap();
    assert_eq!(first, second);
    assert_eq!(store.events_after(0, 100).unwrap().len(), 1);
}

#[test]
fn duplicate_transaction_id_survives_reopen() {
    let dir = temp_dir();
    let path = db_path(&dir);
    let batch = single_event_batch(1, 10, "s1");
    let first = {
        let store = open(&path);
        store.commit(&batch).unwrap()
    };
    let store = open(&path);
    assert_eq!(store.commit(&batch).unwrap(), first);
    assert_eq!(store.events_after(0, 100).unwrap().len(), 1);
}

#[test]
fn duplicate_transaction_id_with_different_content_is_typed_error() {
    let dir = temp_dir();
    let store = open_in(&dir);
    store.commit(&single_event_batch(1, 10, "s1")).unwrap();
    let different = CommitBatch::new(id(1), 2_000).append_event(event(11, "s1", "other"));
    assert_eq!(
        store.commit(&different).unwrap_err(),
        CommitError::DuplicateIdWithDifferentContent
    );
    // The original commit is untouched.
    assert_eq!(store.events_after(0, 100).unwrap().len(), 1);
    assert_eq!(store.stream_version("s1").unwrap(), 1);
}

#[test]
fn recover_transaction_reports_committed_and_absent() {
    let dir = temp_dir();
    let store = open_in(&dir);
    let receipt = store.commit(&single_event_batch(1, 10, "s1")).unwrap();
    assert_eq!(
        store.recover_transaction(id(1)).unwrap(),
        TransactionRecovery::Committed(receipt)
    );
    assert_eq!(
        store.recover_transaction(id(99)).unwrap(),
        TransactionRecovery::Absent
    );
}

// ----- expected stream versions -----

#[test]
fn expected_stream_version_success() {
    let dir = temp_dir();
    let store = open_in(&dir);
    store.commit(&single_event_batch(1, 10, "s1")).unwrap();
    let batch = CommitBatch::new(id(2), 2_001)
        .expect_stream_version("s1", 1)
        .append_event(event(11, "s1", "next"));
    let receipt = store.commit(&batch).unwrap();
    assert_eq!(receipt.transaction_sequence(), 2);
    assert_eq!(store.stream_version("s1").unwrap(), 2);
}

#[test]
fn expected_stream_version_conflict_is_typed_and_persists_nothing() {
    let dir = temp_dir();
    let store = open_in(&dir);
    store.commit(&single_event_batch(1, 10, "s1")).unwrap();
    let conflicting = CommitBatch::new(id(2), 2_001)
        .expect_stream_version("s1", 0)
        .append_event(event(11, "s1", "next"));
    assert_eq!(
        store.commit(&conflicting).unwrap_err(),
        CommitError::Conflict(Conflict::StreamVersion {
            stream_id: "s1".into(),
            expected: 0,
            actual: 1,
        })
    );
    assert_eq!(store.events_after(0, 100).unwrap().len(), 1);
    assert_eq!(store.stream_version("s1").unwrap(), 1);
    assert_eq!(
        store.recover_transaction(id(2)).unwrap(),
        TransactionRecovery::Absent
    );
}

#[test]
fn expected_version_zero_requires_absent_stream() {
    let dir = temp_dir();
    let store = open_in(&dir);
    let batch = CommitBatch::new(id(1), 2_000)
        .expect_stream_version("fresh", 0)
        .append_event(event(10, "fresh", "created"));
    store.commit(&batch).unwrap();
    assert_eq!(store.stream_version("fresh").unwrap(), 1);
}

// ----- event ID uniqueness -----

#[test]
fn event_id_reuse_across_transactions_is_rejected() {
    let dir = temp_dir();
    let store = open_in(&dir);
    store.commit(&single_event_batch(1, 10, "s1")).unwrap();
    let reuse = CommitBatch::new(id(2), 2_001).append_event(event(10, "s2", "dup"));
    assert!(store.commit(&reuse).is_err());
    // Nothing from the failed commit persisted.
    assert_eq!(store.events_after(0, 100).unwrap().len(), 1);
    assert_eq!(store.stream_version("s2").unwrap(), 0);
    assert_eq!(
        store.recover_transaction(id(2)).unwrap(),
        TransactionRecovery::Absent
    );
}

#[test]
fn event_id_reuse_within_one_batch_is_rejected_atomically() {
    let dir = temp_dir();
    let store = open_in(&dir);
    let batch = CommitBatch::new(id(1), 2_000)
        .append_event(event(10, "s1", "a"))
        .append_event(event(10, "s1", "b"));
    assert!(store.commit(&batch).is_err());
    assert_eq!(store.events_after(0, 100).unwrap().len(), 0);
    assert_eq!(store.stream_version("s1").unwrap(), 0);
}

// ----- ordered reads and pagination after reopen -----

#[test]
fn ordered_event_reads_and_pagination_after_reopen() {
    let dir = temp_dir();
    let path = db_path(&dir);
    {
        let store = open(&path);
        for i in 0..10u128 {
            let stream = if i % 2 == 0 { "even" } else { "odd" };
            let batch = CommitBatch::new(id(100 + i), 2_000 + i as i64).append_event(event(
                200 + i,
                stream,
                "tick",
            ));
            store.commit(&batch).unwrap();
        }
    }

    let store = open(&path);

    // Global reads paginate in global-sequence order across reopen.
    let mut seen = Vec::new();
    let mut after = 0u64;
    loop {
        let page = store.events_after(after, 3).unwrap();
        if page.is_empty() {
            break;
        }
        after = page.last().unwrap().global_sequence;
        seen.extend(page);
    }
    assert_eq!(seen.len(), 10);
    for (i, persisted) in seen.iter().enumerate() {
        assert_eq!(persisted.global_sequence, i as u64 + 1);
        assert_eq!(persisted.event.event_id, id(200 + i as u128));
    }

    // Per-stream reads paginate in stream-version order.
    let mut versions = Vec::new();
    let mut from = 1u64;
    loop {
        let page = store.stream_events("even", from, 2).unwrap();
        if page.is_empty() {
            break;
        }
        from = page.last().unwrap().stream_version + 1;
        versions.extend(page.iter().map(|p| p.stream_version));
    }
    assert_eq!(versions, vec![1, 2, 3, 4, 5]);
    assert_eq!(store.stream_version("even").unwrap(), 5);
    assert_eq!(store.stream_version("odd").unwrap(), 5);
}

// ----- projections -----

#[test]
fn projection_patch_advances_exactly_one_version() {
    let dir = temp_dir();
    let store = open_in(&dir);
    assert_eq!(store.projection_version("p").unwrap(), 0);

    let batch = CommitBatch::new(id(1), 2_000)
        .apply_projection_patch(ProjectionPatch::new("p", 0).put("k", "v1"));
    store.commit(&batch).unwrap();
    assert_eq!(store.projection_version("p").unwrap(), 1);
    assert_eq!(
        store.projection_get("p", b"k").unwrap(),
        Some(b"v1".to_vec())
    );

    let batch = CommitBatch::new(id(2), 2_001)
        .apply_projection_patch(ProjectionPatch::new("p", 1).put("k", "v2"));
    store.commit(&batch).unwrap();
    assert_eq!(store.projection_version("p").unwrap(), 2);
    assert_eq!(
        store.projection_get("p", b"k").unwrap(),
        Some(b"v2".to_vec())
    );
}

#[test]
fn projection_multi_mutation_patch_is_one_version() {
    let dir = temp_dir();
    let store = open_in(&dir);
    let patch = ProjectionPatch::new("p", 0)
        .put("a", "1")
        .put("b", "2")
        .delete("missing");
    store
        .commit(&CommitBatch::new(id(1), 2_000).apply_projection_patch(patch))
        .unwrap();
    assert_eq!(store.projection_version("p").unwrap(), 1);
    assert_eq!(
        store.projection_get("p", b"a").unwrap(),
        Some(b"1".to_vec())
    );
    assert_eq!(
        store.projection_get("p", b"b").unwrap(),
        Some(b"2".to_vec())
    );
    assert_eq!(store.projection_get("p", b"missing").unwrap(), None);

    let entries = store.projection_scan_prefix("p", b"", 100).unwrap();
    assert_eq!(entries.len(), 2);
    assert_eq!(entries[0].key, b"a");
    assert_eq!(entries[1].key, b"b");

    assert_eq!(
        store.projections_list().unwrap(),
        vec![("p".to_string(), 1)]
    );
}

#[test]
fn projection_version_conflict_is_typed_and_persists_nothing() {
    let dir = temp_dir();
    let store = open_in(&dir);
    store
        .commit(
            &CommitBatch::new(id(1), 2_000)
                .apply_projection_patch(ProjectionPatch::new("p", 0).put("k", "v1")),
        )
        .unwrap();

    let stale = CommitBatch::new(id(2), 2_001)
        .apply_projection_patch(ProjectionPatch::new("p", 0).put("k", "v2"));
    assert_eq!(
        store.commit(&stale).unwrap_err(),
        CommitError::Conflict(Conflict::ProjectionVersion {
            projection: "p".into(),
            expected: 0,
            actual: 1,
        })
    );
    assert_eq!(store.projection_version("p").unwrap(), 1);
    assert_eq!(
        store.projection_get("p", b"k").unwrap(),
        Some(b"v1".to_vec())
    );
}

#[test]
fn projection_patch_static_validation_rejects_bad_versions_and_duplicates() {
    let dir = temp_dir();
    let store = open_in(&dir);

    let mut skipping = ProjectionPatch::new("p", 0).put("k", "v");
    skipping.new_version = 3;
    assert!(matches!(
        store
            .commit(&CommitBatch::new(id(1), 2_000).apply_projection_patch(skipping))
            .unwrap_err(),
        CommitError::Validation(_)
    ));

    let contradictory = ProjectionPatch::new("p", 0).put("k", "v1").delete("k");
    assert!(matches!(
        store
            .commit(&CommitBatch::new(id(2), 2_000).apply_projection_patch(contradictory))
            .unwrap_err(),
        CommitError::Validation(_)
    ));
}

// ----- durability and reopen -----

#[test]
fn durability_modes_open_correctly() {
    for durability in [Durability::Strict, Durability::Relaxed] {
        let dir = temp_dir();
        let store = ControlPlaneStore::builder(db_path(&dir))
            .durability(durability)
            .open()
            .unwrap();
        assert_eq!(store.durability(), durability);
        store.commit(&single_event_batch(1, 10, "s1")).unwrap();
        assert_eq!(store.stream_version("s1").unwrap(), 1);
    }
}

#[test]
fn default_durability_is_strict() {
    let dir = temp_dir();
    let store = open_in(&dir);
    assert_eq!(store.durability(), Durability::Strict);
}

#[test]
fn reopen_requires_no_replay() {
    let dir = temp_dir();
    let path = db_path(&dir);
    {
        let store = open(&path);
        store.commit(&single_event_batch(1, 10, "s1")).unwrap();
        store.commit(&single_event_batch(2, 11, "s1")).unwrap();
    }
    // A fresh handle answers reads immediately, with no replay step.
    let store = open(&path);
    assert_eq!(store.stream_version("s1").unwrap(), 2);
    let events = store.events_after(0, 100).unwrap();
    assert_eq!(events.len(), 2);
    assert_eq!(events[0].event.event_id, id(10));
    assert_eq!(events[1].event.event_id, id(11));
    assert_eq!(store.get_event(id(11)).unwrap().unwrap().stream_version, 2);
}

#[test]
fn reopen_with_different_durability_preserves_data() {
    let dir = temp_dir();
    let path = db_path(&dir);
    {
        let store = ControlPlaneStore::builder(&path)
            .durability(Durability::Strict)
            .open()
            .unwrap();
        store.commit(&single_event_batch(1, 10, "s1")).unwrap();
    }
    let store = ControlPlaneStore::builder(&path)
        .durability(Durability::Relaxed)
        .open()
        .unwrap();
    assert_eq!(store.stream_version("s1").unwrap(), 1);
}

// ----- misc read guarantees -----

#[test]
fn get_event_and_missing_lookups() {
    let dir = temp_dir();
    let store = open_in(&dir);
    store.commit(&single_event_batch(1, 10, "s1")).unwrap();
    let found = store.get_event(id(10)).unwrap().unwrap();
    assert_eq!(found.event.stream_id, "s1");
    assert_eq!(found.transaction_id, id(1));
    assert!(store.get_event(id(404)).unwrap().is_none());
    assert_eq!(store.stream_version("missing").unwrap(), 0);
    assert!(store.stream_events("missing", 1, 10).unwrap().is_empty());
}

#[test]
fn verify_reports_consistent_store() {
    let dir = temp_dir();
    let store = open_in(&dir);
    store.commit(&single_event_batch(1, 10, "s1")).unwrap();
    let report = store.verify().unwrap();
    assert!(report.is_ok(), "unexpected findings: {:?}", report.findings);
}

#[test]
fn commit_error_variants_are_matchable() {
    // Public error contract: callers can match commit failures without depending
    // on any storage internals.
    fn classify(err: &CommitError) -> &'static str {
        match err {
            CommitError::Conflict(_) => "conflict",
            CommitError::Validation(_) => "validation",
            CommitError::DuplicateIdWithDifferentContent => "duplicate",
            CommitError::Indeterminate(_) => "indeterminate",
            CommitError::Storage(_) => "storage",
            _ => "other",
        }
    }
    assert_eq!(
        classify(&CommitError::DuplicateIdWithDifferentContent),
        "duplicate"
    );
    let _ = Error::from(minisqlite::ValidationError("x".into()));
}
