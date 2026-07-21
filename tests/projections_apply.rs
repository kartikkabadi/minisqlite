//! Integration tests for projection patch application semantics.

use minisqlite::{
    CommitBatch, CommitError, Conflict, ControlPlaneStore, Id, ProjectionEntry, ProjectionPatch,
};

fn open_store(dir: &tempfile::TempDir) -> ControlPlaneStore {
    ControlPlaneStore::open(dir.path().join("db")).unwrap()
}

fn commit_patch(store: &ControlPlaneStore, txn: u128, patch: ProjectionPatch) {
    store
        .commit(&CommitBatch::new(Id::from(txn), 1_000).apply_projection_patch(patch))
        .unwrap();
}

#[test]
fn patch_creates_projection_and_increments_version() {
    let dir = tempfile::tempdir().unwrap();
    let store = open_store(&dir);
    assert_eq!(store.projection_version("p").unwrap(), 0);

    commit_patch(&store, 1, ProjectionPatch::new("p", 0).put("a", "1"));
    assert_eq!(store.projection_version("p").unwrap(), 1);
    assert_eq!(
        store.projection_get("p", b"a").unwrap(),
        Some(b"1".to_vec())
    );

    commit_patch(&store, 2, ProjectionPatch::new("p", 1).put("b", "2"));
    assert_eq!(store.projection_version("p").unwrap(), 2);
    assert_eq!(store.projection_entry_count("p").unwrap(), 2);
}

#[test]
fn multi_mutation_patch_applies_in_order() {
    let dir = tempfile::tempdir().unwrap();
    let store = open_store(&dir);
    commit_patch(
        &store,
        1,
        ProjectionPatch::new("p", 0).put("a", "1").put("b", "2"),
    );
    // Mutations apply in order: the Put after Clear survives, the one before does not.
    commit_patch(
        &store,
        2,
        ProjectionPatch::new("p", 1)
            .put("c", "3")
            .clear()
            .put("d", "4"),
    );
    assert_eq!(store.projection_get("p", b"a").unwrap(), None);
    assert_eq!(store.projection_get("p", b"c").unwrap(), None);
    assert_eq!(
        store.projection_get("p", b"d").unwrap(),
        Some(b"4".to_vec())
    );
    assert_eq!(store.projection_entry_count("p").unwrap(), 1);
}

#[test]
fn delete_absent_key_is_noop() {
    let dir = tempfile::tempdir().unwrap();
    let store = open_store(&dir);
    commit_patch(&store, 1, ProjectionPatch::new("p", 0).delete("missing"));
    assert_eq!(store.projection_version("p").unwrap(), 1);
    assert_eq!(store.projection_entry_count("p").unwrap(), 0);
}

#[test]
fn clear_removes_all_entries_but_keeps_version() {
    let dir = tempfile::tempdir().unwrap();
    let store = open_store(&dir);
    commit_patch(
        &store,
        1,
        ProjectionPatch::new("p", 0).put("a", "1").put("b", "2"),
    );
    commit_patch(
        &store,
        2,
        ProjectionPatch::new("p", 1).clear().put("c", "3"),
    );
    assert_eq!(store.projection_version("p").unwrap(), 2);
    assert_eq!(store.projection_get("p", b"a").unwrap(), None);
    assert_eq!(
        store.projection_get("p", b"c").unwrap(),
        Some(b"3".to_vec())
    );
    assert_eq!(store.projection_entry_count("p").unwrap(), 1);
}

#[test]
fn replace_swaps_entire_contents() {
    let dir = tempfile::tempdir().unwrap();
    let store = open_store(&dir);
    commit_patch(
        &store,
        1,
        ProjectionPatch::new("p", 0).put("a", "1").put("b", "2"),
    );
    commit_patch(
        &store,
        2,
        ProjectionPatch::new("p", 1).replace(vec![
            ProjectionEntry::new("x", "9"),
            ProjectionEntry::new("y", "8"),
        ]),
    );
    assert_eq!(store.projection_get("p", b"a").unwrap(), None);
    assert_eq!(
        store.projection_get("p", b"x").unwrap(),
        Some(b"9".to_vec())
    );
    assert_eq!(store.projection_entry_count("p").unwrap(), 2);
}

#[test]
fn duplicate_key_in_patch_is_validation_error() {
    let dir = tempfile::tempdir().unwrap();
    let store = open_store(&dir);
    let patch = ProjectionPatch::new("p", 0).put("k", "v1").delete("k");
    let err = store
        .commit(&CommitBatch::new(Id::from(1u128), 1_000).apply_projection_patch(patch))
        .unwrap_err();
    assert!(matches!(err, CommitError::Validation(_)));
    // Nothing was persisted.
    assert_eq!(store.projection_version("p").unwrap(), 0);
}

#[test]
fn wrong_new_version_is_validation_error() {
    let dir = tempfile::tempdir().unwrap();
    let store = open_store(&dir);
    let mut patch = ProjectionPatch::new("p", 0).put("k", "v");
    patch.new_version = 3;
    let err = store
        .commit(&CommitBatch::new(Id::from(1u128), 1_000).apply_projection_patch(patch))
        .unwrap_err();
    assert!(matches!(err, CommitError::Validation(_)));
}

#[test]
fn version_conflict_is_typed_and_rolls_back() {
    let dir = tempfile::tempdir().unwrap();
    let store = open_store(&dir);
    commit_patch(&store, 1, ProjectionPatch::new("p", 0).put("a", "1"));
    let stale = ProjectionPatch::new("p", 0).put("a", "2");
    let err = store
        .commit(&CommitBatch::new(Id::from(2u128), 1_000).apply_projection_patch(stale))
        .unwrap_err();
    assert_eq!(
        err,
        CommitError::Conflict(Conflict::ProjectionVersion {
            projection: "p".into(),
            expected: 0,
            actual: 1,
        })
    );
    assert_eq!(store.projection_version("p").unwrap(), 1);
    assert_eq!(
        store.projection_get("p", b"a").unwrap(),
        Some(b"1".to_vec())
    );
}

#[test]
fn multiple_patches_to_distinct_projections_in_one_commit() {
    let dir = tempfile::tempdir().unwrap();
    let store = open_store(&dir);
    store
        .commit(
            &CommitBatch::new(Id::from(1u128), 1_000)
                .apply_projection_patch(ProjectionPatch::new("p1", 0).put("a", "1"))
                .apply_projection_patch(ProjectionPatch::new("p2", 0).put("b", "2")),
        )
        .unwrap();
    assert_eq!(
        store.projections_list().unwrap(),
        vec![("p1".to_string(), 1), ("p2".to_string(), 1)]
    );
}

#[test]
fn sequential_patches_to_same_projection_in_one_commit() {
    let dir = tempfile::tempdir().unwrap();
    let store = open_store(&dir);
    store
        .commit(
            &CommitBatch::new(Id::from(1u128), 1_000)
                .apply_projection_patch(ProjectionPatch::new("p", 0).put("a", "1"))
                .apply_projection_patch(ProjectionPatch::new("p", 1).put("b", "2")),
        )
        .unwrap();
    assert_eq!(store.projection_version("p").unwrap(), 2);
    assert_eq!(store.projection_entry_count("p").unwrap(), 2);
}

#[test]
fn reopen_persists_projection_state() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("db");
    {
        let store = ControlPlaneStore::open(&path).unwrap();
        store
            .commit(
                &CommitBatch::new(Id::from(1u128), 1_000).apply_projection_patch(
                    ProjectionPatch::new("p", 0)
                        .put(vec![0x00, 0xFF], "binary")
                        .put("plain", "text"),
                ),
            )
            .unwrap();
    }
    let reopened = ControlPlaneStore::open(&path).unwrap();
    assert_eq!(reopened.projection_version("p").unwrap(), 1);
    assert_eq!(
        reopened.projection_get("p", &[0x00, 0xFF]).unwrap(),
        Some(b"binary".to_vec())
    );
    assert_eq!(reopened.projection_entry_count("p").unwrap(), 2);
}
