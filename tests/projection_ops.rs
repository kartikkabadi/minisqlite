use minisqlite::{CommitBatch, Durability, Error, Id, ProjectionEntry, Store, StoreBuilder};
mod common;

fn store() -> (common::TempDir, Store) {
    let tmp = common::TempDir::new();
    let store = StoreBuilder::new(tmp.path().join("proj.mini"))
        .durability(Durability::Memory)
        .open()
        .unwrap();
    (tmp, store)
}

#[test]
fn put_and_get_round_trip() {
    let (_tmp, store) = store();
    store
        .commit(CommitBatch::new(Id::new().unwrap(), 0).projection_put(
            "kv",
            1,
            b"hello".to_vec(),
            b"world".to_vec(),
        ))
        .unwrap();

    let value = store.get_projection("kv", b"hello").unwrap().unwrap();
    assert_eq!(value, b"world");
    assert_eq!(store.projection_version("kv").unwrap(), 1);
}

#[test]
fn delete_removes_key() {
    let (_tmp, store) = store();
    store
        .commit(
            CommitBatch::new(Id::new().unwrap(), 0)
                .projection_put("kv", 1, b"a".to_vec(), b"1".to_vec())
                .projection_put("kv", 2, b"b".to_vec(), b"2".to_vec()),
        )
        .unwrap();

    store
        .commit(CommitBatch::new(Id::new().unwrap(), 0).projection_delete("kv", 3, b"a".to_vec()))
        .unwrap();

    assert!(store.get_projection("kv", b"a").unwrap().is_none());
    assert!(store.get_projection("kv", b"b").unwrap().is_some());
    assert_eq!(store.projection_version("kv").unwrap(), 3);
}

#[test]
fn clear_removes_all_keys() {
    let (_tmp, store) = store();
    store
        .commit(
            CommitBatch::new(Id::new().unwrap(), 0)
                .projection_put("kv", 1, b"a".to_vec(), b"1".to_vec())
                .projection_put("kv", 2, b"b".to_vec(), b"2".to_vec()),
        )
        .unwrap();

    store
        .commit(CommitBatch::new(Id::new().unwrap(), 0).projection_clear("kv", 3))
        .unwrap();

    let all = store.scan_projection_prefix("kv", b"").unwrap();
    assert!(all.is_empty());
    assert_eq!(store.projection_version("kv").unwrap(), 3);
}

#[test]
fn replace_atomically_swaps_contents() {
    let (_tmp, store) = store();
    store
        .commit(CommitBatch::new(Id::new().unwrap(), 0).projection_put(
            "kv",
            1,
            b"old".to_vec(),
            b"x".to_vec(),
        ))
        .unwrap();

    let entries = vec![
        ProjectionEntry::new(b"a".to_vec(), b"1".to_vec()),
        ProjectionEntry::new(b"b".to_vec(), b"2".to_vec()),
    ];
    store
        .commit(CommitBatch::new(Id::new().unwrap(), 0).projection_replace("kv", 2, entries))
        .unwrap();

    assert!(store.get_projection("kv", b"old").unwrap().is_none());
    assert_eq!(store.get_projection("kv", b"a").unwrap().unwrap(), b"1");
    assert_eq!(store.get_projection("kv", b"b").unwrap().unwrap(), b"2");
    assert_eq!(store.projection_version("kv").unwrap(), 2);
}

#[test]
fn version_mismatch_fails() {
    let (_tmp, store) = store();
    store
        .commit(CommitBatch::new(Id::new().unwrap(), 0).projection_put(
            "kv",
            1,
            b"k".to_vec(),
            b"v".to_vec(),
        ))
        .unwrap();

    let result = store.commit(CommitBatch::new(Id::new().unwrap(), 0).projection_put(
        "kv",
        3,
        b"k".to_vec(),
        b"v2".to_vec(),
    ));
    assert!(result.is_err(), "skipping a version must fail");

    let result = store.commit(CommitBatch::new(Id::new().unwrap(), 0).projection_put(
        "kv",
        1,
        b"k".to_vec(),
        b"v2".to_vec(),
    ));
    assert!(
        result.is_err(),
        "reusing an old version must fail unless the op is a no-op"
    );
}

#[test]
fn no_op_same_version_is_allowed() {
    let (_tmp, store) = store();
    store
        .commit(CommitBatch::new(Id::new().unwrap(), 0).projection_put(
            "kv",
            1,
            b"k".to_vec(),
            b"v".to_vec(),
        ))
        .unwrap();

    // Re-putting the same key/value with the same version is a no-op.
    store
        .commit(CommitBatch::new(Id::new().unwrap(), 0).projection_put(
            "kv",
            1,
            b"k".to_vec(),
            b"v".to_vec(),
        ))
        .unwrap();
    assert_eq!(store.projection_version("kv").unwrap(), 1);
}

#[test]
fn prefix_scan_is_ordered() {
    let (_tmp, store) = store();
    let mut batch = CommitBatch::new(Id::new().unwrap(), 0);
    for (i, key) in ["c", "a", "b"].into_iter().enumerate() {
        let version = (i + 1) as u64;
        batch = batch.projection_put(
            "kv",
            version,
            key.as_bytes().to_vec(),
            key.as_bytes().to_vec(),
        );
    }
    store.commit(batch).unwrap();

    let prefix = store.scan_projection_prefix("kv", b"").unwrap();
    let keys: Vec<_> = prefix
        .iter()
        .map(|e| String::from_utf8(e.key.clone()).unwrap())
        .collect();
    assert_eq!(keys, vec!["a", "b", "c"]);
    assert_eq!(store.projection_version("kv").unwrap(), 3);
}

#[test]
fn range_scan_excludes_end() {
    let (_tmp, store) = store();
    let mut batch = CommitBatch::new(Id::new().unwrap(), 0);
    for (i, key) in ["a", "b", "c", "d"].into_iter().enumerate() {
        let version = (i + 1) as u64;
        batch = batch.projection_put(
            "kv",
            version,
            key.as_bytes().to_vec(),
            key.as_bytes().to_vec(),
        );
    }
    store.commit(batch).unwrap();

    let range = store.scan_projection_range("kv", b"b", b"d").unwrap();
    let keys: Vec<_> = range
        .iter()
        .map(|e| String::from_utf8(e.key.clone()).unwrap())
        .collect();
    assert_eq!(keys, vec!["b", "c"]);
    assert_eq!(store.projection_version("kv").unwrap(), 4);
}

#[test]
fn projection_persisted_across_reopen() {
    let (tmp, store) = store();
    store
        .commit(CommitBatch::new(Id::new().unwrap(), 0).projection_put(
            "kv",
            1,
            b"k".to_vec(),
            b"v".to_vec(),
        ))
        .unwrap();
    drop(store);

    let path = tmp.path().join("proj.mini");
    let store = StoreBuilder::new(&path)
        .durability(Durability::Memory)
        .open()
        .unwrap();
    assert_eq!(store.get_projection("kv", b"k").unwrap().unwrap(), b"v");
    assert_eq!(store.projection_version("kv").unwrap(), 1);
}

#[test]
fn projection_not_found_error() {
    let (_tmp, store) = store();
    let result = store.get_projection("missing", b"k");
    assert!(matches!(result, Err(Error::ProjectionNotFound(_))));
}

#[test]
fn delete_on_missing_projection_materializes_empty_projection() {
    let (_tmp, store) = store();
    // Deleting from a projection that does not yet exist should create it at version 1
    // so a later mutation at version 2 does not conflict.
    store
        .commit(CommitBatch::new(Id::new().unwrap(), 0).projection_delete("kv", 1, b"k".to_vec()))
        .unwrap();

    assert_eq!(store.projection_version("kv").unwrap(), 1);
    assert!(store.get_projection("kv", b"k").unwrap().is_none());

    store
        .commit(CommitBatch::new(Id::new().unwrap(), 0).projection_put(
            "kv",
            2,
            b"k".to_vec(),
            b"v".to_vec(),
        ))
        .unwrap();
    assert_eq!(store.get_projection("kv", b"k").unwrap().unwrap(), b"v");
}
