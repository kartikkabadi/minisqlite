//! Integration tests for projection read APIs: point reads, binary prefix and
//! range scans, and pagination edge cases.

use minisqlite::{CommitBatch, ControlPlaneStore, Id, ProjectionEntry, ProjectionPatch};

fn open_store(dir: &tempfile::TempDir) -> ControlPlaneStore {
    ControlPlaneStore::open(dir.path().join("db")).unwrap()
}

fn seed(store: &ControlPlaneStore, entries: &[(&[u8], &[u8])]) {
    let mut patch = ProjectionPatch::new("p", 0);
    for (key, value) in entries {
        patch = patch.put(key.to_vec(), value.to_vec());
    }
    store
        .commit(&CommitBatch::new(Id::from(1u128), 1_000).apply_projection_patch(patch))
        .unwrap();
}

fn keys(entries: &[ProjectionEntry]) -> Vec<Vec<u8>> {
    entries.iter().map(|e| e.key.clone()).collect()
}

#[test]
fn point_read_hits_and_misses() {
    let dir = tempfile::tempdir().unwrap();
    let store = open_store(&dir);
    seed(&store, &[(b"a", b"1")]);
    assert_eq!(
        store.projection_get("p", b"a").unwrap(),
        Some(b"1".to_vec())
    );
    assert_eq!(store.projection_get("p", b"z").unwrap(), None);
    assert_eq!(store.projection_get("other", b"a").unwrap(), None);
}

#[test]
fn prefix_scan_returns_only_matching_keys_in_order() {
    let dir = tempfile::tempdir().unwrap();
    let store = open_store(&dir);
    seed(
        &store,
        &[
            (b"ab", b"1"),
            (b"abc", b"2"),
            (b"abd", b"3"),
            (b"ac", b"4"),
            (b"b", b"5"),
        ],
    );
    let hits = store.projection_scan_prefix("p", b"ab", 100).unwrap();
    assert_eq!(
        keys(&hits),
        vec![b"ab".to_vec(), b"abc".to_vec(), b"abd".to_vec()]
    );
}

#[test]
fn empty_prefix_scans_everything() {
    let dir = tempfile::tempdir().unwrap();
    let store = open_store(&dir);
    seed(&store, &[(b"a", b"1"), (&[0xFF], b"2"), (&[0x00], b"3")]);
    let hits = store.projection_scan_prefix("p", b"", 100).unwrap();
    assert_eq!(keys(&hits), vec![vec![0x00], b"a".to_vec(), vec![0xFF]]);
}

#[test]
fn prefix_scan_handles_0xff_prefixes() {
    let dir = tempfile::tempdir().unwrap();
    let store = open_store(&dir);
    seed(
        &store,
        &[
            (&[0xFE, 0xFF], b"1"),
            (&[0xFF], b"2"),
            (&[0xFF, 0x00], b"3"),
            (&[0xFF, 0xFF], b"4"),
            (&[0xFF, 0xFF, 0x01], b"5"),
        ],
    );
    // All-0xFF prefix: no finite upper bound; scan is open-ended above.
    let hits = store.projection_scan_prefix("p", &[0xFF], 100).unwrap();
    assert_eq!(
        keys(&hits),
        vec![
            vec![0xFF],
            vec![0xFF, 0x00],
            vec![0xFF, 0xFF],
            vec![0xFF, 0xFF, 0x01]
        ]
    );
    let hits = store
        .projection_scan_prefix("p", &[0xFF, 0xFF], 100)
        .unwrap();
    assert_eq!(keys(&hits), vec![vec![0xFF, 0xFF], vec![0xFF, 0xFF, 0x01]]);
    // Prefix ending in 0xFF but with a smaller byte before it: upper bound
    // computed by dropping the 0xFF and incrementing (0xFE -> 0xFF).
    let hits = store
        .projection_scan_prefix("p", &[0xFE, 0xFF], 100)
        .unwrap();
    assert_eq!(keys(&hits), vec![vec![0xFE, 0xFF]]);
}

#[test]
fn prefix_scan_pagination_with_after_cursor() {
    let dir = tempfile::tempdir().unwrap();
    let store = open_store(&dir);
    seed(
        &store,
        &[
            (b"k1", b"1"),
            (b"k2", b"2"),
            (b"k3", b"3"),
            (b"k4", b"4"),
            (b"x", b"other"),
        ],
    );
    let page1 = store
        .projection_scan_prefix_page("p", b"k", None, 2)
        .unwrap();
    assert_eq!(keys(&page1), vec![b"k1".to_vec(), b"k2".to_vec()]);
    let page2 = store
        .projection_scan_prefix_page("p", b"k", Some(&page1[1].key), 2)
        .unwrap();
    assert_eq!(keys(&page2), vec![b"k3".to_vec(), b"k4".to_vec()]);
    let page3 = store
        .projection_scan_prefix_page("p", b"k", Some(&page2[1].key), 2)
        .unwrap();
    assert!(page3.is_empty());
}

#[test]
fn pagination_with_binary_keys_and_0xff() {
    let dir = tempfile::tempdir().unwrap();
    let store = open_store(&dir);
    seed(
        &store,
        &[
            (&[0xFF], b"1"),
            (&[0xFF, 0x00], b"2"),
            (&[0xFF, 0xFF], b"3"),
            (&[0xFF, 0xFF, 0xFF], b"4"),
        ],
    );
    let page1 = store
        .projection_scan_prefix_page("p", &[0xFF], None, 2)
        .unwrap();
    assert_eq!(keys(&page1), vec![vec![0xFF], vec![0xFF, 0x00]]);
    let page2 = store
        .projection_scan_prefix_page("p", &[0xFF], Some(&page1[1].key), 2)
        .unwrap();
    assert_eq!(keys(&page2), vec![vec![0xFF, 0xFF], vec![0xFF, 0xFF, 0xFF]]);
    let page3 = store
        .projection_scan_prefix_page("p", &[0xFF], Some(&page2[1].key), 2)
        .unwrap();
    assert!(page3.is_empty());
}

#[test]
fn range_scan_bounds_are_inclusive_exclusive() {
    let dir = tempfile::tempdir().unwrap();
    let store = open_store(&dir);
    seed(
        &store,
        &[(b"a", b"1"), (b"b", b"2"), (b"c", b"3"), (b"d", b"4")],
    );
    let hits = store
        .projection_scan_range("p", Some(b"b"), Some(b"d"), None, 100)
        .unwrap();
    assert_eq!(keys(&hits), vec![b"b".to_vec(), b"c".to_vec()]);
    // Open lower bound.
    let hits = store
        .projection_scan_range("p", None, Some(b"c"), None, 100)
        .unwrap();
    assert_eq!(keys(&hits), vec![b"a".to_vec(), b"b".to_vec()]);
    // Open upper bound.
    let hits = store
        .projection_scan_range("p", Some(b"c"), None, None, 100)
        .unwrap();
    assert_eq!(keys(&hits), vec![b"c".to_vec(), b"d".to_vec()]);
    // Empty range.
    let hits = store
        .projection_scan_range("p", Some(b"c"), Some(b"c"), None, 100)
        .unwrap();
    assert!(hits.is_empty());
}

#[test]
fn range_scan_pagination() {
    let dir = tempfile::tempdir().unwrap();
    let store = open_store(&dir);
    seed(
        &store,
        &[(b"a", b"1"), (b"b", b"2"), (b"c", b"3"), (b"d", b"4")],
    );
    let page1 = store
        .projection_scan_range("p", Some(b"a"), Some(b"e"), None, 2)
        .unwrap();
    assert_eq!(keys(&page1), vec![b"a".to_vec(), b"b".to_vec()]);
    let page2 = store
        .projection_scan_range("p", Some(b"a"), Some(b"e"), Some(&page1[1].key), 2)
        .unwrap();
    assert_eq!(keys(&page2), vec![b"c".to_vec(), b"d".to_vec()]);
}

#[test]
fn zero_limit_returns_nothing() {
    let dir = tempfile::tempdir().unwrap();
    let store = open_store(&dir);
    seed(&store, &[(b"a", b"1")]);
    assert!(store
        .projection_scan_prefix("p", b"", 0)
        .unwrap()
        .is_empty());
}

#[test]
fn scans_are_isolated_per_projection() {
    let dir = tempfile::tempdir().unwrap();
    let store = open_store(&dir);
    store
        .commit(
            &CommitBatch::new(Id::from(1u128), 1_000)
                .apply_projection_patch(ProjectionPatch::new("p1", 0).put("a", "1"))
                .apply_projection_patch(ProjectionPatch::new("p2", 0).put("a", "2")),
        )
        .unwrap();
    let hits = store.projection_scan_prefix("p1", b"", 100).unwrap();
    assert_eq!(hits, vec![ProjectionEntry::new("a", "1")]);
    assert_eq!(store.projection_entry_count("p1").unwrap(), 1);
    assert_eq!(store.projection_entry_count("missing").unwrap(), 0);
}

#[test]
fn list_projections_with_versions() {
    let dir = tempfile::tempdir().unwrap();
    let store = open_store(&dir);
    assert!(store.projections_list().unwrap().is_empty());
    store
        .commit(
            &CommitBatch::new(Id::from(1u128), 1_000)
                .apply_projection_patch(ProjectionPatch::new("b", 0).put("k", "v"))
                .apply_projection_patch(ProjectionPatch::new("a", 0).clear())
                .apply_projection_patch(ProjectionPatch::new("a", 1).put("k", "v")),
        )
        .unwrap();
    assert_eq!(
        store.projections_list().unwrap(),
        vec![("a".to_string(), 2), ("b".to_string(), 1)]
    );
}
