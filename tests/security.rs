#[cfg(unix)]
use std::os::unix::fs::PermissionsExt;
mod common;

#[cfg(unix)]
use minisqlite::{CommitBatch, Durability, Event, Id, StoreBuilder};

#[test]
#[cfg(unix)]
fn rejects_symlinked_primary_path() {
    let tmp = common::TempDir::new();
    let real = tmp.path().join("real.mini");
    let link = tmp.path().join("link.mini");
    std::os::unix::fs::symlink(&real, &link).unwrap();

    match StoreBuilder::new(&link)
        .durability(Durability::Memory)
        .open()
    {
        Ok(_) => panic!("expected symlink error"),
        Err(e) => assert!(
            e.to_string().contains("symlink"),
            "expected symlink error, got {e}"
        ),
    }
}

#[test]
#[cfg(unix)]
fn primary_file_is_owner_only() {
    let tmp = common::TempDir::new();
    let path = tmp.path().join("secure.mini");
    {
        let _store = StoreBuilder::new(&path)
            .durability(Durability::Memory)
            .open()
            .unwrap();
    }

    let meta = std::fs::metadata(&path).unwrap();
    let mode = meta.permissions().mode();
    assert_eq!(
        mode & 0o777,
        0o600,
        "primary data file should be owner read/write only"
    );
}

#[test]
#[cfg(unix)]
fn backup_file_is_owner_only() {
    let tmp = common::TempDir::new();
    let src = tmp.path().join("secure_src.mini");
    let dest = tmp.path().join("secure_backup.mini");
    let store = StoreBuilder::new(&src)
        .durability(Durability::Memory)
        .open()
        .unwrap();
    store
        .commit(
            CommitBatch::new(Id::new().unwrap(), 0).append_event(Event::with_json_payload(
                Id::new().unwrap(),
                "s",
                "e",
                0,
                b"p",
            )),
        )
        .unwrap();
    store.backup(&dest).unwrap();
    drop(store);

    let meta = std::fs::metadata(&dest).unwrap();
    let mode = meta.permissions().mode();
    assert_eq!(
        mode & 0o777,
        0o600,
        "backup file should be owner read/write only (temp file created with 0o600)"
    );
}
