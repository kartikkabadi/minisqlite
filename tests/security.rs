#[cfg(unix)]
use std::os::unix::fs::PermissionsExt;

#[cfg(unix)]
use minisqlite::{Durability, StoreBuilder};

#[test]
#[cfg(unix)]
fn rejects_symlinked_primary_path() {
    let tmp = tempfile::tempdir().unwrap();
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
    let tmp = tempfile::tempdir().unwrap();
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

    let lock = path.with_extension("mini.lock");
    let lock_meta = std::fs::metadata(&lock).unwrap();
    let lock_mode = lock_meta.permissions().mode();
    assert_eq!(
        lock_mode & 0o777,
        0o600,
        "lock file should be owner read/write only"
    );
}
