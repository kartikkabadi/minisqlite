use std::path::PathBuf;
use std::process::{Command, Stdio};

use minisqlite::{Durability, StoreBuilder};

fn crash_driver_path() -> PathBuf {
    if let Ok(p) = std::env::var("CARGO_BIN_EXE_crash_driver") {
        return p.into();
    }
    let target = std::env::var("CARGO_TARGET_DIR")
        .map(PathBuf::from)
        .unwrap_or_else(|_| PathBuf::from("target"));
    target.join("debug").join("crash_driver")
}

fn run_failpoint(failpoint: &str, path: &std::path::Path) {
    let status = Command::new(crash_driver_path())
        .arg(path)
        .arg(failpoint)
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .status()
        .expect("failed to spawn crash driver; run `cargo build --bin crash_driver --features failpoint` first");

    // The child aborts on a failpoint, which is a non-zero exit.
    // We only care that the command was able to start.
    let _ = status;
}

fn assert_valid_state(path: &std::path::Path) {
    let store = StoreBuilder::new(path)
        .durability(Durability::Memory)
        .open()
        .unwrap();

    let high = store.high_water_sequence();
    assert!(
        high == 1 || high == 2,
        "unexpected high water sequence {}",
        high
    );

    if high == 1 {
        // Only the first commit survived.
        assert_eq!(store.stream_version("stream"), Some(1));
        assert_eq!(
            store.get_projection("state", b"key").unwrap().as_deref(),
            Some(b"first".as_slice())
        );
        assert_eq!(store.stats().job_count, 1);
    } else {
        // The second commit is fully present and consistent.
        assert_eq!(store.stream_version("stream"), Some(2));
        assert_eq!(
            store.get_projection("state", b"key").unwrap().as_deref(),
            Some(b"second".as_slice())
        );
        assert_eq!(store.stats().job_count, 2);
    }
}

#[test]
fn crash_before_append_recovers() {
    let tmp = tempfile::tempdir().unwrap();
    let path = tmp.path().join("crash.mini");
    run_failpoint("before-append", &path);
    assert_valid_state(&path);
}

#[test]
fn crash_partial_header_recovers() {
    let tmp = tempfile::tempdir().unwrap();
    let path = tmp.path().join("crash.mini");
    run_failpoint("partial-header", &path);
    assert_valid_state(&path);
}

#[test]
fn crash_during_payload_recovers() {
    let tmp = tempfile::tempdir().unwrap();
    let path = tmp.path().join("crash.mini");
    run_failpoint("during-payload", &path);
    assert_valid_state(&path);
}

#[test]
fn crash_after_payload_recovers() {
    let tmp = tempfile::tempdir().unwrap();
    let path = tmp.path().join("crash.mini");
    run_failpoint("after-payload", &path);
    assert_valid_state(&path);
}

#[test]
fn crash_after_trailer_recovers() {
    let tmp = tempfile::tempdir().unwrap();
    let path = tmp.path().join("crash.mini");
    run_failpoint("after-trailer", &path);
    assert_valid_state(&path);
}

#[test]
fn crash_before_sync_recovers() {
    let tmp = tempfile::tempdir().unwrap();
    let path = tmp.path().join("crash.mini");
    run_failpoint("before-sync", &path);
    assert_valid_state(&path);
}

#[test]
fn crash_after_sync_recovers() {
    let tmp = tempfile::tempdir().unwrap();
    let path = tmp.path().join("crash.mini");
    run_failpoint("after-sync", &path);
    assert_valid_state(&path);
}

#[test]
fn crash_before_memory_apply_recovers() {
    let tmp = tempfile::tempdir().unwrap();
    let path = tmp.path().join("crash.mini");
    run_failpoint("before-memory-apply", &path);
    assert_valid_state(&path);
}

#[test]
fn crash_after_memory_apply_recovers() {
    let tmp = tempfile::tempdir().unwrap();
    let path = tmp.path().join("crash.mini");
    run_failpoint("after-memory-apply", &path);
    assert_valid_state(&path);
}
