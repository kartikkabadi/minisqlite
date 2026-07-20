#![cfg(feature = "failpoint")]

use std::path::PathBuf;
use std::process::{Command, Stdio};

mod common;

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

/// Run a failpoint that is expected to abort the child process before commit returns.
fn run_failpoint_abort(failpoint: &str, path: &std::path::Path) {
    let status = Command::new(crash_driver_path())
        .arg(path)
        .arg(failpoint)
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .status()
        .expect("failed to spawn crash driver; run `cargo build --bin crash_driver --features failpoint` first");

    assert!(
        !status.success(),
        "failpoint {failpoint}: expected child to abort, but it exited successfully"
    );
}

fn assert_first_commit_only(path: &std::path::Path) {
    let store = StoreBuilder::new(path)
        .durability(Durability::Memory)
        .open()
        .unwrap();
    assert!(
        !store.is_poisoned(),
        "store must not be poisoned after reopen"
    );
    assert_eq!(store.high_water_sequence(), 1);
    assert_eq!(store.stream_version("stream"), Some(1));
    assert_eq!(
        store.get_projection("state", b"key").unwrap().as_deref(),
        Some(b"first".as_slice())
    );
    assert_eq!(store.stats().job_count, 1);
}

fn assert_both_commits(path: &std::path::Path) {
    let store = StoreBuilder::new(path)
        .durability(Durability::Memory)
        .open()
        .unwrap();
    assert!(
        !store.is_poisoned(),
        "store must not be poisoned after reopen"
    );
    assert_eq!(store.high_water_sequence(), 2);
    assert_eq!(store.stream_version("stream"), Some(2));
    assert_eq!(
        store.get_projection("state", b"key").unwrap().as_deref(),
        Some(b"second".as_slice())
    );
    assert_eq!(store.stats().job_count, 2);
}

/// After a full write without fsync, the OS may or may not have persisted the frame.
fn assert_first_or_both_commits(path: &std::path::Path) {
    let store = StoreBuilder::new(path)
        .durability(Durability::Memory)
        .open()
        .unwrap();
    assert!(
        !store.is_poisoned(),
        "store must not be poisoned after reopen"
    );
    let high = store.high_water_sequence();
    assert!(
        high == 1 || high == 2,
        "unexpected high water sequence {high}"
    );
    if high == 1 {
        assert_eq!(store.stream_version("stream"), Some(1));
        assert_eq!(
            store.get_projection("state", b"key").unwrap().as_deref(),
            Some(b"first".as_slice())
        );
        assert_eq!(store.stats().job_count, 1);
    } else {
        assert_eq!(store.stream_version("stream"), Some(2));
        assert_eq!(
            store.get_projection("state", b"key").unwrap().as_deref(),
            Some(b"second".as_slice())
        );
        assert_eq!(store.stats().job_count, 2);
    }
}

#[test]
fn crash_before_append_recovers_old_state() {
    let tmp = common::TempDir::new();
    let path = tmp.path().join("crash.mini");
    run_failpoint_abort("before-append", &path);
    assert_first_commit_only(&path);
}

#[test]
fn crash_partial_header_recovers_old_state() {
    let tmp = common::TempDir::new();
    let path = tmp.path().join("crash.mini");
    run_failpoint_abort("partial-header", &path);
    assert_first_commit_only(&path);
}

#[test]
fn crash_during_payload_recovers_old_state() {
    let tmp = common::TempDir::new();
    let path = tmp.path().join("crash.mini");
    run_failpoint_abort("during-payload", &path);
    assert_first_commit_only(&path);
}

#[test]
fn crash_after_payload_recovers_old_state() {
    let tmp = common::TempDir::new();
    let path = tmp.path().join("crash.mini");
    run_failpoint_abort("after-payload", &path);
    assert_first_commit_only(&path);
}

#[test]
fn crash_after_trailer_may_recover_old_or_new_state() {
    let tmp = common::TempDir::new();
    let path = tmp.path().join("crash.mini");
    run_failpoint_abort("after-trailer", &path);
    assert_first_or_both_commits(&path);
}

#[test]
fn crash_before_sync_may_recover_old_or_new_state() {
    let tmp = common::TempDir::new();
    let path = tmp.path().join("crash.mini");
    run_failpoint_abort("before-sync", &path);
    assert_first_or_both_commits(&path);
}

#[test]
fn crash_after_sync_recovers_new_state() {
    let tmp = common::TempDir::new();
    let path = tmp.path().join("crash.mini");
    run_failpoint_abort("after-sync", &path);
    assert_both_commits(&path);
}

#[test]
fn crash_before_memory_apply_recovers_new_state() {
    let tmp = common::TempDir::new();
    let path = tmp.path().join("crash.mini");
    run_failpoint_abort("before-memory-apply", &path);
    assert_both_commits(&path);
}

#[test]
fn crash_after_memory_apply_recovers_new_state() {
    let tmp = common::TempDir::new();
    let path = tmp.path().join("crash.mini");
    run_failpoint_abort("after-memory-apply", &path);
    assert_both_commits(&path);
}

fn run_failpoint_with_output(failpoint: &str, path: &std::path::Path) -> String {
    let output = Command::new(crash_driver_path())
        .arg(path)
        .arg(failpoint)
        .stdout(Stdio::piped())
        .stderr(Stdio::null())
        .output()
        .expect("failed to spawn crash driver; run `cargo build --bin crash_driver --features failpoint` first");
    String::from_utf8_lossy(&output.stdout).into_owned()
}

#[test]
fn disk_full_short_write_returns_error() {
    let tmp = common::TempDir::new();
    let path = tmp.path().join("short-write.mini");
    let out = run_failpoint_with_output("append-error", &path);
    assert!(out.contains("Io"), "expected Io error, got: {out}");
    assert_first_commit_only(&path);
}

#[test]
fn sync_failure_returns_error() {
    let tmp = common::TempDir::new();
    let path = tmp.path().join("sync-fail.mini");
    let out = run_failpoint_with_output("sync-error", &path);
    assert!(out.contains("Io"), "expected Io error, got: {out}");
    assert_first_commit_only(&path);
}

#[test]
fn rollback_failure_returns_uncertain_outcome() {
    let tmp = common::TempDir::new();
    let path = tmp.path().join("rollback-fail.mini");
    let out = run_failpoint_with_output("rollback-error", &path);
    assert!(
        out.contains("CommitOutcomeUncertain"),
        "expected uncertain outcome, got: {out}"
    );
    assert_first_commit_only(&path);
}
