use std::path::PathBuf;
mod common;
use std::process::Command;

use minisqlite::{CommitBatch, Durability, Event, Id, JobSpec, StoreBuilder};

fn bin_path() -> PathBuf {
    std::env::var("CARGO_BIN_EXE_minisqlite")
        .map(PathBuf::from)
        .unwrap_or_else(|_| PathBuf::from("target/debug/minisqlite"))
}

fn run(args: &[&str]) -> (String, std::process::ExitStatus) {
    let output = Command::new(bin_path())
        .args(args)
        .output()
        .expect("failed to spawn minisqlite CLI; build with `cargo build --bin minisqlite`");
    let stdout = String::from_utf8_lossy(&output.stdout).into_owned();
    let stderr = String::from_utf8_lossy(&output.stderr).into_owned();
    if !output.status.success() {
        eprintln!(
            "minisqlite {:?} failed with {}\nstdout:\n{}\nstderr:\n{}",
            args, output.status, stdout, stderr
        );
    }
    (stdout, output.status)
}

#[test]
fn cli_verify_and_doctor_succeed() {
    let tmp = common::TempDir::new();
    let path = tmp.path().join("cli.mini");

    let store = StoreBuilder::new(&path)
        .durability(Durability::Memory)
        .open()
        .unwrap();
    let event =
        Event::with_json_payload(Id::new().unwrap(), "thread:abc", "thread.created", 0, b"{}");
    store
        .commit(
            CommitBatch::new(Id::new().unwrap(), 0)
                .append_event(event)
                .projection_put("threads", 1, b"thread-1".to_vec(), b"hello".to_vec()),
        )
        .unwrap();
    drop(store);

    let (_, status) = run(&[path.to_str().unwrap(), "verify"]);
    assert!(status.success());

    let (out, status) = run(&[path.to_str().unwrap(), "doctor"]);
    assert!(status.success());
    assert!(out.contains("status:        OK") || out.contains("status:        OK (tail"));

    let (out, status) = run(&[path.to_str().unwrap(), "stats", "--json"]);
    assert!(status.success());
    assert!(out.contains("\"event_count\":"));

    let (out, status) = run(&[path.to_str().unwrap(), "projections", "list"]);
    assert!(status.success());
    assert!(out.contains("threads"));

    let (out, status) = run(&[
        path.to_str().unwrap(),
        "--show-payloads",
        "projections",
        "scan",
        "threads",
        "--prefix",
        "thread-1",
    ]);
    assert!(status.success());
    assert!(out.contains("hello"));
    assert!(out.contains("thread-1"));

    let (out, status) = run(&[
        path.to_str().unwrap(),
        "--json",
        "--show-payloads",
        "projections",
        "scan",
        "threads",
        "--prefix",
        "thread-",
    ]);
    assert!(status.success());
    assert!(out.contains("\"key\":\"7468726561642d31\""));
    assert!(out.contains("\"value\":\"68656c6c6f\""));

    let (out, status) = run(&[path.to_str().unwrap(), "events", "tail", "1"]);
    assert!(status.success());
    assert!(out.contains("thread.created"));

    let (out, status) = run(&[
        path.to_str().unwrap(),
        "events",
        "stream",
        "thread:abc",
        "1",
    ]);
    assert!(status.success());
    assert!(out.contains("thread.created"));

    let (out, status) = run(&[path.to_str().unwrap(), "export", "--format", "jsonl"]);
    assert!(status.success());
    assert!(out.contains("\"type\":\"event\""));

    let backup = tmp.path().join("backup.mini");
    let (_, status) = run(&[path.to_str().unwrap(), "backup", backup.to_str().unwrap()]);
    assert!(status.success());

    let (_, status) = run(&[backup.to_str().unwrap(), "verify"]);
    assert!(status.success());
}

#[test]
fn cli_rejects_missing_source() {
    let missing = "/tmp/minisqlite_does_not_exist_7a3f9e2c.mini";
    let dest = "/tmp/minisqlite_should_not_be_created_9e8d7c6b.mini";
    // Remove any leftover files from a previous run.
    let _ = std::fs::remove_file(missing);
    let _ = std::fs::remove_file(dest);

    for args in [
        vec![missing, "verify"],
        vec![missing, "doctor"],
        vec![missing, "stats"],
        vec![missing, "events", "tail", "1"],
        vec![missing, "events", "stream", "s", "1"],
        vec![missing, "projections", "list"],
        vec![missing, "jobs", "list"],
        vec![missing, "export"],
        vec![missing, "backup", dest],
    ] {
        let output = Command::new(bin_path())
            .args(&args)
            .output()
            .expect("failed to spawn minisqlite CLI");
        assert!(
            !output.status.success(),
            "command {:?} should fail for missing source",
            args
        );
    }

    // None of the missing-source commands should have created the destination.
    assert!(!std::path::Path::new(dest).exists());
}

#[test]
fn cli_jobs_round_trip() {
    let tmp = common::TempDir::new();
    let path = tmp.path().join("jobs_cli.mini");

    let store = StoreBuilder::new(&path)
        .durability(Durability::Memory)
        .open()
        .unwrap();
    let job = JobSpec::new(Id::new().unwrap(), "provider", "p1", b"work".to_vec());
    store
        .commit(CommitBatch::new(Id::new().unwrap(), 0).enqueue_job(job))
        .unwrap();
    drop(store);

    let p = path.to_str().unwrap();
    let (out, status) = run(&[p, "jobs", "list", "--json"]);
    assert!(status.success());
    assert!(out.contains("\"state\":\"pending\""));
}
