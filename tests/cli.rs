use std::path::PathBuf;
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
    let tmp = tempfile::tempdir().unwrap();
    let path = tmp.path().join("cli.mini");

    let store = StoreBuilder::new(&path)
        .durability(Durability::Memory)
        .open()
        .unwrap();
    let event = Event::with_json_payload(Id::new(), "thread:abc", "thread.created", 0, b"{}");
    store
        .commit(
            CommitBatch::new(Id::new(), 0)
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
        "projections",
        "get",
        "threads",
        "thread-1",
    ]);
    assert!(status.success());
    assert!(out.contains("hello"));

    let (out, status) = run(&[
        path.to_str().unwrap(),
        "--json",
        "projections",
        "scan",
        "threads",
        "--prefix",
        "thread-",
    ]);
    assert!(status.success());
    assert!(out.contains("thread-1"));

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
fn cli_jobs_round_trip() {
    let tmp = tempfile::tempdir().unwrap();
    let path = tmp.path().join("jobs_cli.mini");

    let store = StoreBuilder::new(&path)
        .durability(Durability::Memory)
        .open()
        .unwrap();
    let job = JobSpec::new(Id::new(), "provider", "p1", b"work".to_vec());
    store
        .commit(CommitBatch::new(Id::new(), 0).enqueue_job(job))
        .unwrap();
    drop(store);

    let p = path.to_str().unwrap();
    let (out, status) = run(&[p, "jobs", "list", "--json"]);
    assert!(status.success());
    assert!(out.contains("\"state\":\"pending\""));
}
