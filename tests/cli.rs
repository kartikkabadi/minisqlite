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

/// Build a store with two commits and return (path, file length after first commit,
/// file length after second commit).
fn two_commit_store(tmp: &common::TempDir, name: &str) -> (PathBuf, u64, u64) {
    let path = tmp.path().join(name);
    let store = StoreBuilder::new(&path)
        .durability(Durability::Memory)
        .open()
        .unwrap();
    store
        .commit(
            CommitBatch::new(Id::new().unwrap(), 0).append_event(Event::with_json_payload(
                Id::new().unwrap(),
                "s",
                "first",
                0,
                b"{}",
            )),
        )
        .unwrap();
    let after_first = std::fs::metadata(&path).unwrap().len();
    store
        .commit(
            CommitBatch::new(Id::new().unwrap(), 0).append_event(Event::with_json_payload(
                Id::new().unwrap(),
                "s",
                "second",
                0,
                b"{}",
            )),
        )
        .unwrap();
    let after_second = std::fs::metadata(&path).unwrap().len();
    drop(store);
    (path, after_first, after_second)
}

#[test]
fn cli_repair_clean_store_is_a_no_op() {
    let tmp = common::TempDir::new();
    let (path, _, len) = two_commit_store(&tmp, "repair_clean.mini");
    let p = path.to_str().unwrap();

    let (out, status) = run(&[p, "repair", "--json"]);
    assert!(status.success());
    assert!(out.contains(&format!("\"file_length\":{len}")));
    assert!(out.contains(&format!("\"last_valid_offset\":{len}")));
    assert!(out.contains("\"bytes_removed\":0"));
    assert!(out.contains("\"repaired\":false"));
    assert_eq!(std::fs::metadata(&path).unwrap().len(), len);
}

#[test]
fn cli_repair_truncates_torn_tail_only_with_force() {
    let tmp = common::TempDir::new();
    let (path, after_first, after_second) = two_commit_store(&tmp, "repair_torn.mini");
    // Tear the final frame in half, as a crashed writer would.
    let torn_len = after_first + (after_second - after_first) / 2;
    let f = std::fs::OpenOptions::new().write(true).open(&path).unwrap();
    f.set_len(torn_len).unwrap();
    drop(f);
    let p = path.to_str().unwrap();

    // Without --force: report the plan, leave the file untouched, exit non-zero.
    let output = Command::new(bin_path())
        .args([p, "repair", "--json"])
        .output()
        .unwrap();
    assert_eq!(output.status.code(), Some(23));
    let out = String::from_utf8_lossy(&output.stdout);
    assert!(out.contains(&format!("\"file_length\":{torn_len}")));
    assert!(out.contains(&format!("\"last_valid_offset\":{after_first}")));
    assert!(out.contains("\"needs_repair\":true"));
    assert!(out.contains("\"repaired\":false"));
    assert_eq!(std::fs::metadata(&path).unwrap().len(), torn_len);

    // With --force: truncate to the last valid offset and verify cleanly.
    let (out, status) = run(&[p, "repair", "--force", "--json"]);
    assert!(status.success());
    assert!(out.contains(&format!("\"bytes_removed\":{}", torn_len - after_first)));
    assert!(out.contains("\"repaired\":true"));
    assert_eq!(std::fs::metadata(&path).unwrap().len(), after_first);
    let (_, status) = run(&[p, "verify"]);
    assert!(status.success());
}

#[test]
fn cli_repair_refuses_complete_frame_corruption() {
    let tmp = common::TempDir::new();
    let (path, after_first, after_second) = two_commit_store(&tmp, "repair_corrupt.mini");
    // Flip a byte inside the first (complete, non-tail) frame.
    let mut bytes = std::fs::read(&path).unwrap();
    let inside_first_frame = (after_first - 8) as usize;
    bytes[inside_first_frame] ^= 0xff;
    std::fs::write(&path, &bytes).unwrap();
    let p = path.to_str().unwrap();

    let output = Command::new(bin_path())
        .args([p, "repair", "--force"])
        .output()
        .unwrap();
    assert_eq!(
        output.status.code(),
        Some(5),
        "complete-frame corruption must fail as corruption, not be truncated"
    );
    assert_eq!(
        std::fs::metadata(&path).unwrap().len(),
        after_second,
        "repair must never truncate mid-file corruption"
    );
}

#[cfg(feature = "failpoint")]
#[test]
fn cli_repair_reports_uncertain_outcome_when_sync_fails() {
    let tmp = common::TempDir::new();
    let (path, after_first, after_second) = two_commit_store(&tmp, "repair_uncertain.mini");
    let torn_len = after_first + (after_second - after_first) / 2;
    let f = std::fs::OpenOptions::new().write(true).open(&path).unwrap();
    f.set_len(torn_len).unwrap();
    drop(f);

    let output = Command::new(bin_path())
        .args([path.to_str().unwrap(), "repair", "--force"])
        .env("MINISQLITE_FAILPOINT", "truncate-sync-error")
        .output()
        .unwrap();
    assert_eq!(
        output.status.code(),
        Some(24),
        "a failed truncate sync must surface RepairOutcomeUncertain"
    );
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
