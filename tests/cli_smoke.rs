//! CLI smoke tests: spawn the real binary against a temp database.

use std::path::Path;
use std::process::{Command, Output};

use minisqlite::{CommitBatch, ControlPlaneStore, Event, Id};

fn seed_db(path: &Path) {
    let store = ControlPlaneStore::open(path).unwrap();
    let batch = CommitBatch::new(Id::from(1u128), 2_000)
        .append_event(Event::with_json_payload(
            Id::from(10u128),
            "s1",
            "created",
            1_000,
            b"{}",
        ))
        .append_event(Event::with_json_payload(
            Id::from(11u128),
            "s1",
            "updated",
            1_001,
            b"{}",
        ));
    store.commit(&batch).unwrap();
}

fn run_cli(args: &[&str]) -> Output {
    Command::new(env!("CARGO_BIN_EXE_minisqlite"))
        .args(args)
        .output()
        .unwrap()
}

fn stdout(output: &Output) -> String {
    String::from_utf8_lossy(&output.stdout).into_owned()
}

#[test]
fn doctor_reports_healthy_store() {
    let dir = tempfile::tempdir().unwrap();
    let db = dir.path().join("db");
    seed_db(&db);
    let output = run_cli(&["doctor", "--db", db.to_str().unwrap()]);
    assert!(output.status.success());
    let out = stdout(&output);
    assert!(out.contains("schema version: 2"));
    assert!(out.contains("verify: ok"));
}

#[test]
fn verify_succeeds_on_healthy_store() {
    let dir = tempfile::tempdir().unwrap();
    let db = dir.path().join("db");
    seed_db(&db);
    let output = run_cli(&["verify", "--db", db.to_str().unwrap()]);
    assert!(output.status.success());
    assert!(stdout(&output).contains("ok"));
}

#[test]
fn stats_prints_counts() {
    let dir = tempfile::tempdir().unwrap();
    let db = dir.path().join("db");
    seed_db(&db);
    let output = run_cli(&["stats", "--db", db.to_str().unwrap()]);
    assert!(output.status.success());
    let out = stdout(&output);
    assert!(out.contains("transactions: 1"));
    assert!(out.contains("events: 2"));
}

#[test]
fn events_tail_respects_limit() {
    let dir = tempfile::tempdir().unwrap();
    let db = dir.path().join("db");
    seed_db(&db);
    let output = run_cli(&[
        "events",
        "tail",
        "--limit",
        "1",
        "--db",
        db.to_str().unwrap(),
    ]);
    assert!(output.status.success());
    let out = stdout(&output);
    assert_eq!(out.lines().count(), 1);
    assert!(out.contains("updated"));
}

#[test]
fn backup_writes_and_refuses_existing_destination() {
    let dir = tempfile::tempdir().unwrap();
    let db = dir.path().join("db");
    seed_db(&db);
    let dest = dir.path().join("backup.db");
    let dest_str = dest.to_str().unwrap();

    let output = run_cli(&["backup", dest_str, "--db", db.to_str().unwrap()]);
    assert!(output.status.success());
    assert!(stdout(&output).contains("schema version 2"));
    assert!(dest.exists());

    let refused = run_cli(&["backup", dest_str, "--db", db.to_str().unwrap()]);
    assert!(!refused.status.success());

    let overwritten = run_cli(&[
        "backup",
        dest_str,
        "--overwrite",
        "--db",
        db.to_str().unwrap(),
    ]);
    assert!(overwritten.status.success());
}

#[test]
fn diagnostic_export_writes_to_stdout_and_file() {
    let dir = tempfile::tempdir().unwrap();
    let db = dir.path().join("db");
    seed_db(&db);
    let output = run_cli(&["diagnostic-export", "--db", db.to_str().unwrap()]);
    assert!(output.status.success());
    let out = stdout(&output);
    assert!(out.contains("\"kind\":\"header\""));
    assert!(!out.contains("payload_hex"));

    let file = dir.path().join("export.jsonl");
    let output = run_cli(&[
        "diagnostic-export",
        "--out",
        file.to_str().unwrap(),
        "--include-payloads",
        "--db",
        db.to_str().unwrap(),
    ]);
    assert!(output.status.success());
    let contents = std::fs::read_to_string(&file).unwrap();
    assert!(contents.contains("\"payloads_included\":true"));
    assert!(contents.contains("payload_hex"));
    assert!(!contents.contains("lease_token"));
}

#[test]
fn migrations_status_lists_versions() {
    let dir = tempfile::tempdir().unwrap();
    let db = dir.path().join("db");
    seed_db(&db);
    let output = run_cli(&["migrations", "status", "--db", db.to_str().unwrap()]);
    assert!(output.status.success());
    assert!(stdout(&output).contains("v1"));
}

#[test]
fn missing_db_flag_and_unknown_command_fail() {
    let output = run_cli(&["stats"]);
    assert!(!output.status.success());
    let dir = tempfile::tempdir().unwrap();
    let db = dir.path().join("db");
    let output = run_cli(&["bogus", "--db", db.to_str().unwrap()]);
    assert!(!output.status.success());
}

#[test]
fn nonexistent_db_path_errors_without_creating_a_file() {
    let dir = tempfile::tempdir().unwrap();
    let db = dir.path().join("typo.db");
    let output = run_cli(&["stats", "--db", db.to_str().unwrap()]);
    assert!(!output.status.success());
    assert!(String::from_utf8_lossy(&output.stderr).contains("does not exist"));
    assert!(!db.exists(), "inspection command created a database file");
}

#[test]
fn verify_and_backup_error_on_nonexistent_db_without_creating_files() {
    let dir = tempfile::tempdir().unwrap();
    let db = dir.path().join("typo.db");
    let dest = dir.path().join("backup.db");

    let output = run_cli(&["verify", "--db", db.to_str().unwrap()]);
    assert!(!output.status.success());
    assert!(String::from_utf8_lossy(&output.stderr).contains("does not exist"));
    assert!(!db.exists(), "verify created a database file");

    let output = run_cli(&[
        "backup",
        dest.to_str().unwrap(),
        "--db",
        db.to_str().unwrap(),
    ]);
    assert!(!output.status.success());
    assert!(String::from_utf8_lossy(&output.stderr).contains("does not exist"));
    assert!(!db.exists(), "backup created a database file");
    assert!(!dest.exists(), "backup of a missing database wrote a file");
}

#[test]
fn unknown_command_creates_no_file() {
    let dir = tempfile::tempdir().unwrap();
    let db = dir.path().join("db");
    let output = run_cli(&["frobnicate", "--db", db.to_str().unwrap()]);
    assert!(!output.status.success());
    assert!(!db.exists(), "unknown command created a database file");
}
