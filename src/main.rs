//! Operational CLI for a minisqlite control-plane store.
//!
//! Usage: `minisqlite <command> --db <path> [options]`

use std::process::ExitCode;

use minisqlite::{ControlPlaneStore, JobState};

const USAGE: &str = "\
minisqlite — control-plane state kernel CLI

USAGE:
    minisqlite <command> --db <path> [options]

COMMANDS:
    doctor                         open the store and run a quick health check
    verify                         run full integrity and semantic checks
    stats                          print store-wide statistics
    events tail [--limit N]        print the most recent events
    projections list               list projections and versions
    jobs list [--queue Q] [--state S]
                                   list jobs, optionally filtered
    backup <dest> [--overwrite]    copy the database to <dest>
    diagnostic-export [--out FILE] [--include-payloads]
                                   write a redacted diagnostic export
    migrations status              print migration versions and checksums

The CLI never creates or migrates databases; it opens existing files only.
To create or migrate a store, open it with the library (StoreBuilder::open).
";

fn main() -> ExitCode {
    let args: Vec<String> = std::env::args().skip(1).collect();
    match run(&args) {
        Ok(()) => ExitCode::SUCCESS,
        Err(message) => {
            eprintln!("error: {message}");
            ExitCode::FAILURE
        }
    }
}

/// Parsed common flags plus the remaining positional arguments.
struct Parsed {
    db: String,
    positional: Vec<String>,
    limit: Option<usize>,
    queue: Option<String>,
    state: Option<String>,
    out: Option<String>,
    overwrite: bool,
    include_payloads: bool,
}

fn parse(args: &[String]) -> Result<Parsed, String> {
    let mut db = None;
    let mut positional = Vec::new();
    let mut limit = None;
    let mut queue = None;
    let mut state = None;
    let mut out = None;
    let mut overwrite = false;
    let mut include_payloads = false;
    let mut iter = args.iter();
    while let Some(arg) = iter.next() {
        match arg.as_str() {
            "--db" => db = Some(flag_value(&mut iter, "--db")?),
            "--limit" => {
                let raw = flag_value(&mut iter, "--limit")?;
                limit = Some(raw.parse().map_err(|_| format!("invalid --limit: {raw}"))?);
            }
            "--queue" => queue = Some(flag_value(&mut iter, "--queue")?),
            "--state" => state = Some(flag_value(&mut iter, "--state")?),
            "--out" => out = Some(flag_value(&mut iter, "--out")?),
            "--overwrite" => overwrite = true,
            "--include-payloads" => include_payloads = true,
            other if other.starts_with("--") => return Err(format!("unknown flag: {other}")),
            other => positional.push(other.to_string()),
        }
    }
    Ok(Parsed {
        db: db.ok_or("missing required --db <path>")?,
        positional,
        limit,
        queue,
        state,
        out,
        overwrite,
        include_payloads,
    })
}

fn flag_value(iter: &mut std::slice::Iter<'_, String>, flag: &str) -> Result<String, String> {
    iter.next()
        .map(|s| s.to_string())
        .ok_or_else(|| format!("{flag} requires a value"))
}

fn parse_state(raw: &str) -> Result<JobState, String> {
    match raw.to_ascii_lowercase().as_str() {
        "pending" => Ok(JobState::Pending),
        "leased" => Ok(JobState::Leased),
        "retry-wait" | "retrywait" => Ok(JobState::RetryWait),
        "uncertain" => Ok(JobState::Uncertain),
        "succeeded" => Ok(JobState::Succeeded),
        "dead" => Ok(JobState::Dead),
        "cancelled" => Ok(JobState::Cancelled),
        other => Err(format!("unknown job state: {other}")),
    }
}

fn run(args: &[String]) -> Result<(), String> {
    if args.is_empty() || args[0] == "--help" || args[0] == "help" {
        print!("{USAGE}");
        return Ok(());
    }
    let parsed = parse(&args[1..])?;
    let command = (
        args[0].as_str(),
        parsed.positional.first().map(String::as_str),
    );

    // Validate the command before touching the database: an unknown command or a
    // typo'd path must never create or migrate a database file.
    match command {
        ("doctor", None)
        | ("verify", None)
        | ("stats", None)
        | ("events", Some("tail"))
        | ("projections", Some("list"))
        | ("jobs", Some("list"))
        | ("backup", Some(_))
        | ("diagnostic-export", None)
        | ("migrations", Some("status")) => {}
        _ => return Err(format!("unknown command: {}\n{USAGE}", args.join(" "))),
    }

    // All CLI commands are inspection commands: open the existing database without
    // creating it and without migrating it.
    let store = ControlPlaneStore::open_existing(&parsed.db).map_err(|e| e.to_string())?;

    match command {
        ("doctor", None) => {
            println!("store opened: {}", parsed.db);
            let stats = store.stats().map_err(|e| e.to_string())?;
            println!("schema version: {}", stats.migration_version);
            println!(
                "transactions: {}, events: {}, streams: {}",
                stats.transactions, stats.events, stats.streams
            );
            println!(
                "jobs by state: {:?}, active partitions: {}",
                stats.jobs_by_state, stats.active_partitions
            );
            println!("file size: {} bytes", stats.file_size_bytes);
            let report = store.verify().map_err(|e| e.to_string())?;
            if report.is_ok() {
                println!("verify: ok");
                Ok(())
            } else {
                for finding in &report.findings {
                    println!("{}: {}", finding.check, finding.detail);
                }
                Err(format!("verify: {} finding(s)", report.findings.len()))
            }
        }
        ("verify", None) => {
            let report = store.verify().map_err(|e| e.to_string())?;
            if report.is_ok() {
                println!("ok");
                Ok(())
            } else {
                for finding in &report.findings {
                    println!("{}: {}", finding.check, finding.detail);
                }
                Err(format!("{} finding(s)", report.findings.len()))
            }
        }
        ("stats", None) => {
            let stats = store.stats().map_err(|e| e.to_string())?;
            println!("{stats:#?}");
            Ok(())
        }
        ("events", Some("tail")) => {
            let limit = parsed.limit.unwrap_or(10);
            for event in store.last_events(limit).map_err(|e| e.to_string())? {
                println!(
                    "{} {} {}@{} {}",
                    event.global_sequence,
                    event.event.event_id,
                    event.event.stream_id,
                    event.stream_version,
                    event.event.event_type
                );
            }
            Ok(())
        }
        ("projections", Some("list")) => {
            for (projection, version) in store.projections_list().map_err(|e| e.to_string())? {
                println!("{projection} v{version}");
            }
            Ok(())
        }
        ("jobs", Some("list")) => {
            let state = parsed.state.as_deref().map(parse_state).transpose()?;
            let limit = parsed.limit.unwrap_or(100);
            let jobs = store
                .jobs(parsed.queue.as_deref(), state, limit)
                .map_err(|e| e.to_string())?;
            for job in jobs {
                println!(
                    "{} {} {} {:?} attempt {}",
                    job.job_id,
                    job.spec.queue(),
                    job.spec.partition_key(),
                    job.state,
                    job.attempt
                );
            }
            Ok(())
        }
        ("backup", Some(dest)) => {
            store
                .backup(dest, parsed.overwrite)
                .map_err(|e| e.to_string())?;
            let version = store
                .migrations_status()
                .map_err(|e| e.to_string())?
                .iter()
                .map(|s| s.version)
                .max()
                .unwrap_or(0);
            println!("backup written to {dest} (schema version {version})");
            Ok(())
        }
        ("diagnostic-export", None) => {
            let export = store
                .diagnostic_export_with(parsed.include_payloads)
                .map_err(|e| e.to_string())?;
            match parsed.out {
                Some(path) => {
                    std::fs::write(&path, export).map_err(|e| e.to_string())?;
                    println!("diagnostic export written to {path}");
                }
                None => print!("{export}"),
            }
            Ok(())
        }
        ("migrations", Some("status")) => {
            for status in store.migrations_status().map_err(|e| e.to_string())? {
                println!(
                    "v{} applied_at_ms={} checksum={}",
                    status.version,
                    status.applied_at_ms,
                    if status.checksum_ok { "ok" } else { "MISMATCH" }
                );
            }
            Ok(())
        }
        _ => unreachable!("command validated before dispatch"),
    }
}
