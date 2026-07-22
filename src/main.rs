//! Operational CLI for a minisqlite control-plane store.
//!
//! Usage: `minisqlite <command> --db <path> [options]`
//!
//! Exit codes: 0 success, 1 operational error, 2 usage error,
//! 3 verification findings, 4 not found.

use std::process::ExitCode;

use minisqlite::{CommitBatch, ControlPlaneStore, Id, JobState, Resolution};

const USAGE: &str = "\
minisqlite — atomic events, state, and jobs on SQLite

USAGE:
    minisqlite [store] <command> --db <path> [options]

COMMANDS:
    doctor                         open the store and run a quick health check
    verify                         run full integrity and semantic checks
    stats                          print store-wide statistics
    events tail [--limit N]        print the most recent events
    events stream <stream-id> [--from V] [--limit N]
                                   print one stream's events from version V
    projections list               list projections and versions
    projections scan <projection> [--prefix HEX] [--limit N]
                                   scan a projection's entries in key order
    projections get <projection> <key-hex>
                                   print one projection entry
    jobs list [--queue Q] [--state S] [--after N]
                                   list jobs, optionally filtered; --after
                                   pages by the printed cursor
    jobs show <job-id>             print one job's full status
    jobs uncertain [--limit N]     list jobs with uncertain outcomes
    jobs resolve <job-id> <retry|succeeded|dead>
                                   resolve an uncertain job (opens read-write)
    backup <dest> [--overwrite]    copy the database to <dest>
    diagnostic-export [--out FILE] [--include-payloads]
                                   write a redacted diagnostic export
    migrations status              print migration versions and checksums

EXIT CODES:
    0 success   1 operational error   2 usage error
    3 verification findings          4 not found

Except for `jobs resolve`, the CLI never creates or migrates databases; it
opens existing files read-only. To create or migrate a store, open it with
the library (StoreBuilder::open).
";

/// A CLI failure carrying its exit-code class.
enum CliError {
    /// Bad arguments or unknown command (exit 2).
    Usage(String),
    /// The store, or a requested entity within it, does not exist (exit 4).
    NotFound(String),
    /// Verification completed and reported findings (exit 3).
    Findings(usize),
    /// Any other operational failure (exit 1).
    Op(String),
}

impl CliError {
    fn report(self) -> ExitCode {
        match self {
            CliError::Usage(m) => {
                eprintln!("error: {m}");
                ExitCode::from(2)
            }
            CliError::NotFound(m) => {
                eprintln!("error: {m}");
                ExitCode::from(4)
            }
            CliError::Findings(n) => {
                eprintln!("error: verify reported {n} finding(s)");
                ExitCode::from(3)
            }
            CliError::Op(m) => {
                eprintln!("error: {m}");
                ExitCode::FAILURE
            }
        }
    }
}

fn op_err(e: impl std::fmt::Display) -> CliError {
    CliError::Op(e.to_string())
}

fn main() -> ExitCode {
    let args: Vec<String> = std::env::args().skip(1).collect();
    match run(&args) {
        Ok(()) => ExitCode::SUCCESS,
        Err(e) => e.report(),
    }
}

/// Parsed common flags plus the remaining positional arguments.
struct Parsed {
    db: String,
    positional: Vec<String>,
    limit: Option<usize>,
    from: Option<u64>,
    after: Option<u64>,
    prefix: Option<String>,
    queue: Option<String>,
    state: Option<String>,
    out: Option<String>,
    overwrite: bool,
    include_payloads: bool,
}

fn parse(args: &[String]) -> Result<Parsed, CliError> {
    let mut db = None;
    let mut positional = Vec::new();
    let mut limit = None;
    let mut from = None;
    let mut after = None;
    let mut prefix = None;
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
                limit = Some(
                    raw.parse()
                        .map_err(|_| CliError::Usage(format!("invalid --limit: {raw}")))?,
                );
            }
            "--from" => {
                let raw = flag_value(&mut iter, "--from")?;
                from = Some(
                    raw.parse()
                        .map_err(|_| CliError::Usage(format!("invalid --from: {raw}")))?,
                );
            }
            "--after" => {
                let raw = flag_value(&mut iter, "--after")?;
                after = Some(
                    raw.parse()
                        .map_err(|_| CliError::Usage(format!("invalid --after: {raw}")))?,
                );
            }
            "--prefix" => prefix = Some(flag_value(&mut iter, "--prefix")?),
            "--queue" => queue = Some(flag_value(&mut iter, "--queue")?),
            "--state" => state = Some(flag_value(&mut iter, "--state")?),
            "--out" => out = Some(flag_value(&mut iter, "--out")?),
            "--overwrite" => overwrite = true,
            "--include-payloads" => include_payloads = true,
            other if other.starts_with("--") => {
                return Err(CliError::Usage(format!("unknown flag: {other}")))
            }
            other => positional.push(other.to_string()),
        }
    }
    Ok(Parsed {
        db: db.ok_or_else(|| CliError::Usage("missing required --db <path>".into()))?,
        positional,
        limit,
        from,
        after,
        prefix,
        queue,
        state,
        out,
        overwrite,
        include_payloads,
    })
}

fn flag_value(iter: &mut std::slice::Iter<'_, String>, flag: &str) -> Result<String, CliError> {
    iter.next()
        .map(|s| s.to_string())
        .ok_or_else(|| CliError::Usage(format!("{flag} requires a value")))
}

fn parse_state(raw: &str) -> Result<JobState, CliError> {
    match raw.to_ascii_lowercase().as_str() {
        "pending" => Ok(JobState::Pending),
        "leased" => Ok(JobState::Leased),
        "retry-wait" | "retrywait" => Ok(JobState::RetryWait),
        "uncertain" => Ok(JobState::Uncertain),
        "succeeded" => Ok(JobState::Succeeded),
        "dead" => Ok(JobState::Dead),
        "cancelled" => Ok(JobState::Cancelled),
        other => Err(CliError::Usage(format!("unknown job state: {other}"))),
    }
}

fn parse_id(raw: &str, what: &str) -> Result<Id, CliError> {
    Id::from_hex(raw).map_err(|_| CliError::Usage(format!("invalid {what}: {raw}")))
}

fn parse_hex(raw: &str, what: &str) -> Result<Vec<u8>, CliError> {
    let bytes = raw.as_bytes();
    let mut out = Vec::with_capacity(bytes.len() / 2);
    for pair in bytes.chunks(2) {
        let (Some(hi), Some(lo)) = (
            pair.first().and_then(|b| (*b as char).to_digit(16)),
            pair.get(1).and_then(|b| (*b as char).to_digit(16)),
        ) else {
            return Err(CliError::Usage(format!("invalid {what} hex: {raw}")));
        };
        out.push(((hi as u8) << 4) | lo as u8);
    }
    Ok(out)
}

fn hex(bytes: &[u8]) -> String {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    let mut s = String::with_capacity(bytes.len() * 2);
    for &b in bytes {
        s.push(HEX[(b >> 4) as usize] as char);
        s.push(HEX[(b & 0xf) as usize] as char);
    }
    s
}

fn now_ms() -> i64 {
    use std::time::{SystemTime, UNIX_EPOCH};
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis() as i64)
        .unwrap_or(0)
}

fn print_job(job: &minisqlite::JobInfo) {
    println!("job_id: {}", job.job_id);
    println!("queue: {}", job.spec.queue());
    println!("partition_key: {}", job.spec.partition_key());
    println!("state: {:?}", job.state);
    println!("attempt: {} / {}", job.attempt, job.spec.max_attempts());
    println!("effect_mode: {:?}", job.spec.effect_mode());
    println!("payload_len: {}", job.spec.payload().len());
    println!("not_before_ms: {}", job.spec.not_before_ms());
    println!("worker_id: {:?}", job.worker_id);
    println!("lease_expires_at_ms: {:?}", job.lease_expires_at_ms);
    println!("retry_after_ms: {:?}", job.retry_after_ms);
    println!("terminal_at_ms: {:?}", job.terminal_at_ms);
    println!("error_summary: {:?}", job.error_summary);
}

fn run(args: &[String]) -> Result<(), CliError> {
    if args.is_empty() || args[0] == "--help" || args[0] == "help" {
        print!("{USAGE}");
        return Ok(());
    }
    // Accept an optional leading `store` namespace: `store doctor` == `doctor`.
    let args = if args[0] == "store" && args.len() > 1 {
        &args[1..]
    } else {
        args
    };
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
        | ("events", Some("stream"))
        | ("projections", Some("list"))
        | ("projections", Some("scan"))
        | ("projections", Some("get"))
        | ("jobs", Some("list"))
        | ("jobs", Some("show"))
        | ("jobs", Some("uncertain"))
        | ("jobs", Some("resolve"))
        | ("backup", Some(_))
        | ("diagnostic-export", None)
        | ("migrations", Some("status")) => {}
        _ => {
            return Err(CliError::Usage(format!(
                "unknown command: {}\n{USAGE}",
                args.join(" ")
            )))
        }
    }

    // `jobs resolve` is the only mutating command: it needs a writable store.
    if command == ("jobs", Some("resolve")) {
        return jobs_resolve(&parsed);
    }

    // All other commands are inspection commands: open the existing database
    // without creating it and without migrating it.
    if !std::path::Path::new(&parsed.db).exists() {
        return Err(CliError::NotFound(format!(
            "database file {} does not exist",
            parsed.db
        )));
    }
    let store = ControlPlaneStore::open_existing(&parsed.db).map_err(op_err)?;

    match command {
        ("doctor", None) => {
            println!("store opened: {}", parsed.db);
            let stats = store.stats().map_err(op_err)?;
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
            let report = store.verify().map_err(op_err)?;
            if report.is_ok() {
                println!("verify: ok");
                Ok(())
            } else {
                for finding in &report.findings {
                    println!("{}: {}", finding.check, finding.detail);
                }
                Err(CliError::Findings(report.findings.len()))
            }
        }
        ("verify", None) => {
            let report = store.verify().map_err(op_err)?;
            if report.is_ok() {
                println!("ok");
                Ok(())
            } else {
                for finding in &report.findings {
                    println!("{}: {}", finding.check, finding.detail);
                }
                Err(CliError::Findings(report.findings.len()))
            }
        }
        ("stats", None) => {
            let stats = store.stats().map_err(op_err)?;
            println!("{stats:#?}");
            Ok(())
        }
        ("events", Some("tail")) => {
            let limit = parsed.limit.unwrap_or(10);
            for event in store.last_events(limit).map_err(op_err)? {
                print_event(&event);
            }
            Ok(())
        }
        ("events", Some("stream")) => {
            let stream_id = parsed
                .positional
                .get(1)
                .ok_or_else(|| CliError::Usage("events stream requires a <stream-id>".into()))?;
            let from = parsed.from.unwrap_or(1);
            let limit = parsed.limit.unwrap_or(100);
            let events = store
                .stream_events(stream_id, from, limit)
                .map_err(op_err)?;
            if events.is_empty() && store.stream_version(stream_id).map_err(op_err)? == 0 {
                return Err(CliError::NotFound(format!("stream not found: {stream_id}")));
            }
            for event in events {
                print_event(&event);
            }
            Ok(())
        }
        ("projections", Some("list")) => {
            for (projection, version) in store.projections_list().map_err(op_err)? {
                println!("{projection} v{version}");
            }
            Ok(())
        }
        ("projections", Some("scan")) => {
            let projection = parsed.positional.get(1).ok_or_else(|| {
                CliError::Usage("projections scan requires a <projection>".into())
            })?;
            if store.projection_version(projection).map_err(op_err)? == 0 {
                return Err(CliError::NotFound(format!(
                    "projection not found: {projection}"
                )));
            }
            let prefix = match &parsed.prefix {
                Some(raw) => parse_hex(raw, "--prefix")?,
                None => Vec::new(),
            };
            let limit = parsed.limit.unwrap_or(100);
            for entry in store
                .projection_scan_prefix(projection, &prefix, limit)
                .map_err(op_err)?
            {
                println!("{} {}", hex(&entry.key), hex(&entry.value));
            }
            Ok(())
        }
        ("projections", Some("get")) => {
            let projection = parsed
                .positional
                .get(1)
                .ok_or_else(|| CliError::Usage("projections get requires a <projection>".into()))?;
            let key_raw = parsed
                .positional
                .get(2)
                .ok_or_else(|| CliError::Usage("projections get requires a <key-hex>".into()))?;
            let key = parse_hex(key_raw, "key")?;
            match store.projection_get(projection, &key).map_err(op_err)? {
                Some(value) => {
                    println!("{}", hex(&value));
                    Ok(())
                }
                None => Err(CliError::NotFound(format!(
                    "no entry for key {key_raw} in projection {projection}"
                ))),
            }
        }
        ("jobs", Some("list")) => {
            let state = parsed.state.as_deref().map(parse_state).transpose()?;
            let limit = parsed.limit.unwrap_or(100);
            let after = parsed.after.unwrap_or(0);
            let (jobs, cursor) = store
                .jobs_page(parsed.queue.as_deref(), state, after, limit)
                .map_err(op_err)?;
            let page_full = jobs.len() == limit;
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
            if page_full {
                println!("next: --after {cursor}");
            }
            Ok(())
        }
        ("jobs", Some("show")) => {
            let raw = parsed
                .positional
                .get(1)
                .ok_or_else(|| CliError::Usage("jobs show requires a <job-id>".into()))?;
            let job_id = parse_id(raw, "job id")?;
            match store.job(job_id).map_err(op_err)? {
                Some(job) => {
                    print_job(&job);
                    Ok(())
                }
                None => Err(CliError::NotFound(format!("job not found: {raw}"))),
            }
        }
        ("jobs", Some("uncertain")) => {
            let limit = parsed.limit.unwrap_or(100);
            for job in store
                .jobs(None, Some(JobState::Uncertain), limit)
                .map_err(op_err)?
            {
                println!(
                    "{} {} {} attempt {}",
                    job.job_id,
                    job.spec.queue(),
                    job.spec.partition_key(),
                    job.attempt
                );
            }
            Ok(())
        }
        ("backup", Some(dest)) => {
            store.backup(dest, parsed.overwrite).map_err(op_err)?;
            let version = store
                .migrations_status()
                .map_err(op_err)?
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
                .map_err(op_err)?;
            match parsed.out {
                Some(path) => {
                    std::fs::write(&path, export).map_err(op_err)?;
                    println!("diagnostic export written to {path}");
                }
                None => print!("{export}"),
            }
            Ok(())
        }
        ("migrations", Some("status")) => {
            for status in store.migrations_status().map_err(op_err)? {
                println!(
                    "v{} applied_at_ms={} checksum={}",
                    status.version,
                    status.applied_at_ms,
                    if status.checksum_ok { "ok" } else { "MISMATCH" }
                );
            }
            Ok(())
        }
        _ => Err(CliError::Usage(format!(
            "unknown command: {}\n{USAGE}",
            args.join(" ")
        ))),
    }
}

fn print_event(event: &minisqlite::PersistedEvent) {
    println!(
        "{} {} {}@{} {}",
        event.global_sequence,
        event.event.event_id,
        event.event.stream_id,
        event.stream_version,
        event.event.event_type
    );
}

fn jobs_resolve(parsed: &Parsed) -> Result<(), CliError> {
    let raw_id = parsed
        .positional
        .get(1)
        .ok_or_else(|| CliError::Usage("jobs resolve requires a <job-id>".into()))?;
    let job_id = parse_id(raw_id, "job id")?;
    let resolution = match parsed.positional.get(2).map(String::as_str) {
        Some("retry") => Resolution::Retry,
        Some("succeeded") => Resolution::MarkSucceeded,
        Some("dead") => Resolution::MarkDead,
        _ => {
            return Err(CliError::Usage(
                "jobs resolve requires a resolution: retry | succeeded | dead".into(),
            ))
        }
    };
    if !std::path::Path::new(&parsed.db).exists() {
        return Err(CliError::NotFound(format!(
            "database file {} does not exist",
            parsed.db
        )));
    }
    let store = ControlPlaneStore::open(&parsed.db).map_err(op_err)?;
    if store.job(job_id).map_err(op_err)?.is_none() {
        return Err(CliError::NotFound(format!("job not found: {raw_id}")));
    }
    let transaction_id = Id::new().map_err(op_err)?;
    store
        .commit(
            &CommitBatch::new(transaction_id, now_ms()).resolve_uncertain_job(job_id, resolution),
        )
        .map_err(op_err)?;
    println!("job {raw_id} resolved");
    Ok(())
}
