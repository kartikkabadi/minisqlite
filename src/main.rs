use std::env;
use std::io::{self, Write};
use std::process;
use std::time::{SystemTime, UNIX_EPOCH};

use minisqlite::{Durability, Error, JobState, StoreBuilder};

const HELP: &str = "MiniSQLite control-plane state engine

Usage:
  minisqlite [OPTIONS] <command> [command-args]
  minisqlite [OPTIONS] <path> <command> [command-args]

Commands:
  doctor <database>                 Open the store, verify it, and print diagnostics.
  verify <database>               Verify the file and exit with status 0 if intact.
  stats <database>                Print store statistics.
  events tail <database> [LIMIT]  Print the last events.
  events stream <database> <stream-id> [LIMIT]
                                  Print events for a single stream.
  projections list <database>     List projections and versions.
  projections scan <database> <projection> [--prefix <prefix>]
                                  Scan a projection for keys with a prefix.
  jobs list <database> [--queue <queue>] [--state <state>]
                                  List jobs, optionally filtered.
  export <database> [--format jsonl]
                                  Dump a JSONL snapshot.
  backup <database> <destination>   Atomically copy the primary file.

Options:
  -j, --json                       Emit machine-readable JSON output.
  -p, --show-payloads              Show full payloads/values in events, jobs, and projections.
  -d, --durability strict|memory   Durability mode (default: strict).
  -l, --lock <PATH>                Custom lock-file path.
  -h, --help                       Print this help.
";

struct GlobalOpts {
    json: bool,
    show_payloads: bool,
    durability: Durability,
    lock_path: Option<String>,
}

struct CommandOpts {
    limit: Option<usize>,
    prefix: Option<String>,
    queue: Option<String>,
    state: Option<JobState>,
    format: Option<String>,
}

enum Command {
    Doctor {
        path: String,
    },
    Verify {
        path: String,
    },
    Stats {
        path: String,
    },
    EventsTail {
        path: String,
        limit: usize,
    },
    EventsStream {
        path: String,
        stream_id: String,
        limit: usize,
    },
    ProjectionsList {
        path: String,
    },
    ProjectionsScan {
        path: String,
        name: String,
        prefix: Option<String>,
    },
    JobsList {
        path: String,
        queue: Option<String>,
        state: Option<JobState>,
    },
    Export {
        path: String,
    },
    Backup {
        path: String,
        dest: String,
    },
}

fn main() {
    if let Err(e) = run() {
        eprintln!("{e}");
        process::exit(exit_code(&e));
    }
}

fn exit_code(error: &Error) -> i32 {
    match error {
        Error::Usage(_) => 2,
        Error::NotMiniSQLite => 3,
        Error::UnsupportedVersion { .. } => 4,
        Error::Corruption { .. } => 5,
        Error::AlreadyOpen | Error::LockUnavailable => 6,
        Error::StorePoisoned { .. } => 7,
        Error::Conflict { .. } => 8,
        Error::DuplicateIdWithDifferentContent { .. } => 9,
        Error::DuplicateEventId(_) => 10,
        Error::DuplicateJobId(_) => 11,
        Error::InvalidLease { .. } => 12,
        Error::JobNotFound(_) => 13,
        Error::ProjectionVersionMismatch { .. } => 14,
        Error::PayloadTooLarge { .. } => 15,
        Error::CommitOutcomeUncertain { .. } => 16,
        Error::ProjectionNotFound(_) => 17,
        Error::StreamNotFound(_) => 18,
        Error::EventNotFound(_) => 19,
        Error::TransactionNotFound(_) => 20,
        Error::Validation(_) => 21,
        Error::Io(_) => 22,
    }
}

fn is_top_level_command(s: &str) -> bool {
    matches!(
        s,
        "doctor" | "verify" | "stats" | "events" | "projections" | "jobs" | "export" | "backup"
    )
}

fn is_events_subcommand(s: &str) -> bool {
    matches!(s, "tail" | "stream")
}

fn is_projections_subcommand(s: &str) -> bool {
    matches!(s, "list" | "scan")
}

fn parse_args() -> Result<(GlobalOpts, Command), Error> {
    let mut args = env::args().skip(1);
    let mut global = GlobalOpts {
        json: false,
        show_payloads: false,
        durability: Durability::Strict,
        lock_path: None,
    };
    let mut cmd_opts = CommandOpts {
        limit: None,
        prefix: None,
        queue: None,
        state: None,
        format: None,
    };
    let mut positionals: Vec<String> = Vec::new();

    while let Some(arg) = args.next() {
        if arg.starts_with('-') {
            match arg.as_str() {
                "-h" | "--help" => {
                    print!("{}", HELP);
                    process::exit(0);
                }
                "-j" | "--json" => global.json = true,
                "-p" | "--show-payloads" => global.show_payloads = true,
                "-d" | "--durability" => {
                    let value = args
                        .next()
                        .ok_or_else(|| Error::Usage("missing durability value".into()))?;
                    global.durability = parse_durability(&value)?;
                }
                "-l" | "--lock" => {
                    let value = args
                        .next()
                        .ok_or_else(|| Error::Usage("missing lock path".into()))?;
                    global.lock_path = Some(value);
                }
                "--limit" => {
                    let value = args
                        .next()
                        .ok_or_else(|| Error::Usage("missing --limit value".into()))?;
                    let n = value
                        .parse()
                        .map_err(|_| Error::Usage(format!("invalid limit: {value}")))?;
                    cmd_opts.limit = Some(n);
                }
                "--prefix" => {
                    let value = args
                        .next()
                        .ok_or_else(|| Error::Usage("missing --prefix value".into()))?;
                    cmd_opts.prefix = Some(value);
                }
                "--queue" => {
                    let value = args
                        .next()
                        .ok_or_else(|| Error::Usage("missing --queue value".into()))?;
                    cmd_opts.queue = Some(value);
                }
                "--state" => {
                    let value = args
                        .next()
                        .ok_or_else(|| Error::Usage("missing --state value".into()))?;
                    cmd_opts.state = Some(parse_job_state(&value)?);
                }
                "--format" => {
                    let value = args
                        .next()
                        .ok_or_else(|| Error::Usage("missing --format value".into()))?;
                    cmd_opts.format = Some(value);
                }
                _ => return Err(Error::Usage(format!("unknown option: {arg}"))),
            }
        } else {
            positionals.push(arg);
        }
    }

    if positionals.is_empty() {
        return Err(Error::Usage("missing command".into()));
    }

    // Determine command tokens, database path, and any remaining positional args.
    let (cmd_tokens, path, args_after_path) = if is_top_level_command(&positionals[0]) {
        parse_command_first(&positionals)?
    } else {
        parse_path_first(&positionals)?
    };

    let command = match (
        cmd_tokens[0].as_str(),
        cmd_tokens.get(1).map(|s| s.as_str()),
    ) {
        ("doctor", None) => Command::Doctor { path },
        ("verify", None) => Command::Verify { path },
        ("stats", None) => Command::Stats { path },
        ("events", Some("tail")) => {
            let limit = cmd_opts
                .limit
                .or_else(|| args_after_path.first().and_then(|s| s.parse().ok()))
                .unwrap_or(50);
            Command::EventsTail { path, limit }
        }
        ("events", Some("stream")) => {
            if args_after_path.is_empty() {
                return Err(Error::Usage("missing stream-id".into()));
            }
            let stream_id = args_after_path[0].clone();
            let limit = cmd_opts
                .limit
                .or_else(|| args_after_path.get(1).and_then(|s| s.parse().ok()))
                .unwrap_or(100);
            Command::EventsStream {
                path,
                stream_id,
                limit,
            }
        }
        ("projections", Some("list")) => Command::ProjectionsList { path },
        ("projections", Some("scan")) => {
            if args_after_path.is_empty() {
                return Err(Error::Usage(
                    "projections scan requires <projection>".into(),
                ));
            }
            let name = args_after_path[0].clone();
            let prefix = cmd_opts.prefix.or_else(|| args_after_path.get(1).cloned());
            Command::ProjectionsScan { path, name, prefix }
        }
        ("jobs", Some("list")) => Command::JobsList {
            path,
            queue: cmd_opts.queue,
            state: cmd_opts.state,
        },
        ("export", None) => {
            let format = cmd_opts.format.unwrap_or_else(|| "jsonl".into());
            if format != "jsonl" {
                return Err(Error::Usage(format!("unsupported export format: {format}")));
            }
            Command::Export { path }
        }
        ("backup", None) => {
            if args_after_path.is_empty() {
                return Err(Error::Usage("backup requires <destination>".into()));
            }
            Command::Backup {
                path,
                dest: args_after_path[0].clone(),
            }
        }
        _ => {
            return Err(Error::Usage(format!(
                "unknown command: {}",
                cmd_tokens.join(" ")
            )))
        }
    };

    Ok((global, command))
}

fn parse_command_first(
    positionals: &[String],
) -> Result<(Vec<String>, String, Vec<String>), Error> {
    let mut cmd_tokens = vec![positionals[0].clone()];

    let i = match positionals[0].as_str() {
        "events" => {
            if positionals.len() < 2 || !is_events_subcommand(&positionals[1]) {
                return Err(Error::Usage("expected events tail|stream".into()));
            }
            cmd_tokens.push(positionals[1].clone());
            2
        }
        "projections" => {
            if positionals.len() < 2 || !is_projections_subcommand(&positionals[1]) {
                return Err(Error::Usage("expected projections list|scan".into()));
            }
            cmd_tokens.push(positionals[1].clone());
            2
        }
        "jobs" => {
            if positionals.len() < 2 || positionals[1] != "list" {
                return Err(Error::Usage("expected jobs list".into()));
            }
            cmd_tokens.push(positionals[1].clone());
            2
        }
        _ => 1,
    };

    if i >= positionals.len() {
        return Err(Error::Usage("missing database path".into()));
    }
    let path = positionals[i].clone();
    let args = positionals[i + 1..].to_vec();
    Ok((cmd_tokens, path, args))
}

fn parse_path_first(positionals: &[String]) -> Result<(Vec<String>, String, Vec<String>), Error> {
    let path = positionals[0].clone();
    if positionals.len() < 2 || !is_top_level_command(&positionals[1]) {
        return Err(Error::Usage("missing command".into()));
    }

    let mut cmd_tokens = vec![positionals[1].clone()];

    let i = match positionals[1].as_str() {
        "events" => {
            if positionals.len() < 3 || !is_events_subcommand(&positionals[2]) {
                return Err(Error::Usage("expected events tail|stream".into()));
            }
            cmd_tokens.push(positionals[2].clone());
            3
        }
        "projections" => {
            if positionals.len() < 3 || !is_projections_subcommand(&positionals[2]) {
                return Err(Error::Usage("expected projections list|scan".into()));
            }
            cmd_tokens.push(positionals[2].clone());
            3
        }
        "jobs" => {
            if positionals.len() < 3 || positionals[2] != "list" {
                return Err(Error::Usage("expected jobs list".into()));
            }
            cmd_tokens.push(positionals[2].clone());
            3
        }
        _ => 2,
    };

    let args = positionals[i..].to_vec();
    Ok((cmd_tokens, path, args))
}

fn run() -> Result<(), Error> {
    let (opts, cmd) = parse_args()?;
    match cmd {
        Command::Doctor { path } => {
            doctor(&path, opts.durability, opts.lock_path.as_deref(), opts.json)
        }
        Command::Verify { path } => {
            verify(&path, opts.durability, opts.lock_path.as_deref(), opts.json)
        }
        Command::Stats { path } => {
            stats(&path, opts.durability, opts.lock_path.as_deref(), opts.json)
        }
        Command::EventsTail { path, limit } => events_tail(
            &path,
            opts.durability,
            opts.lock_path.as_deref(),
            opts.json,
            opts.show_payloads,
            limit,
        ),
        Command::EventsStream {
            path,
            stream_id,
            limit,
        } => events_stream(
            &path,
            opts.durability,
            opts.lock_path.as_deref(),
            opts.json,
            opts.show_payloads,
            &stream_id,
            limit,
        ),
        Command::ProjectionsList { path } => {
            projections_list(&path, opts.durability, opts.lock_path.as_deref(), opts.json)
        }
        Command::ProjectionsScan { path, name, prefix } => projections_scan(
            &path,
            opts.durability,
            opts.lock_path.as_deref(),
            opts.json,
            opts.show_payloads,
            &name,
            prefix.as_deref(),
        ),
        Command::JobsList { path, queue, state } => jobs_list(
            &path,
            opts.durability,
            opts.lock_path.as_deref(),
            opts.json,
            opts.show_payloads,
            queue.as_deref(),
            state,
        ),
        Command::Export { path } => export(&path, opts.durability, opts.lock_path.as_deref()),
        Command::Backup { path, dest } => backup(
            &path,
            opts.durability,
            opts.lock_path.as_deref(),
            opts.json,
            &dest,
        ),
    }
}

fn open_store(
    path: &str,
    durability: Durability,
    lock_path: Option<&str>,
) -> Result<minisqlite::Store, Error> {
    let mut builder = StoreBuilder::new(path).durability(durability);
    if let Some(lock) = lock_path {
        builder = builder.lock_path(lock);
    }
    builder.open()
}

fn parse_durability(s: &str) -> Result<Durability, Error> {
    match s.to_lowercase().as_str() {
        "strict" => Ok(Durability::Strict),
        "memory" => Ok(Durability::Memory),
        _ => Err(Error::Usage(format!("invalid durability mode: {s}"))),
    }
}

fn doctor(
    path: &str,
    durability: Durability,
    lock_path: Option<&str>,
    json: bool,
) -> Result<(), Error> {
    match open_store(path, durability, lock_path) {
        Ok(store) => {
            store.verify()?;
            let stats = store.stats();
            if json {
                let status = if stats.poisoned {
                    "poisoned"
                } else if stats.recovered_tail {
                    "ok_tail_truncated"
                } else {
                    "ok"
                };
                println!(
                    "{}",
                    serde_json::json!({
                        "path": store.path().to_string_lossy(),
                        "status": status,
                        "locked": true,
                        "stats": stats,
                    })
                );
                return Ok(());
            }
            println!("path:          {}", store.path().display());
            println!(
                "format:        {}.{}",
                stats.format_version_major, stats.format_version_minor
            );
            println!("file_size:     {}", stats.file_size);
            println!("transactions:  {}", stats.transaction_count);
            println!("events:        {}", stats.event_count);
            println!("streams:       {}", stats.stream_count);
            println!("projections:   {}", stats.projection_count);
            println!("jobs:          {}", stats.job_count);
            println!("last_tx_seq:   {}", stats.last_transaction_sequence);
            println!("last_event_seq:{}", stats.last_event_sequence);
            for (state, count) in &stats.job_counts {
                println!("jobs.{:?}        {}", state, count);
            }
            if stats.poisoned {
                println!("status:        POISONED");
            } else if stats.recovered_tail {
                println!("status:        OK (tail truncated on recovery)");
            } else {
                println!("status:        OK");
            }
            println!("locked:        true (this process owns the store)");
            Ok(())
        }
        Err(Error::AlreadyOpen) | Err(Error::LockUnavailable) => {
            if json {
                println!(
                    "{}",
                    serde_json::json!({
                        "path": path,
                        "status": "locked",
                        "locked": true,
                    })
                );
            } else {
                println!("path:   {path}");
                println!("status: LOCKED (another process owns the store)");
            }
            Ok(())
        }
        Err(e) => Err(e),
    }
}

fn verify(
    path: &str,
    durability: Durability,
    lock_path: Option<&str>,
    json: bool,
) -> Result<(), Error> {
    let store = open_store(path, durability, lock_path)?;
    store.verify()?;
    if json {
        println!("{}", serde_json::json!({ "ok": true }));
    } else {
        println!("ok");
    }
    Ok(())
}

fn stats(
    path: &str,
    durability: Durability,
    lock_path: Option<&str>,
    json: bool,
) -> Result<(), Error> {
    let store = open_store(path, durability, lock_path)?;
    let stats = store.stats();
    if json {
        println!("{}", serde_json::to_string(&stats).unwrap());
        return Ok(());
    }
    println!("file_size {}", stats.file_size);
    println!("transaction_count {}", stats.transaction_count);
    println!("event_count {}", stats.event_count);
    println!("stream_count {}", stats.stream_count);
    println!("projection_count {}", stats.projection_count);
    println!("job_count {}", stats.job_count);
    println!(
        "last_transaction_sequence {}",
        stats.last_transaction_sequence
    );
    println!("last_event_sequence {}", stats.last_event_sequence);
    for (state, count) in &stats.job_counts {
        println!("jobs.{:?} {}", state, count);
    }
    if stats.recovered_tail {
        println!("recovered_tail true");
    }
    if stats.poisoned {
        println!("poisoned true");
    }
    Ok(())
}

fn events_tail(
    path: &str,
    durability: Durability,
    lock_path: Option<&str>,
    json: bool,
    show_payloads: bool,
    limit: usize,
) -> Result<(), Error> {
    let store = open_store(path, durability, lock_path)?;
    let stdout = io::stdout();
    let mut stdout = stdout.lock();
    let start = store.high_water_sequence().saturating_sub(limit as u64);
    for e in store.events_after(start, limit) {
        if json {
            writeln!(&mut stdout, "{}", event_json(&e, show_payloads))?;
        } else {
            writeln_event(&mut stdout, &e, show_payloads)?;
        }
    }
    Ok(())
}

fn events_stream(
    path: &str,
    durability: Durability,
    lock_path: Option<&str>,
    json: bool,
    show_payloads: bool,
    stream_id: &str,
    limit: usize,
) -> Result<(), Error> {
    let store = open_store(path, durability, lock_path)?;
    let stdout = io::stdout();
    let mut stdout = stdout.lock();
    let start = store
        .stream_version(stream_id)
        .unwrap_or(0)
        .saturating_sub(limit as u64);
    for e in store.stream_events(stream_id, start, limit) {
        if json {
            writeln!(&mut stdout, "{}", event_json(&e, show_payloads))?;
        } else {
            writeln_event(&mut stdout, &e, show_payloads)?;
        }
    }
    Ok(())
}

fn projections_list(
    path: &str,
    durability: Durability,
    lock_path: Option<&str>,
    json: bool,
) -> Result<(), Error> {
    let store = open_store(path, durability, lock_path)?;
    let stdout = io::stdout();
    let mut stdout = stdout.lock();
    for name in store.projection_names() {
        let version = store.projection_version(&name)?;
        if json {
            writeln!(
                &mut stdout,
                "{}",
                serde_json::json!({"projection": name, "version": version})
            )?;
        } else {
            writeln!(stdout, "{} {}", name, version)?;
        }
    }
    Ok(())
}

fn projections_scan(
    path: &str,
    durability: Durability,
    lock_path: Option<&str>,
    json: bool,
    show_payloads: bool,
    name: &str,
    prefix: Option<&str>,
) -> Result<(), Error> {
    let store = open_store(path, durability, lock_path)?;
    let prefix_bytes = prefix.map(|p| p.as_bytes()).unwrap_or_default();
    let entries = store.scan_projection_prefix(name, prefix_bytes)?;
    let stdout = io::stdout();
    let mut stdout = stdout.lock();
    for entry in entries {
        if json {
            let value = if show_payloads {
                serde_json::Value::String(hex(&entry.value))
            } else {
                serde_json::Value::Null
            };
            writeln!(
                &mut stdout,
                "{}",
                serde_json::json!({
                    "projection": name,
                    "key": String::from_utf8_lossy(&entry.key),
                    "value": value,
                })
            )?;
        } else {
            writeln!(
                stdout,
                "{} {} {}",
                name,
                bytes_repr(&entry.key, show_payloads),
                bytes_repr(&entry.value, show_payloads)
            )?;
        }
    }
    Ok(())
}

fn jobs_list(
    path: &str,
    durability: Durability,
    lock_path: Option<&str>,
    json: bool,
    show_payloads: bool,
    queue: Option<&str>,
    state: Option<JobState>,
) -> Result<(), Error> {
    let store = open_store(path, durability, lock_path)?;
    let now_ms = current_time_ms();
    let queue = queue.map(|s| s.to_string());
    let records = store.jobs(now_ms, queue, state);
    for info in records {
        let payload = if show_payloads {
            serde_json::Value::String(hex(&info.spec.payload))
        } else {
            serde_json::Value::Null
        };
        if json {
            println!(
                "{}",
                serde_json::json!({
                    "job_id": info.job_id.to_hex(),
                    "state": job_state_str(info.state),
                    "queue": info.spec.queue,
                    "partition": info.spec.partition,
                    "payload": payload,
                    "attempt": info.attempt,
                    "max_attempts": info.spec.max_attempts,
                    "effect_mode": effect_mode_str(info.spec.effect_mode),
                    "not_before_ms": info.spec.not_before_ms,
                    "idempotency_key": info.spec.idempotency_key,
                    "worker_id": info.worker_id,
                    "lease_expires_at_ms": info.lease_expires_at_ms,
                    "retry_after_ms": info.retry_after_ms,
                    "terminal_at_ms": info.terminal_at_ms,
                })
            );
        } else {
            println!(
                "{} {:?} {} {} {} {} {} {:?} {:?}",
                info.job_id,
                info.state,
                info.spec.queue,
                info.spec.partition,
                info.attempt,
                info.worker_id.as_deref().unwrap_or("-"),
                info.lease_expires_at_ms
                    .map_or_else(|| "-".to_string(), |v| v.to_string()),
                info.retry_after_ms,
                info.terminal_at_ms,
            );
            if show_payloads {
                println!("    {}", bytes_repr(&info.spec.payload, true));
            }
        }
    }
    Ok(())
}

fn export(path: &str, durability: Durability, lock_path: Option<&str>) -> Result<(), Error> {
    let store = open_store(path, durability, lock_path)?;
    let now_ms = current_time_ms();
    let stdout = io::stdout();
    let mut out = stdout.lock();

    for e in store.events_after(0, usize::MAX) {
        writeln!(
            out,
            r#"{{"type":"event","global_sequence":{},"stream_version":{},"event_id":"{}","stream_id":"{}","event_type":"{}","schema_version":{},"occurred_at_ms":{},"causation_id":"{}","correlation_id":"{}","payload":"{}","metadata":"{}"}}"#,
            e.global_sequence,
            e.stream_version,
            e.event.event_id,
            json_escape(&e.event.stream_id),
            json_escape(&e.event.event_type),
            e.event.schema_version,
            e.event.occurred_at_ms,
            e.event.causation_id.unwrap_or(minisqlite::Id::ZERO),
            e.event.correlation_id.unwrap_or(minisqlite::Id::ZERO),
            hex(&e.event.payload),
            hex(&e.event.metadata)
        )?;
    }

    for name in store.projection_names() {
        let version = store.projection_version(&name)?;
        let entries = store.scan_projection_prefix(&name, b"")?;
        for entry in entries {
            writeln!(
                out,
                r#"{{"type":"projection","projection":"{}","version":{},"key":"{}","value":"{}"}}"#,
                json_escape(&name),
                version,
                hex(&entry.key),
                hex(&entry.value)
            )?;
        }
    }

    for info in store.jobs(now_ms, None, None) {
        writeln!(
            out,
            r#"{{"type":"job","job_id":"{}","state":"{:?}","queue":"{}","partition":"{}","payload":"{}","attempt":{},"max_attempts":{},"effect_mode":"{:?}","worker_id":"{}","lease_expires_at_ms":{},"retry_after_ms":{},"terminal_at_ms":{}}}"#,
            info.job_id,
            info.state,
            json_escape(&info.spec.queue),
            json_escape(&info.spec.partition),
            hex(&info.spec.payload),
            info.attempt,
            info.spec.max_attempts,
            info.spec.effect_mode,
            info.worker_id.as_deref().unwrap_or(""),
            info.lease_expires_at_ms
                .map_or_else(|| "null".to_string(), |v| v.to_string()),
            info.retry_after_ms
                .map_or_else(|| "null".to_string(), |v| v.to_string()),
            info.terminal_at_ms
                .map_or_else(|| "null".to_string(), |v| v.to_string()),
        )?;
    }

    Ok(())
}

fn backup(
    path: &str,
    durability: Durability,
    lock_path: Option<&str>,
    json: bool,
    dest: &str,
) -> Result<(), Error> {
    let store = open_store(path, durability, lock_path)?;
    store.backup(dest)?;
    if json {
        println!("{}", serde_json::json!({ "destination": dest }));
    } else {
        println!("backup written to {dest}");
    }
    Ok(())
}

fn parse_job_state(s: &str) -> Result<JobState, Error> {
    match s.to_lowercase().as_str() {
        "pending" => Ok(JobState::Pending),
        "leased" => Ok(JobState::Leased),
        "retrywait" | "retry-wait" => Ok(JobState::RetryWait),
        "succeeded" => Ok(JobState::Succeeded),
        "dead" => Ok(JobState::Dead),
        "cancelled" => Ok(JobState::Cancelled),
        "uncertain" => Ok(JobState::Uncertain),
        _ => Err(Error::Usage(format!("invalid job state: {s}"))),
    }
}

fn job_state_str(state: JobState) -> &'static str {
    match state {
        JobState::Pending => "pending",
        JobState::Leased => "leased",
        JobState::RetryWait => "retry_wait",
        JobState::Succeeded => "succeeded",
        JobState::Dead => "dead",
        JobState::Cancelled => "cancelled",
        JobState::Uncertain => "uncertain",
    }
}

fn effect_mode_str(mode: minisqlite::EffectMode) -> &'static str {
    match mode {
        minisqlite::EffectMode::Idempotent => "idempotent",
        minisqlite::EffectMode::UncertainOnLeaseExpiry => "uncertain_on_lease_expiry",
    }
}

fn writeln_event<W: Write>(
    out: &mut W,
    e: &minisqlite::PersistedEvent,
    show_payloads: bool,
) -> Result<(), Error> {
    writeln!(
        out,
        "{} {} {} {} {} {} {}",
        e.global_sequence,
        e.stream_version,
        e.event.event_type,
        e.event.stream_id,
        e.event.schema_version,
        e.event.occurred_at_ms,
        bytes_repr(&e.event.payload, show_payloads)
    )?;
    Ok(())
}

fn event_json(e: &minisqlite::PersistedEvent, show_payloads: bool) -> String {
    let payload = if show_payloads {
        serde_json::Value::String(hex(&e.event.payload))
    } else {
        serde_json::Value::Null
    };
    let metadata = if show_payloads {
        serde_json::Value::String(hex(&e.event.metadata))
    } else {
        serde_json::Value::Null
    };
    serde_json::json!({
        "global_sequence": e.global_sequence,
        "stream_version": e.stream_version,
        "transaction_id": e.transaction_id.to_hex(),
        "event": {
            "event_id": e.event.event_id.to_hex(),
            "stream_id": e.event.stream_id,
            "event_type": e.event.event_type,
            "schema_version": e.event.schema_version,
            "occurred_at_ms": e.event.occurred_at_ms,
            "causation_id": e.event.causation_id.map(|id| id.to_hex()),
            "correlation_id": e.event.correlation_id.map(|id| id.to_hex()),
            "payload": payload,
            "metadata": metadata,
        }
    })
    .to_string()
}

fn bytes_repr(v: &[u8], show_payloads: bool) -> String {
    if !show_payloads {
        return format!("<{} bytes>", v.len());
    }
    match std::str::from_utf8(v) {
        Ok(s) if s.chars().all(|c| !c.is_control()) => s.to_string(),
        _ => hex(v),
    }
}

fn hex(v: &[u8]) -> String {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    let mut out = String::with_capacity(v.len() * 2);
    for b in v {
        out.push(HEX[(b >> 4) as usize] as char);
        out.push(HEX[(b & 0xf) as usize] as char);
    }
    out
}

fn json_escape(s: &str) -> String {
    let mut out = String::with_capacity(s.len());
    for c in s.chars() {
        match c {
            '\\' => out.push_str("\\\\"),
            '"' => out.push_str("\\\""),
            '\n' => out.push_str("\\n"),
            '\r' => out.push_str("\\r"),
            '\t' => out.push_str("\\t"),
            c if (c as u32) < 0x20 => out.push_str(&format!("\\u{:04x}", c as u32)),
            c => out.push(c),
        }
    }
    out
}

fn current_time_ms() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis() as i64)
        .unwrap_or(0)
}
