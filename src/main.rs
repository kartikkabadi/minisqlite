use std::io::{self, Write};
use std::process;
use std::time::{SystemTime, UNIX_EPOCH};

use lexopt::prelude::*;

use minisqlite::{Durability, Error, JobState, StoreBuilder};

const HELP: &str = "MiniSQLite control-plane state engine

Usage:
  minisqlite [OPTIONS] <path> <command> [ARGS]

Commands:
  doctor                 Open the store, verify it, and print diagnostics.
  verify                 Verify the file and exit with status 0 if intact.
  stats                  Print store statistics.
  events tail [LIMIT]    Print the last events.
  events stream <ID> [LIMIT]
                         Print events for a single stream.
  projections list       List projections and versions.
  projections get <NAME> <KEY>
                         Read a single projection key.
  projections scan <NAME> [PREFIX]
                         Scan a projection for keys with a prefix.
  jobs list [QUEUE] [--state <STATE>]
                         List jobs, optionally filtered by queue and state.
  export                 Dump a JSONL snapshot of events, projections and jobs.
  backup <DEST>          Atomically copy the primary file.

Options:
  -j, --json                       Emit machine-readable JSON output.
  -d, --durability strict|memory   Durability mode (default: strict).
  -l, --lock <PATH>                Custom lock-file path.
  -h, --help                       Print this help.
";

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

fn run() -> Result<(), Error> {
    let mut parser = lexopt::Parser::from_env();
    let mut path: Option<String> = None;
    let mut cmd: Option<String> = None;
    let mut durability = Durability::Strict;
    let mut lock_path: Option<String> = None;
    let mut json = false;

    while let Some(arg) = parser.next().map_err(|e| Error::Usage(e.to_string()))? {
        match arg {
            Short('h') | Long("help") => {
                print!("{}", HELP);
                return Ok(());
            }
            Short('j') | Long("json") => json = true,
            Short('d') | Long("durability") => {
                let value = parser.value().map_err(|e| Error::Usage(e.to_string()))?;
                durability =
                    parse_durability(&value.string().map_err(|e| Error::Usage(e.to_string()))?)?;
            }
            Short('l') | Long("lock") => {
                let value = parser.value().map_err(|e| Error::Usage(e.to_string()))?;
                lock_path = Some(value.string().map_err(|e| Error::Usage(e.to_string()))?);
            }
            Value(v) if path.is_none() => {
                path = Some(v.string().map_err(|e| Error::Usage(e.to_string()))?);
            }
            Value(v) if cmd.is_none() => {
                cmd = Some(v.string().map_err(|e| Error::Usage(e.to_string()))?);
                break;
            }
            _ => return Err(Error::Usage("unexpected argument".into())),
        }
    }

    let path = path.ok_or_else(|| Error::Usage("missing store path".into()))?;
    let cmd = cmd.ok_or_else(|| Error::Usage("missing command".into()))?;

    match cmd.as_str() {
        "doctor" => doctor(&path, durability, lock_path.as_deref(), json),
        "verify" => verify(&path, durability, lock_path.as_deref(), json),
        "stats" => stats(&path, durability, lock_path.as_deref(), json),
        "events" => events(&mut parser, &path, durability, lock_path.as_deref(), json),
        "projections" => projections(&mut parser, &path, durability, lock_path.as_deref(), json),
        "jobs" => jobs(&mut parser, &path, durability, lock_path.as_deref(), json),
        "export" => export(&path, durability, lock_path.as_deref()),
        "backup" => backup(&mut parser, &path, durability, lock_path.as_deref(), json),
        _ => Err(Error::Usage(format!("unknown command: {cmd}"))),
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
    let store = open_store(path, durability, lock_path)?;
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
    Ok(())
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

fn events(
    parser: &mut lexopt::Parser,
    path: &str,
    durability: Durability,
    lock_path: Option<&str>,
    json: bool,
) -> Result<(), Error> {
    let sub = parser
        .value()
        .map_err(|e| Error::Usage(e.to_string()))?
        .string()
        .map_err(|e| Error::Usage(e.to_string()))?;

    let store = open_store(path, durability, lock_path)?;
    let stdout = io::stdout();
    let mut stdout = stdout.lock();

    match sub.as_str() {
        "tail" => {
            let limit = next_usize(parser, 10)?;
            let events = store.events_after(0, limit);
            for e in events {
                if json {
                    writeln!(&mut stdout, "{}", event_json(&e))?;
                } else {
                    writeln_event(&mut stdout, &e)?;
                }
            }
        }
        "stream" => {
            let stream_id = parser
                .value()
                .map_err(|e| Error::Usage(e.to_string()))?
                .string()
                .map_err(|e| Error::Usage(e.to_string()))?;
            let limit = next_usize(parser, 10)?;
            let events = store.stream_events(&stream_id, 0, limit);
            for e in events {
                if json {
                    writeln!(&mut stdout, "{}", event_json(&e))?;
                } else {
                    writeln_event(&mut stdout, &e)?;
                }
            }
        }
        _ => return Err(Error::Usage(format!("unknown events subcommand: {sub}"))),
    }
    Ok(())
}

fn projections(
    parser: &mut lexopt::Parser,
    path: &str,
    durability: Durability,
    lock_path: Option<&str>,
    json: bool,
) -> Result<(), Error> {
    let sub = parser
        .value()
        .map_err(|e| Error::Usage(e.to_string()))?
        .string()
        .map_err(|e| Error::Usage(e.to_string()))?;

    let store = open_store(path, durability, lock_path)?;
    let stdout = io::stdout();
    let mut stdout = stdout.lock();

    match sub.as_str() {
        "list" => {
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
        }
        "get" => {
            let name = parser
                .value()
                .map_err(|e| Error::Usage(e.to_string()))?
                .string()
                .map_err(|e| Error::Usage(e.to_string()))?;
            let key = parser
                .value()
                .map_err(|e| Error::Usage(e.to_string()))?
                .string()
                .map_err(|e| Error::Usage(e.to_string()))?;
            let value = store.get_projection(&name, key.as_bytes())?;
            match value {
                Some(v) => {
                    if json {
                        writeln!(
                            &mut stdout,
                            "{}",
                            serde_json::json!({
                                "projection": name,
                                "key": key,
                                "value": hex(&v),
                            })
                        )?;
                    } else {
                        stdout.write_all(&v)?;
                        writeln!(stdout)?;
                    }
                }
                None => return Err(Error::ProjectionNotFound(name)),
            }
        }
        "scan" => {
            let name = parser
                .value()
                .map_err(|e| Error::Usage(e.to_string()))?
                .string()
                .map_err(|e| Error::Usage(e.to_string()))?;
            let prefix = parser
                .value()
                .ok()
                .and_then(|v| v.string().ok())
                .unwrap_or_default();
            let entries = store.scan_projection_prefix(&name, prefix.as_bytes())?;
            for entry in entries {
                if json {
                    writeln!(
                        &mut stdout,
                        "{}",
                        serde_json::json!({
                            "projection": name,
                            "key": String::from_utf8_lossy(&entry.key),
                            "value": hex(&entry.value),
                        })
                    )?;
                } else {
                    writeln!(
                        stdout,
                        "{} {} {}",
                        name,
                        bytes_repr(&entry.key),
                        bytes_repr(&entry.value)
                    )?;
                }
            }
        }
        _ => {
            return Err(Error::Usage(format!(
                "unknown projections subcommand: {sub}"
            )))
        }
    }
    Ok(())
}

fn jobs(
    parser: &mut lexopt::Parser,
    path: &str,
    durability: Durability,
    lock_path: Option<&str>,
    json: bool,
) -> Result<(), Error> {
    let mut state: Option<JobState> = None;
    let mut queue: Option<String> = None;

    while let Some(arg) = parser.next().map_err(|e| Error::Usage(e.to_string()))? {
        match arg {
            Short('s') | Long("state") => {
                let value = parser.value().map_err(|e| Error::Usage(e.to_string()))?;
                state = Some(parse_job_state(
                    &value.string().map_err(|e| Error::Usage(e.to_string()))?,
                )?);
            }
            Value(v) if queue.is_none() => {
                queue = Some(v.string().map_err(|e| Error::Usage(e.to_string()))?);
            }
            _ => return Err(Error::Usage("unexpected argument".into())),
        }
    }

    let store = open_store(path, durability, lock_path)?;
    let now_ms = current_time_ms();
    let records = store.jobs(now_ms, queue.clone(), state);
    for (job_id, spec, job_state) in records {
        if json {
            println!(
                "{}",
                serde_json::json!({
                    "job_id": job_id.to_hex(),
                    "state": job_state_str(job_state),
                    "queue": spec.queue,
                    "partition": spec.partition,
                    "payload": hex(&spec.payload),
                    "max_attempts": spec.max_attempts,
                    "effect_mode": effect_mode_str(spec.effect_mode),
                    "not_before_ms": spec.not_before_ms,
                    "idempotency_key": spec.idempotency_key,
                })
            );
        } else {
            println!(
                "{} {:?} {} {} {}",
                job_id,
                job_state,
                spec.queue,
                spec.partition,
                bytes_repr(&spec.payload)
            );
        }
    }
    Ok(())
}

fn export(path: &str, durability: Durability, lock_path: Option<&str>) -> Result<(), Error> {
    let store = open_store(path, durability, lock_path)?;
    let now_ms = current_time_ms();
    let stdout = io::stdout();
    let mut out = stdout.lock();

    let events = store.events_after(0, usize::MAX);
    for e in events {
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

    for (job_id, spec, state) in store.jobs(now_ms, None, None) {
        writeln!(
            out,
            r#"{{"type":"job","job_id":"{}","state":"{:?}","queue":"{}","partition":"{}","payload":"{}","max_attempts":{},"effect_mode":"{:?}"}}"#,
            job_id,
            state,
            json_escape(&spec.queue),
            json_escape(&spec.partition),
            hex(&spec.payload),
            spec.max_attempts,
            spec.effect_mode
        )?;
    }

    Ok(())
}

fn backup(
    parser: &mut lexopt::Parser,
    path: &str,
    durability: Durability,
    lock_path: Option<&str>,
    json: bool,
) -> Result<(), Error> {
    let dest = parser
        .value()
        .map_err(|e| Error::Usage(e.to_string()))?
        .string()
        .map_err(|e| Error::Usage(e.to_string()))?;
    let store = open_store(path, durability, lock_path)?;
    store.backup(&dest)?;
    if json {
        println!("{}", serde_json::json!({ "destination": dest }));
    } else {
        println!("backup written to {dest}");
    }
    Ok(())
}

fn next_usize(parser: &mut lexopt::Parser, default: usize) -> Result<usize, Error> {
    match parser.value() {
        Ok(v) => v
            .string()
            .map_err(|e| Error::Usage(e.to_string()))?
            .parse()
            .map_err(|e| Error::Usage(format!("invalid number: {e}"))),
        Err(lexopt::Error::MissingValue { .. }) => Ok(default),
        Err(e) => Err(Error::Usage(e.to_string())),
    }
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

fn writeln_event<W: Write>(out: &mut W, e: &minisqlite::PersistedEvent) -> Result<(), Error> {
    writeln!(
        out,
        "{} {} {} {} {} {} {}",
        e.global_sequence,
        e.stream_version,
        e.event.event_type,
        e.event.stream_id,
        e.event.schema_version,
        e.event.occurred_at_ms,
        bytes_repr(&e.event.payload)
    )?;
    Ok(())
}

fn event_json(e: &minisqlite::PersistedEvent) -> String {
    serde_json::json!({
        "global_sequence": e.global_sequence,
        "stream_version": e.stream_version,
        "transaction_id": e.transaction_id.to_hex(),
        "frame_offset": e.frame_offset,
        "event": {
            "event_id": e.event.event_id.to_hex(),
            "stream_id": e.event.stream_id,
            "event_type": e.event.event_type,
            "schema_version": e.event.schema_version,
            "occurred_at_ms": e.event.occurred_at_ms,
            "causation_id": e.event.causation_id.map(|id| id.to_hex()),
            "correlation_id": e.event.correlation_id.map(|id| id.to_hex()),
            "payload": hex(&e.event.payload),
            "metadata": hex(&e.event.metadata),
        }
    })
    .to_string()
}

fn bytes_repr(v: &[u8]) -> String {
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
