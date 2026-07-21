//! Operational tooling: backup, verify, stats, diagnostic export.

use std::collections::BTreeMap;
use std::fmt::Write as _;
use std::path::Path;

use rusqlite::{Connection, OptionalExtension};

use crate::error::{Error, StorageError, ValidationError};
use crate::jobs::JobState;
use crate::store::jobs::{ACTIVE_STATES_SQL, TERMINAL_STATES_SQL};
use crate::store::migrations;

/// A single finding from [`ControlPlaneStore::verify`](crate::ControlPlaneStore::verify):
/// which check failed and a human-readable detail.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct VerifyFinding {
    pub check: String,
    pub detail: String,
}

/// Result of a full verification pass. An empty `findings` list means the store is
/// consistent.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct VerifyReport {
    pub findings: Vec<VerifyFinding>,
}

impl VerifyReport {
    pub fn is_ok(&self) -> bool {
        self.findings.is_empty()
    }
}

/// Store-wide statistics.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct StoreStats {
    pub transactions: u64,
    pub events: u64,
    pub streams: u64,
    pub projections: u64,
    pub projection_entries: u64,
    pub jobs_by_state: BTreeMap<String, u64>,
    pub active_partitions: u64,
    pub file_size_bytes: u64,
    pub migration_version: u32,
    pub oldest_active_lease_ms: Option<i64>,
    pub oldest_uncertain_job_ms: Option<i64>,
}

/// Rows fetched per page during a diagnostic export.
const EXPORT_PAGE_SIZE: usize = 512;

/// Copy the database to `dest_path` using the SQLite backup API. Refuses an existing
/// destination unless `overwrite` is set. The resulting file's integrity is verified
/// before returning.
pub(crate) fn backup(conn: &Connection, dest_path: &Path, overwrite: bool) -> Result<(), Error> {
    if dest_path.exists() {
        if !overwrite {
            return Err(Error::Validation(ValidationError(format!(
                "backup destination {} already exists (pass overwrite to replace it)",
                dest_path.display()
            ))));
        }
        // Remove the old file so overwrite works even when the destination is
        // not a SQLite database.
        std::fs::remove_file(dest_path)
            .map_err(|e| StorageError::Io(format!("remove {}: {e}", dest_path.display())))?;
    }
    let mut dest = Connection::open(dest_path).map_err(StorageError::from_sqlite)?;
    {
        let backup =
            rusqlite::backup::Backup::new(conn, &mut dest).map_err(StorageError::from_sqlite)?;
        backup
            .run_to_completion(256, std::time::Duration::from_millis(10), None)
            .map_err(StorageError::from_sqlite)?;
    }
    let integrity: String = dest
        .query_row("PRAGMA integrity_check", [], |row| row.get(0))
        .map_err(StorageError::from_sqlite)?;
    if integrity != "ok" {
        return Err(Error::Storage(StorageError::Io(format!(
            "backup at {} failed integrity check: {integrity}",
            dest_path.display()
        ))));
    }
    Ok(())
}

/// Run integrity, foreign-key, migration-checksum, and semantic checks.
pub(crate) fn verify(conn: &Connection) -> Result<VerifyReport, Error> {
    let mut findings = Vec::new();

    let mut stmt = conn
        .prepare("PRAGMA integrity_check")
        .map_err(StorageError::from_sqlite)?;
    let rows = stmt
        .query_map([], |row| row.get::<_, String>(0))
        .map_err(StorageError::from_sqlite)?;
    for row in rows {
        let detail = row.map_err(StorageError::from_sqlite)?;
        if detail != "ok" {
            findings.push(VerifyFinding {
                check: "integrity_check".into(),
                detail,
            });
        }
    }

    let mut stmt = conn
        .prepare("PRAGMA foreign_key_check")
        .map_err(StorageError::from_sqlite)?;
    let rows = stmt
        .query_map([], |row| {
            Ok((
                row.get::<_, String>(0)?,
                row.get::<_, Option<i64>>(1)?,
                row.get::<_, String>(2)?,
            ))
        })
        .map_err(StorageError::from_sqlite)?;
    for row in rows {
        let (table, rowid, parent) = row.map_err(StorageError::from_sqlite)?;
        findings.push(VerifyFinding {
            check: "foreign_key_check".into(),
            detail: format!(
                "table {table} rowid {} references missing row in {parent}",
                rowid.map_or_else(|| "?".into(), |r| r.to_string())
            ),
        });
    }

    let statuses = migrations::status(conn).map_err(Error::from)?;
    if statuses.is_empty() {
        findings.push(VerifyFinding {
            check: "migrations".into(),
            detail: "no migrations applied".into(),
        });
    }
    for status in &statuses {
        if !status.checksum_ok {
            findings.push(VerifyFinding {
                check: "migrations".into(),
                detail: format!("migration {} checksum mismatch", status.version),
            });
        }
    }

    semantic_checks(conn, &mut findings)?;
    Ok(VerifyReport { findings })
}

fn semantic_checks(conn: &Connection, findings: &mut Vec<VerifyFinding>) -> Result<(), Error> {
    // streams.current_version must equal the highest event version of the stream.
    let mut stmt = conn
        .prepare(
            "SELECT s.stream_id, s.current_version, COALESCE(MAX(e.stream_version), 0)
         FROM streams s LEFT JOIN events e ON e.stream_id = s.stream_id
         GROUP BY s.stream_id HAVING s.current_version != COALESCE(MAX(e.stream_version), 0)",
        )
        .map_err(StorageError::from_sqlite)?;
    let rows = stmt
        .query_map([], |row| {
            Ok((
                row.get::<_, String>(0)?,
                row.get::<_, i64>(1)?,
                row.get::<_, i64>(2)?,
            ))
        })
        .map_err(StorageError::from_sqlite)?;
    for row in rows {
        let (stream_id, current, max) = row.map_err(StorageError::from_sqlite)?;
        findings.push(VerifyFinding {
            check: "stream_versions".into(),
            detail: format!(
                "stream {stream_id} current_version {current} != max event version {max}"
            ),
        });
    }
    let mut stmt = conn
        .prepare(
            "SELECT DISTINCT e.stream_id FROM events e
         LEFT JOIN streams s ON s.stream_id = e.stream_id WHERE s.stream_id IS NULL",
        )
        .map_err(StorageError::from_sqlite)?;
    let rows = stmt
        .query_map([], |row| row.get::<_, String>(0))
        .map_err(StorageError::from_sqlite)?;
    for row in rows {
        let stream_id = row.map_err(StorageError::from_sqlite)?;
        findings.push(VerifyFinding {
            check: "stream_versions".into(),
            detail: format!("stream {stream_id} has events but no streams row"),
        });
    }

    // Terminal jobs must carry no lease fields.
    let mut stmt = conn
        .prepare(&format!(
            "SELECT job_id FROM jobs WHERE state IN ({})
         AND (lease_token IS NOT NULL OR worker_id IS NOT NULL OR lease_expires_at_ms IS NOT NULL)",
            TERMINAL_STATES_SQL.as_str(),
        ))
        .map_err(StorageError::from_sqlite)?;
    let rows = stmt
        .query_map([], |row| row.get::<_, Vec<u8>>(0))
        .map_err(StorageError::from_sqlite)?;
    for row in rows {
        let job_id = row.map_err(StorageError::from_sqlite)?;
        findings.push(VerifyFinding {
            check: "terminal_jobs".into(),
            detail: format!("terminal job {} still has lease fields", hex(&job_id)),
        });
    }

    // Every active partition must contain nonterminal work.
    let mut stmt = conn
        .prepare(&format!(
            "SELECT ap.queue, ap.partition_key FROM active_partitions ap
         WHERE NOT EXISTS (SELECT 1 FROM jobs j WHERE j.queue = ap.queue
             AND j.partition_key = ap.partition_key AND j.state IN ({}))",
            ACTIVE_STATES_SQL.as_str(),
        ))
        .map_err(StorageError::from_sqlite)?;
    let rows = stmt
        .query_map([], |row| {
            Ok((row.get::<_, String>(0)?, row.get::<_, String>(1)?))
        })
        .map_err(StorageError::from_sqlite)?;
    for row in rows {
        let (queue, partition) = row.map_err(StorageError::from_sqlite)?;
        findings.push(VerifyFinding {
            check: "active_partitions".into(),
            detail: format!("partition {queue}/{partition} is active but has no nonterminal jobs"),
        });
    }

    // Applied migrations must match this build's migration SQL.
    for status in migrations::status(conn)? {
        if !status.checksum_ok {
            findings.push(VerifyFinding {
                check: "migration_checksums".into(),
                detail: format!("migration {} checksum mismatch", status.version),
            });
        }
    }

    // Leased jobs must carry complete lease fields.
    let mut stmt = conn
        .prepare(&format!(
            "SELECT job_id FROM jobs WHERE state = {}
         AND (lease_token IS NULL OR worker_id IS NULL OR lease_expires_at_ms IS NULL)",
            JobState::Leased.encode(),
        ))
        .map_err(StorageError::from_sqlite)?;
    let rows = stmt
        .query_map([], |row| row.get::<_, Vec<u8>>(0))
        .map_err(StorageError::from_sqlite)?;
    for row in rows {
        let job_id = row.map_err(StorageError::from_sqlite)?;
        findings.push(VerifyFinding {
            check: "leased_jobs".into(),
            detail: format!("leased job {} is missing lease fields", hex(&job_id)),
        });
    }

    // Claim receipts must reference existing jobs (no FK enforces this).
    let mut stmt = conn
        .prepare(
            "SELECT cr.transaction_id, cr.job_id FROM claim_receipts cr
         LEFT JOIN jobs j ON j.job_id = cr.job_id
         WHERE j.job_id IS NULL",
        )
        .map_err(StorageError::from_sqlite)?;
    let rows = stmt
        .query_map([], |row| {
            Ok((row.get::<_, Vec<u8>>(0)?, row.get::<_, Vec<u8>>(1)?))
        })
        .map_err(StorageError::from_sqlite)?;
    for row in rows {
        let (transaction_id, job_id) = row.map_err(StorageError::from_sqlite)?;
        findings.push(VerifyFinding {
            check: "claim_receipts".into(),
            detail: format!(
                "claim receipt in transaction {} references missing job {}",
                hex(&transaction_id),
                hex(&job_id)
            ),
        });
    }

    // Claim receipts must reference existing transactions.
    let mut stmt = conn
        .prepare(
            "SELECT cr.transaction_id, cr.job_id FROM claim_receipts cr
         LEFT JOIN transactions t ON t.transaction_id = cr.transaction_id
         WHERE t.transaction_id IS NULL",
        )
        .map_err(StorageError::from_sqlite)?;
    let rows = stmt
        .query_map([], |row| {
            Ok((row.get::<_, Vec<u8>>(0)?, row.get::<_, Vec<u8>>(1)?))
        })
        .map_err(StorageError::from_sqlite)?;
    for row in rows {
        let (transaction_id, job_id) = row.map_err(StorageError::from_sqlite)?;
        findings.push(VerifyFinding {
            check: "claim_receipts".into(),
            detail: format!(
                "claim receipt for job {} references missing transaction {}",
                hex(&job_id),
                hex(&transaction_id)
            ),
        });
    }
    Ok(())
}

/// Collect store-wide statistics.
pub(crate) fn stats(conn: &Connection, db_path: &Path) -> Result<StoreStats, Error> {
    let mut jobs_by_state = BTreeMap::new();
    let mut stmt = conn
        .prepare("SELECT state, COUNT(*) FROM jobs GROUP BY state")
        .map_err(StorageError::from_sqlite)?;
    let rows = stmt
        .query_map([], |row| Ok((row.get::<_, i64>(0)?, row.get::<_, i64>(1)?)))
        .map_err(StorageError::from_sqlite)?;
    for row in rows {
        let (state, count) = row.map_err(StorageError::from_sqlite)?;
        jobs_by_state.insert(state_name(state), count as u64);
    }

    Ok(StoreStats {
        transactions: count(conn, "transactions")?,
        events: count(conn, "events")?,
        streams: count(conn, "streams")?,
        projections: count(conn, "projection_meta")?,
        projection_entries: count(conn, "projection_entries")?,
        jobs_by_state,
        active_partitions: count(conn, "active_partitions")?,
        file_size_bytes: std::fs::metadata(db_path)
            .map(|m| m.len())
            .map_err(|e| StorageError::Io(e.to_string()))?,
        migration_version: conn
            .query_row(
                "SELECT COALESCE(MAX(version), 0) FROM schema_migrations",
                [],
                |row| row.get::<_, i64>(0),
            )
            .map_err(StorageError::from_sqlite)? as u32,
        oldest_active_lease_ms: conn
            .query_row(
                &format!(
                    "SELECT MIN(lease_expires_at_ms) FROM jobs WHERE state = {}",
                    JobState::Leased.encode()
                ),
                [],
                |row| row.get(0),
            )
            .optional()
            .map_err(StorageError::from_sqlite)?
            .flatten(),
        // For uncertain jobs the interesting age is when their lease expired into
        // uncertainty, which is the lease expiry they carried at that moment.
        oldest_uncertain_job_ms: conn
            .query_row(
                &format!(
                    "SELECT MIN(lease_expires_at_ms) FROM jobs WHERE state = {}",
                    JobState::Uncertain.encode()
                ),
                [],
                |row| row.get(0),
            )
            .optional()
            .map_err(StorageError::from_sqlite)?
            .flatten(),
    })
}

fn count(conn: &Connection, table: &str) -> Result<u64, Error> {
    let n: i64 = conn
        .query_row(&format!("SELECT COUNT(*) FROM {table}"), [], |row| {
            row.get(0)
        })
        .map_err(StorageError::from_sqlite)?;
    Ok(n as u64)
}

fn state_name(code: i64) -> String {
    match JobState::decode(code) {
        Some(state) => state.name().into(),
        None => format!("unknown({code})"),
    }
}

/// Produce a diagnostic export as JSON Lines text.
///
/// The export is explicitly non-restorable: it exists for debugging, not backup.
/// Event and job payloads (and projection entry values) are redacted to their byte
/// length unless `include_payloads` is set, and job error summaries are likewise
/// redacted to their length by default. Lease tokens are never included.
/// Rows are read in pages of [`EXPORT_PAGE_SIZE`] to bound memory usage.
pub(crate) fn diagnostic_export(
    conn: &Connection,
    db_path: &Path,
    include_payloads: bool,
) -> Result<String, Error> {
    let mut out = String::new();
    let stats = stats(conn, db_path)?;
    let _ = writeln!(
        out,
        "{{\"kind\":\"header\",\"format\":\"minisqlite-diagnostic-v1\",\"restorable\":false,\
         \"schema_version\":{},\"payloads_included\":{}}}",
        stats.migration_version, include_payloads
    );
    let mut states = String::new();
    for (i, (state, n)) in stats.jobs_by_state.iter().enumerate() {
        if i > 0 {
            states.push(',');
        }
        let _ = write!(states, "\"{}\":{n}", json_escape(state));
    }
    let _ = writeln!(
        out,
        "{{\"kind\":\"stats\",\"transactions\":{},\"events\":{},\"streams\":{},\
         \"projections\":{},\"projection_entries\":{},\"active_partitions\":{},\
         \"file_size_bytes\":{},\"jobs_by_state\":{{{states}}}}}",
        stats.transactions,
        stats.events,
        stats.streams,
        stats.projections,
        stats.projection_entries,
        stats.active_partitions,
        stats.file_size_bytes,
    );

    export_transactions(conn, &mut out)?;
    export_events(conn, &mut out, include_payloads)?;
    export_streams(conn, &mut out)?;
    export_projections(conn, &mut out, include_payloads)?;
    export_jobs(conn, &mut out, include_payloads)?;
    export_partitions(conn, &mut out)?;
    export_claim_receipts(conn, &mut out)?;
    Ok(out)
}

fn export_transactions(conn: &Connection, out: &mut String) -> Result<(), Error> {
    let mut last: i64 = 0;
    loop {
        let mut stmt = conn
            .prepare(
                "SELECT transaction_sequence, transaction_id, committed_at_ms, operation_count
             FROM transactions WHERE transaction_sequence > ?1
             ORDER BY transaction_sequence LIMIT ?2",
            )
            .map_err(StorageError::from_sqlite)?;
        let rows = stmt
            .query_map(rusqlite::params![last, EXPORT_PAGE_SIZE as i64], |row| {
                Ok((
                    row.get::<_, i64>(0)?,
                    row.get::<_, Vec<u8>>(1)?,
                    row.get::<_, i64>(2)?,
                    row.get::<_, i64>(3)?,
                ))
            })
            .map_err(StorageError::from_sqlite)?;
        let mut n = 0;
        for row in rows {
            let (seq, id, committed_at_ms, operation_count) =
                row.map_err(StorageError::from_sqlite)?;
            let _ = writeln!(
                out,
                "{{\"kind\":\"transaction\",\"transaction_sequence\":{seq},\
                 \"transaction_id\":\"{}\",\"committed_at_ms\":{committed_at_ms},\
                 \"operation_count\":{operation_count}}}",
                hex(&id)
            );
            last = seq;
            n += 1;
        }
        if n < EXPORT_PAGE_SIZE {
            return Ok(());
        }
    }
}

fn export_events(conn: &Connection, out: &mut String, include_payloads: bool) -> Result<(), Error> {
    let mut last: i64 = 0;
    loop {
        let mut stmt = conn
            .prepare(
                "SELECT global_sequence, event_id, transaction_id, stream_id, stream_version,
                    event_type, schema_version, occurred_at_ms, payload
             FROM events WHERE global_sequence > ?1 ORDER BY global_sequence LIMIT ?2",
            )
            .map_err(StorageError::from_sqlite)?;
        let rows = stmt
            .query_map(rusqlite::params![last, EXPORT_PAGE_SIZE as i64], |row| {
                Ok((
                    row.get::<_, i64>(0)?,
                    row.get::<_, Vec<u8>>(1)?,
                    row.get::<_, Vec<u8>>(2)?,
                    row.get::<_, String>(3)?,
                    row.get::<_, i64>(4)?,
                    row.get::<_, String>(5)?,
                    row.get::<_, i64>(6)?,
                    row.get::<_, i64>(7)?,
                    row.get::<_, Vec<u8>>(8)?,
                ))
            })
            .map_err(StorageError::from_sqlite)?;
        let mut n = 0;
        for row in rows {
            let (seq, event_id, txn_id, stream_id, version, event_type, schema, occurred, payload) =
                row.map_err(StorageError::from_sqlite)?;
            let _ = writeln!(
                out,
                "{{\"kind\":\"event\",\"global_sequence\":{seq},\"event_id\":\"{}\",\
                 \"transaction_id\":\"{}\",\"stream_id\":\"{}\",\"stream_version\":{version},\
                 \"event_type\":\"{}\",\"schema_version\":{schema},\"occurred_at_ms\":{occurred},{}}}",
                hex(&event_id),
                hex(&txn_id),
                json_escape(&stream_id),
                json_escape(&event_type),
                payload_fields(&payload, include_payloads),
            );
            last = seq;
            n += 1;
        }
        if n < EXPORT_PAGE_SIZE {
            return Ok(());
        }
    }
}

fn export_streams(conn: &Connection, out: &mut String) -> Result<(), Error> {
    let mut last = String::new();
    loop {
        let mut stmt = conn
            .prepare(
                "SELECT stream_id, current_version FROM streams WHERE stream_id > ?1
             ORDER BY stream_id LIMIT ?2",
            )
            .map_err(StorageError::from_sqlite)?;
        let rows = stmt
            .query_map(rusqlite::params![last, EXPORT_PAGE_SIZE as i64], |row| {
                Ok((row.get::<_, String>(0)?, row.get::<_, i64>(1)?))
            })
            .map_err(StorageError::from_sqlite)?;
        let mut n = 0;
        for row in rows {
            let (stream_id, version) = row.map_err(StorageError::from_sqlite)?;
            let _ = writeln!(
                out,
                "{{\"kind\":\"stream\",\"stream_id\":\"{}\",\"current_version\":{version}}}",
                json_escape(&stream_id)
            );
            last = stream_id;
            n += 1;
        }
        if n < EXPORT_PAGE_SIZE {
            return Ok(());
        }
    }
}

fn export_projections(
    conn: &Connection,
    out: &mut String,
    include_payloads: bool,
) -> Result<(), Error> {
    let mut stmt = conn
        .prepare("SELECT projection, version FROM projection_meta ORDER BY projection")
        .map_err(StorageError::from_sqlite)?;
    let rows = stmt
        .query_map([], |row| {
            Ok((row.get::<_, String>(0)?, row.get::<_, i64>(1)?))
        })
        .map_err(StorageError::from_sqlite)?;
    for row in rows {
        let (projection, version) = row.map_err(StorageError::from_sqlite)?;
        let _ = writeln!(
            out,
            "{{\"kind\":\"projection\",\"projection\":\"{}\",\"version\":{version}}}",
            json_escape(&projection)
        );
    }

    let mut last: i64 = 0;
    loop {
        let mut stmt = conn
            .prepare(
                "SELECT rowid, projection, key, value FROM projection_entries WHERE rowid > ?1
             ORDER BY rowid LIMIT ?2",
            )
            .map_err(StorageError::from_sqlite)?;
        let rows = stmt
            .query_map(rusqlite::params![last, EXPORT_PAGE_SIZE as i64], |row| {
                Ok((
                    row.get::<_, i64>(0)?,
                    row.get::<_, String>(1)?,
                    row.get::<_, Vec<u8>>(2)?,
                    row.get::<_, Vec<u8>>(3)?,
                ))
            })
            .map_err(StorageError::from_sqlite)?;
        let mut n = 0;
        for row in rows {
            let (rowid, projection, key, value) = row.map_err(StorageError::from_sqlite)?;
            let value_fields = if include_payloads {
                format!(
                    "\"value_len\":{},\"value_hex\":\"{}\"",
                    value.len(),
                    hex(&value)
                )
            } else {
                format!("\"value_len\":{}", value.len())
            };
            let _ = writeln!(
                out,
                "{{\"kind\":\"projection_entry\",\"projection\":\"{}\",\"key_hex\":\"{}\",{value_fields}}}",
                json_escape(&projection),
                hex(&key),
            );
            last = rowid;
            n += 1;
        }
        if n < EXPORT_PAGE_SIZE {
            return Ok(());
        }
    }
}

fn export_jobs(conn: &Connection, out: &mut String, include_payloads: bool) -> Result<(), Error> {
    let mut last: i64 = 0;
    loop {
        // lease_token is deliberately never selected: exports must not leak leases.
        let mut stmt = conn
            .prepare(
                "SELECT enqueue_sequence, job_id, queue, partition_key, state, attempt,
                    max_attempts, effect_mode, not_before_ms, worker_id, lease_expires_at_ms,
                    retry_after_ms, terminal_at_ms, error_summary, payload
             FROM jobs WHERE enqueue_sequence > ?1 ORDER BY enqueue_sequence LIMIT ?2",
            )
            .map_err(StorageError::from_sqlite)?;
        let rows = stmt
            .query_map(rusqlite::params![last, EXPORT_PAGE_SIZE as i64], |row| {
                Ok((
                    row.get::<_, i64>(0)?,
                    row.get::<_, Vec<u8>>(1)?,
                    row.get::<_, String>(2)?,
                    row.get::<_, String>(3)?,
                    row.get::<_, i64>(4)?,
                    row.get::<_, i64>(5)?,
                    row.get::<_, i64>(6)?,
                    row.get::<_, i64>(7)?,
                    row.get::<_, i64>(8)?,
                    row.get::<_, Option<String>>(9)?,
                    row.get::<_, Option<i64>>(10)?,
                    row.get::<_, Option<i64>>(11)?,
                    row.get::<_, Option<i64>>(12)?,
                    row.get::<_, Option<String>>(13)?,
                    row.get::<_, Vec<u8>>(14)?,
                ))
            })
            .map_err(StorageError::from_sqlite)?;
        let mut n = 0;
        for row in rows {
            let (
                seq,
                job_id,
                queue,
                partition_key,
                state,
                attempt,
                max_attempts,
                effect_mode,
                not_before_ms,
                worker_id,
                lease_expires_at_ms,
                retry_after_ms,
                terminal_at_ms,
                error_summary,
                payload,
            ) = row.map_err(StorageError::from_sqlite)?;
            let _ = writeln!(
                out,
                "{{\"kind\":\"job\",\"enqueue_sequence\":{seq},\"job_id\":\"{}\",\
                 \"queue\":\"{}\",\"partition_key\":\"{}\",\"state\":\"{}\",\
                 \"attempt\":{attempt},\"max_attempts\":{max_attempts},\
                 \"effect_mode\":{effect_mode},\"not_before_ms\":{not_before_ms},\
                 \"worker_id\":{},\"lease_expires_at_ms\":{},\"retry_after_ms\":{},\
                 \"terminal_at_ms\":{},{},{}}}",
                hex(&job_id),
                json_escape(&queue),
                json_escape(&partition_key),
                state_name(state),
                opt_string(worker_id.as_deref()),
                opt_i64(lease_expires_at_ms),
                opt_i64(retry_after_ms),
                opt_i64(terminal_at_ms),
                error_summary_fields(error_summary.as_deref(), include_payloads),
                payload_fields(&payload, include_payloads),
            );
            last = seq;
            n += 1;
        }
        if n < EXPORT_PAGE_SIZE {
            return Ok(());
        }
    }
}

fn export_partitions(conn: &Connection, out: &mut String) -> Result<(), Error> {
    let mut stmt = conn
        .prepare(
            "SELECT queue, partition_key, first_active_sequence FROM active_partitions
         ORDER BY queue, partition_key",
        )
        .map_err(StorageError::from_sqlite)?;
    let rows = stmt
        .query_map([], |row| {
            Ok((
                row.get::<_, String>(0)?,
                row.get::<_, String>(1)?,
                row.get::<_, i64>(2)?,
            ))
        })
        .map_err(StorageError::from_sqlite)?;
    for row in rows {
        let (queue, partition_key, first_active_sequence) =
            row.map_err(StorageError::from_sqlite)?;
        let _ = writeln!(
            out,
            "{{\"kind\":\"active_partition\",\"queue\":\"{}\",\"partition_key\":\"{}\",\
             \"first_active_sequence\":{first_active_sequence}}}",
            json_escape(&queue),
            json_escape(&partition_key)
        );
    }
    Ok(())
}

fn export_claim_receipts(conn: &Connection, out: &mut String) -> Result<(), Error> {
    // lease_token is deliberately never selected: exports must not leak leases.
    let mut stmt = conn
        .prepare(
            "SELECT transaction_id, job_id, attempt, worker_id, lease_expires_at_ms
         FROM claim_receipts ORDER BY rowid",
        )
        .map_err(StorageError::from_sqlite)?;
    let rows = stmt
        .query_map([], |row| {
            Ok((
                row.get::<_, Vec<u8>>(0)?,
                row.get::<_, Vec<u8>>(1)?,
                row.get::<_, i64>(2)?,
                row.get::<_, String>(3)?,
                row.get::<_, i64>(4)?,
            ))
        })
        .map_err(StorageError::from_sqlite)?;
    for row in rows {
        let (transaction_id, job_id, attempt, worker_id, lease_expires_at_ms) =
            row.map_err(StorageError::from_sqlite)?;
        let _ = writeln!(
            out,
            "{{\"kind\":\"claim_receipt\",\"transaction_id\":\"{}\",\"job_id\":\"{}\",\
             \"attempt\":{attempt},\"worker_id\":\"{}\",\
             \"lease_expires_at_ms\":{lease_expires_at_ms}}}",
            hex(&transaction_id),
            hex(&job_id),
            json_escape(&worker_id)
        );
    }
    Ok(())
}

fn error_summary_fields(error_summary: Option<&str>, include_payloads: bool) -> String {
    if include_payloads {
        format!("\"error_summary\":{}", opt_string(error_summary))
    } else {
        // Error summaries can carry sensitive details; export only their length
        // unless payloads were explicitly requested.
        format!(
            "\"error_summary_len\":{}",
            opt_i64(error_summary.map(|s| s.len() as i64))
        )
    }
}

fn payload_fields(payload: &[u8], include_payloads: bool) -> String {
    if include_payloads {
        format!(
            "\"payload_len\":{},\"payload_hex\":\"{}\"",
            payload.len(),
            hex(payload)
        )
    } else {
        format!("\"payload_len\":{}", payload.len())
    }
}

fn opt_i64(value: Option<i64>) -> String {
    value.map_or_else(|| "null".into(), |v| v.to_string())
}

fn opt_string(value: Option<&str>) -> String {
    value.map_or_else(|| "null".into(), |v| format!("\"{}\"", json_escape(v)))
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

fn json_escape(s: &str) -> String {
    let mut out = String::with_capacity(s.len());
    for c in s.chars() {
        match c {
            '"' => out.push_str("\\\""),
            '\\' => out.push_str("\\\\"),
            '\n' => out.push_str("\\n"),
            '\r' => out.push_str("\\r"),
            '\t' => out.push_str("\\t"),
            c if (c as u32) < 0x20 => {
                let _ = write!(out, "\\u{:04x}", c as u32);
            }
            c => out.push(c),
        }
    }
    out
}
