use rusqlite::{Connection, OptionalExtension};

use crate::error::StorageError;

/// A single schema migration. `sql` is checksummed at apply time and verified at open,
/// so a build whose migration SQL diverges from the database's history is rejected.
pub(crate) struct Migration {
    pub version: u32,
    pub sql: &'static str,
}

pub(crate) const MIGRATIONS: &[Migration] = &[Migration {
    version: 1,
    sql: "\
CREATE TABLE transactions (transaction_id BLOB PRIMARY KEY, transaction_sequence INTEGER NOT NULL UNIQUE, committed_at_ms INTEGER NOT NULL, correlation_id BLOB, metadata BLOB NOT NULL, request_digest BLOB NOT NULL, operation_count INTEGER NOT NULL);
CREATE TABLE events (global_sequence INTEGER PRIMARY KEY AUTOINCREMENT, event_id BLOB NOT NULL UNIQUE, transaction_id BLOB NOT NULL REFERENCES transactions(transaction_id), stream_id TEXT NOT NULL, stream_version INTEGER NOT NULL, event_type TEXT NOT NULL, schema_version INTEGER NOT NULL, occurred_at_ms INTEGER NOT NULL, causation_id BLOB, correlation_id BLOB, payload BLOB NOT NULL, metadata BLOB NOT NULL, UNIQUE(stream_id, stream_version));
CREATE INDEX events_stream_idx ON events(stream_id, stream_version);
CREATE INDEX events_transaction_idx ON events(transaction_id);
CREATE TABLE streams (stream_id TEXT PRIMARY KEY, current_version INTEGER NOT NULL);
CREATE TABLE projection_meta (projection TEXT PRIMARY KEY, version INTEGER NOT NULL);
CREATE TABLE projection_entries (projection TEXT NOT NULL REFERENCES projection_meta(projection) ON DELETE CASCADE, key BLOB NOT NULL, value BLOB NOT NULL, PRIMARY KEY(projection, key));
CREATE TABLE jobs (job_id BLOB PRIMARY KEY, enqueue_sequence INTEGER NOT NULL UNIQUE, enqueue_transaction_id BLOB NOT NULL REFERENCES transactions(transaction_id), queue TEXT NOT NULL, partition_key TEXT NOT NULL, payload BLOB NOT NULL, not_before_ms INTEGER NOT NULL, max_attempts INTEGER NOT NULL, effect_mode INTEGER NOT NULL, idempotency_key TEXT, state INTEGER NOT NULL, attempt INTEGER NOT NULL, lease_token BLOB, worker_id TEXT, lease_expires_at_ms INTEGER, retry_after_ms INTEGER, terminal_at_ms INTEGER, result_digest BLOB, error_summary TEXT, updated_transaction_id BLOB NOT NULL);
CREATE INDEX jobs_ready_idx ON jobs(queue, partition_key, state, not_before_ms, enqueue_sequence);
CREATE INDEX jobs_expiry_idx ON jobs(state, lease_expires_at_ms);
CREATE INDEX jobs_queue_state_idx ON jobs(queue, state);
CREATE INDEX jobs_transaction_idx ON jobs(updated_transaction_id);
CREATE TABLE queue_cursors (queue TEXT PRIMARY KEY, last_partition_key TEXT);
CREATE TABLE active_partitions (queue TEXT NOT NULL, partition_key TEXT NOT NULL, first_active_sequence INTEGER NOT NULL, PRIMARY KEY(queue, partition_key));
CREATE TABLE claim_receipts (transaction_id BLOB NOT NULL REFERENCES transactions(transaction_id), job_id BLOB NOT NULL, lease_token BLOB NOT NULL, attempt INTEGER NOT NULL, worker_id TEXT NOT NULL, lease_expires_at_ms INTEGER NOT NULL, PRIMARY KEY(transaction_id, job_id));
",
}];

/// FNV-1a-128 checksum of the migration SQL text (same hash family as the request
/// digest; see `CommitBatch::request_digest` for rationale).
pub(crate) fn checksum(sql: &str) -> [u8; 16] {
    const OFFSET_BASIS: u128 = 0x6c62272e07bb014262b821756295c58d;
    const PRIME: u128 = 0x0000000001000000000000000000013b;
    let mut state = OFFSET_BASIS;
    for &b in sql.as_bytes() {
        state ^= u128::from(b);
        state = state.wrapping_mul(PRIME);
    }
    state.to_be_bytes()
}

/// A row from `schema_migrations`, for status reporting.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MigrationStatus {
    pub version: u32,
    pub applied_at_ms: i64,
    pub checksum_ok: bool,
}

/// Apply all pending migrations, verifying checksums of already-applied ones.
/// Each migration runs in its own transaction, so the schema is never half-applied.
pub(crate) fn migrate(conn: &mut Connection) -> Result<(), StorageError> {
    conn.execute(
        "CREATE TABLE IF NOT EXISTS schema_migrations (version INTEGER PRIMARY KEY, applied_at_ms INTEGER NOT NULL, checksum BLOB NOT NULL)",
        [],
    )?;
    let current: u32 = conn.query_row(
        "SELECT COALESCE(MAX(version), 0) FROM schema_migrations",
        [],
        |row| row.get(0),
    )?;
    let supported = MIGRATIONS.last().map(|m| m.version).unwrap_or(0);
    if current > supported {
        return Err(StorageError::SchemaTooNew {
            version: current,
            supported,
        });
    }
    for migration in MIGRATIONS {
        let applied: Option<Vec<u8>> = conn
            .query_row(
                "SELECT checksum FROM schema_migrations WHERE version = ?1",
                [migration.version],
                |row| row.get(0),
            )
            .optional()?;
        let expected = checksum(migration.sql);
        match applied {
            Some(stored) => {
                if stored != expected {
                    return Err(StorageError::MigrationChecksumMismatch {
                        version: migration.version,
                    });
                }
            }
            None => {
                let tx = conn.transaction()?;
                tx.execute_batch(migration.sql)?;
                tx.execute(
                    "INSERT INTO schema_migrations (version, applied_at_ms, checksum) VALUES (?1, ?2, ?3)",
                    rusqlite::params![migration.version, now_ms(), expected.as_slice()],
                )?;
                tx.commit()?;
            }
        }
    }
    Ok(())
}

/// Report the status of every known migration against the database.
pub(crate) fn status(conn: &Connection) -> Result<Vec<MigrationStatus>, StorageError> {
    let mut out = Vec::with_capacity(MIGRATIONS.len());
    for migration in MIGRATIONS {
        let row: Option<(i64, Vec<u8>)> = conn
            .query_row(
                "SELECT applied_at_ms, checksum FROM schema_migrations WHERE version = ?1",
                [migration.version],
                |row| Ok((row.get(0)?, row.get(1)?)),
            )
            .optional()?;
        if let Some((applied_at_ms, stored)) = row {
            out.push(MigrationStatus {
                version: migration.version,
                applied_at_ms,
                checksum_ok: stored == checksum(migration.sql),
            });
        }
    }
    Ok(out)
}

fn now_ms() -> i64 {
    use std::time::{SystemTime, UNIX_EPOCH};
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis() as i64)
        .unwrap_or(0)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn migrations_are_idempotent() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("db");
        let mut conn = Connection::open(&path).unwrap();
        migrate(&mut conn).unwrap();
        migrate(&mut conn).unwrap();
        let statuses = status(&conn).unwrap();
        assert_eq!(statuses.len(), MIGRATIONS.len());
        assert!(statuses.iter().all(|s| s.checksum_ok));
    }

    #[test]
    fn checksum_mismatch_is_rejected() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("db");
        let mut conn = Connection::open(&path).unwrap();
        migrate(&mut conn).unwrap();
        conn.execute(
            "UPDATE schema_migrations SET checksum = X'00' WHERE version = 1",
            [],
        )
        .unwrap();
        let err = migrate(&mut conn).unwrap_err();
        assert_eq!(err, StorageError::MigrationChecksumMismatch { version: 1 });
    }

    #[test]
    fn v1_schema_creates_all_tables() {
        let dir = tempfile::tempdir().unwrap();
        let mut conn = Connection::open(dir.path().join("db")).unwrap();
        migrate(&mut conn).unwrap();
        for table in [
            "schema_migrations",
            "transactions",
            "events",
            "streams",
            "projection_meta",
            "projection_entries",
            "jobs",
            "queue_cursors",
            "active_partitions",
            "claim_receipts",
        ] {
            let count: i64 = conn
                .query_row(
                    "SELECT COUNT(*) FROM sqlite_master WHERE type='table' AND name = ?1",
                    [table],
                    |row| row.get(0),
                )
                .unwrap();
            assert_eq!(count, 1, "missing table {table}");
        }
    }
}
