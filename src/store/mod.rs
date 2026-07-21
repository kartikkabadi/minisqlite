//! The public store façade. All access goes through [`ControlPlaneStore`]; rusqlite
//! types and raw connections are never exposed.

pub(crate) mod commit;
pub(crate) mod connection;
pub(crate) mod events;
#[cfg(feature = "failpoints")]
pub mod failpoints;
pub(crate) mod jobs;
pub(crate) mod migrations;
pub(crate) mod ops;
pub(crate) mod projections;

use std::path::{Path, PathBuf};
use std::sync::Mutex;

use rusqlite::Connection;

use crate::config::{Durability, Limits};
use crate::error::{ClaimError, CommitError, Error, LeaseError, RecoveryError, StorageError};
use crate::event::PersistedEvent;
use crate::id::Id;
use crate::jobs::{
    ClaimOutcome, ClaimRecovery, ClaimRequest, JobInfo, JobState, LeaseExtensionReceipt,
};
use crate::projection::ProjectionEntry;
use crate::transaction::{CommitBatch, CommitReceipt, TransactionRecovery};

pub use migrations::MigrationStatus;
pub use ops::{StoreStats, VerifyFinding, VerifyReport};

/// Builder for a [`ControlPlaneStore`].
#[derive(Debug, Clone)]
pub struct StoreBuilder {
    path: PathBuf,
    durability: Durability,
    limits: Limits,
}

impl StoreBuilder {
    /// Start building a store at `path` with default durability and limits.
    pub fn new(path: impl Into<PathBuf>) -> Self {
        Self {
            path: path.into(),
            durability: Durability::default(),
            limits: Limits::default(),
        }
    }

    /// Set the durability level.
    pub fn durability(mut self, durability: Durability) -> Self {
        self.durability = durability;
        self
    }

    /// Set the size and shape limits.
    pub fn limits(mut self, limits: Limits) -> Self {
        self.limits = limits;
        self
    }

    /// Open (or create) the store, applying pragmas and any pending migrations.
    pub fn open(self) -> Result<ControlPlaneStore, Error> {
        self.limits.validate()?;
        let mut conn = connection::open(&self.path, self.durability)?;
        migrations::migrate(&mut conn)?;
        Ok(ControlPlaneStore {
            writer: Mutex::new(Some(conn)),
            path: self.path,
            limits: self.limits,
            durability: self.durability,
        })
    }

    /// Open an existing store read-only for inspection: never creates the database
    /// file, never migrates, and never writes. Fails with a clear error if the file
    /// does not exist or its schema is not exactly this build's supported version.
    pub fn open_existing(self) -> Result<ControlPlaneStore, Error> {
        self.limits.validate()?;
        let conn = connection::open_existing(&self.path)?;
        migrations::require_current(&conn)?;
        Ok(ControlPlaneStore {
            writer: Mutex::new(Some(conn)),
            path: self.path,
            limits: self.limits,
            durability: self.durability,
        })
    }
}

/// A typed embedded control-plane state kernel on SQLite.
///
/// One atomic transaction coordinates domain events, materialized projections,
/// durable jobs, and honest uncertainty handling.
#[derive(Debug)]
pub struct ControlPlaneStore {
    /// The single writer connection. `None` means the connection was poisoned by an
    /// indeterminate COMMIT outcome (plan §7.4) and must be reopened before reuse.
    writer: Mutex<Option<Connection>>,
    path: PathBuf,
    limits: Limits,
    durability: Durability,
}

impl ControlPlaneStore {
    /// Open (or create) a store at `path` with default configuration.
    pub fn open(path: impl Into<PathBuf>) -> Result<Self, Error> {
        StoreBuilder::new(path).open()
    }

    /// Open an existing store for inspection with default configuration: never
    /// creates the database file and never migrates.
    pub fn open_existing(path: impl Into<PathBuf>) -> Result<Self, Error> {
        StoreBuilder::new(path).open_existing()
    }

    /// Start building a store with custom configuration.
    pub fn builder(path: impl Into<PathBuf>) -> StoreBuilder {
        StoreBuilder::new(path)
    }

    /// The database file path.
    pub fn path(&self) -> &Path {
        &self.path
    }

    /// The configured durability level.
    pub fn durability(&self) -> Durability {
        self.durability
    }

    /// The configured limits.
    pub fn limits(&self) -> Limits {
        self.limits
    }

    fn writer(&self) -> std::sync::MutexGuard<'_, Option<Connection>> {
        // A poisoned mutex means another thread panicked mid-operation; the SQLite
        // transaction it held has rolled back, so the connection is safe to reuse.
        match self.writer.lock() {
            Ok(guard) => guard,
            Err(poisoned) => poisoned.into_inner(),
        }
    }

    /// Return the live writer connection, reopening it if a previous indeterminate
    /// COMMIT poisoned it.
    fn connection<'a>(
        &self,
        guard: &'a mut Option<Connection>,
    ) -> Result<&'a mut Connection, StorageError> {
        if guard.is_none() {
            *guard = Some(connection::open(&self.path, self.durability)?);
        }
        guard
            .as_mut()
            .ok_or_else(|| StorageError::Io("writer connection unavailable".into()))
    }

    // ----- transactions -----

    /// Atomically commit a batch of events, projection patches, and job operations.
    ///
    /// On an indeterminate COMMIT outcome the writer connection is discarded and
    /// reopened lazily by the next operation (plan §7.4).
    pub fn commit(&self, batch: &CommitBatch) -> Result<CommitReceipt, CommitError> {
        let mut guard = self.writer();
        let conn = self.connection(&mut guard).map_err(CommitError::Storage)?;
        let result = commit::commit(conn, &self.limits, batch);
        if matches!(result, Err(CommitError::Indeterminate(_))) {
            *guard = None;
        }
        result
    }

    /// Recover the outcome of an indeterminate commit. Always reopens a fresh
    /// connection first when the writer was poisoned by the failed COMMIT.
    pub fn recover_transaction(
        &self,
        transaction_id: Id,
    ) -> Result<TransactionRecovery, RecoveryError> {
        let mut guard = self.writer();
        let conn = self
            .connection(&mut guard)
            .map_err(RecoveryError::Storage)?;
        commit::recover_transaction(conn, transaction_id).map_err(RecoveryError::from)
    }

    // ----- events -----

    /// Events with a global sequence strictly greater than `after`, oldest first.
    pub fn events_after(&self, after: u64, limit: usize) -> Result<Vec<PersistedEvent>, Error> {
        let mut guard = self.writer();
        let conn = self.connection(&mut guard)?;
        events::events_after(conn, after, limit).map_err(Error::from)
    }

    /// The most recent `limit` events, oldest first.
    pub fn last_events(&self, limit: usize) -> Result<Vec<PersistedEvent>, Error> {
        let mut guard = self.writer();
        let conn = self.connection(&mut guard)?;
        events::last_events(conn, limit).map_err(Error::from)
    }

    /// Events for one stream with a stream version of at least `from_version`, oldest
    /// first.
    pub fn stream_events(
        &self,
        stream_id: &str,
        from_version: u64,
        limit: usize,
    ) -> Result<Vec<PersistedEvent>, Error> {
        let mut guard = self.writer();
        let conn = self.connection(&mut guard)?;
        events::stream_events(conn, stream_id, from_version, limit).map_err(Error::from)
    }

    /// Look up one event by its ID.
    pub fn get_event(&self, event_id: Id) -> Result<Option<PersistedEvent>, Error> {
        let mut guard = self.writer();
        let conn = self.connection(&mut guard)?;
        events::get_event(conn, event_id).map_err(Error::from)
    }

    /// The current durable version of a stream (0 when the stream does not exist).
    pub fn stream_version(&self, stream_id: &str) -> Result<u64, Error> {
        let mut guard = self.writer();
        let conn = self.connection(&mut guard)?;
        events::stream_version(conn, stream_id).map_err(Error::from)
    }

    // ----- projections -----

    /// The current version of a projection (0 when it does not exist).
    pub fn projection_version(&self, projection: &str) -> Result<u64, Error> {
        let mut guard = self.writer();
        let conn = self.connection(&mut guard)?;
        projections::projection_version(conn, projection)
    }

    /// Get one projection entry by key.
    pub fn projection_get(&self, projection: &str, key: &[u8]) -> Result<Option<Vec<u8>>, Error> {
        let mut guard = self.writer();
        let conn = self.connection(&mut guard)?;
        projections::projection_get(conn, projection, key)
    }

    /// Scan entries with keys starting with `prefix`, in key order.
    pub fn projection_scan_prefix(
        &self,
        projection: &str,
        prefix: &[u8],
        limit: usize,
    ) -> Result<Vec<ProjectionEntry>, Error> {
        let mut guard = self.writer();
        let conn = self.connection(&mut guard)?;
        projections::projection_scan_prefix(conn, projection, prefix, limit)
    }

    /// Paginated prefix scan: entries with keys starting with `prefix` and, when
    /// `after` is given, strictly greater than `after`, in key order.
    pub fn projection_scan_prefix_page(
        &self,
        projection: &str,
        prefix: &[u8],
        after: Option<&[u8]>,
        limit: usize,
    ) -> Result<Vec<ProjectionEntry>, Error> {
        let mut guard = self.writer();
        let conn = self.connection(&mut guard)?;
        projections::projection_scan_prefix_page(conn, projection, prefix, after, limit)
    }

    /// Range scan: entries with `start <= key < end` (either bound optional) and,
    /// when `after` is given, strictly greater than `after`, in key order.
    pub fn projection_scan_range(
        &self,
        projection: &str,
        start: Option<&[u8]>,
        end: Option<&[u8]>,
        after: Option<&[u8]>,
        limit: usize,
    ) -> Result<Vec<ProjectionEntry>, Error> {
        let mut guard = self.writer();
        let conn = self.connection(&mut guard)?;
        projections::projection_scan_range(conn, projection, start, end, after, limit)
    }

    /// List all projections and their versions.
    pub fn projections_list(&self) -> Result<Vec<(String, u64)>, Error> {
        let mut guard = self.writer();
        let conn = self.connection(&mut guard)?;
        projections::projections_list(conn)
    }

    /// The number of entries in a projection (0 when it does not exist).
    pub fn projection_entry_count(&self, projection: &str) -> Result<u64, Error> {
        let mut guard = self.writer();
        let conn = self.connection(&mut guard)?;
        projections::projection_entry_count(conn, projection)
    }

    // ----- jobs -----

    /// Claim ready jobs from one queue, performing bounded expired-lease maintenance.
    ///
    /// On an indeterminate COMMIT outcome the writer connection is discarded and
    /// reopened lazily by the next operation (plan §7.4).
    pub fn claim_jobs(&self, request: &ClaimRequest) -> Result<ClaimOutcome, ClaimError> {
        let mut guard = self.writer();
        let conn = self.connection(&mut guard).map_err(ClaimError::Storage)?;
        let result = jobs::claim_jobs(conn, request);
        if matches!(result, Err(ClaimError::Indeterminate(_))) {
            *guard = None;
        }
        result
    }

    /// Durably extend an active lease. Does not increment the attempt counter.
    pub fn extend_lease(
        &self,
        job_id: Id,
        lease_token: Id,
        new_expiry_ms: i64,
        now_ms: i64,
    ) -> Result<LeaseExtensionReceipt, LeaseError> {
        let mut guard = self.writer();
        let conn = self.connection(&mut guard).map_err(LeaseError::Storage)?;
        jobs::extend_lease(conn, job_id, lease_token, new_expiry_ms, now_ms)
    }

    /// Recover the outcome of an indeterminate claim, reconstructing original lease
    /// tokens from durable claim receipts.
    /// Always reopens a fresh connection first when the writer was poisoned by the
    /// failed COMMIT.
    pub fn recover_claim(
        &self,
        transaction_id: Id,
        now_ms: i64,
    ) -> Result<ClaimRecovery, RecoveryError> {
        let mut guard = self.writer();
        let conn = self
            .connection(&mut guard)
            .map_err(RecoveryError::Storage)?;
        jobs::recover_claim(conn, transaction_id, now_ms)
    }

    /// Look up one job by its ID.
    pub fn job(&self, job_id: Id) -> Result<Option<JobInfo>, Error> {
        let mut guard = self.writer();
        let conn = self.connection(&mut guard)?;
        jobs::get_job(conn, job_id)
    }

    /// List jobs, optionally filtered by queue and state, in enqueue order.
    pub fn jobs(
        &self,
        queue: Option<&str>,
        state: Option<JobState>,
        limit: usize,
    ) -> Result<Vec<JobInfo>, Error> {
        let mut guard = self.writer();
        let conn = self.connection(&mut guard)?;
        jobs::list_jobs(conn, queue, state, limit)
    }

    // ----- ops -----

    /// Copy the database to `dest_path` using the SQLite backup API. Refuses an
    /// existing destination unless `overwrite` is set.
    pub fn backup(&self, dest_path: impl AsRef<Path>, overwrite: bool) -> Result<(), Error> {
        let mut guard = self.writer();
        let conn = self.connection(&mut guard)?;
        ops::backup(conn, dest_path.as_ref(), overwrite)
    }

    /// Run integrity, foreign-key, migration-checksum, and semantic checks.
    pub fn verify(&self) -> Result<VerifyReport, Error> {
        let mut guard = self.writer();
        let conn = self.connection(&mut guard)?;
        ops::verify(conn)
    }

    /// Collect store-wide statistics.
    pub fn stats(&self) -> Result<StoreStats, Error> {
        let mut guard = self.writer();
        let conn = self.connection(&mut guard)?;
        ops::stats(conn, &self.path)
    }

    /// Produce a redacted diagnostic export as JSON Lines text.
    pub fn diagnostic_export(&self) -> Result<String, Error> {
        self.diagnostic_export_with(false)
    }

    /// Produce a diagnostic export, optionally including payload bytes. Lease
    /// tokens are never included.
    pub fn diagnostic_export_with(&self, include_payloads: bool) -> Result<String, Error> {
        let mut guard = self.writer();
        let conn = self.connection(&mut guard)?;
        ops::diagnostic_export(conn, &self.path, include_payloads)
    }

    /// Report the status of every known migration against the database.
    pub fn migrations_status(&self) -> Result<Vec<MigrationStatus>, Error> {
        let mut guard = self.writer();
        let conn = self.connection(&mut guard)?;
        migrations::status(conn).map_err(Error::from)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::error::Conflict;
    use crate::event::Event;

    fn open_store(dir: &tempfile::TempDir) -> ControlPlaneStore {
        ControlPlaneStore::open(dir.path().join("db")).unwrap()
    }

    fn event(id: u128, stream: &str, event_type: &str) -> Event {
        Event::with_json_payload(Id::from(id), stream, event_type, 1_000, b"{}")
    }

    #[test]
    fn commit_and_read_events() {
        let dir = tempfile::tempdir().unwrap();
        let store = open_store(&dir);
        let batch = CommitBatch::new(Id::from(1u128), 2_000)
            .append_event(event(10, "s1", "created"))
            .append_event(event(11, "s1", "updated"))
            .append_event(event(12, "s2", "created"));
        let receipt = store.commit(&batch).unwrap();
        assert_eq!(receipt.transaction_sequence, 1);
        assert_eq!(receipt.committed_at_ms, 2_000);

        let all = store.events_after(0, 100).unwrap();
        assert_eq!(all.len(), 3);
        assert_eq!(all[0].global_sequence, 1);
        assert_eq!(all[0].stream_version, 1);
        assert_eq!(all[1].stream_version, 2);
        assert_eq!(all[2].event.stream_id, "s2");

        let after = store.events_after(2, 100).unwrap();
        assert_eq!(after.len(), 1);

        let s1 = store.stream_events("s1", 2, 100).unwrap();
        assert_eq!(s1.len(), 1);
        assert_eq!(s1[0].event.event_type, "updated");

        let found = store.get_event(Id::from(11u128)).unwrap().unwrap();
        assert_eq!(found.event.stream_id, "s1");
        assert!(store.get_event(Id::from(99u128)).unwrap().is_none());

        assert_eq!(store.stream_version("s1").unwrap(), 2);
        assert_eq!(store.stream_version("s2").unwrap(), 1);
        assert_eq!(store.stream_version("missing").unwrap(), 0);
    }

    #[test]
    fn idempotent_resubmission_returns_original_receipt() {
        let dir = tempfile::tempdir().unwrap();
        let store = open_store(&dir);
        let batch = CommitBatch::new(Id::from(1u128), 2_000).append_event(event(10, "s1", "a"));
        let first = store.commit(&batch).unwrap();
        let second = store.commit(&batch).unwrap();
        assert_eq!(first, second);
        assert_eq!(store.events_after(0, 100).unwrap().len(), 1);
    }

    #[test]
    fn duplicate_id_with_different_content_is_rejected() {
        let dir = tempfile::tempdir().unwrap();
        let store = open_store(&dir);
        let batch = CommitBatch::new(Id::from(1u128), 2_000).append_event(event(10, "s1", "a"));
        store.commit(&batch).unwrap();
        let different = CommitBatch::new(Id::from(1u128), 2_000).append_event(event(11, "s1", "b"));
        assert_eq!(
            store.commit(&different).unwrap_err(),
            CommitError::DuplicateIdWithDifferentContent
        );
    }

    #[test]
    fn stream_version_conflict_is_typed() {
        let dir = tempfile::tempdir().unwrap();
        let store = open_store(&dir);
        store
            .commit(&CommitBatch::new(Id::from(1u128), 2_000).append_event(event(10, "s1", "a")))
            .unwrap();
        let conflicting = CommitBatch::new(Id::from(2u128), 2_001)
            .expect_stream_version("s1", 0)
            .append_event(event(11, "s1", "b"));
        assert_eq!(
            store.commit(&conflicting).unwrap_err(),
            CommitError::Conflict(Conflict::StreamVersion {
                stream_id: "s1".into(),
                expected: 0,
                actual: 1,
            })
        );
        // Nothing was persisted by the failed commit.
        assert_eq!(store.events_after(0, 100).unwrap().len(), 1);

        let ok = CommitBatch::new(Id::from(3u128), 2_002)
            .expect_stream_version("s1", 1)
            .append_event(event(12, "s1", "b"));
        assert_eq!(store.commit(&ok).unwrap().transaction_sequence, 2);
    }

    #[test]
    fn poisoned_writer_connection_is_reopened_before_the_next_operation() {
        let dir = tempfile::tempdir().unwrap();
        let store = open_store(&dir);
        let batch = CommitBatch::new(Id::from(1u128), 2_000).append_event(event(10, "s1", "a"));
        let receipt = store.commit(&batch).unwrap();

        // Simulate the poisoning performed after an indeterminate COMMIT outcome.
        *store.writer() = None;
        assert_eq!(
            store.recover_transaction(Id::from(1u128)).unwrap(),
            TransactionRecovery::Committed(receipt)
        );

        // Writes also reopen the connection after poisoning.
        *store.writer() = None;
        let next = CommitBatch::new(Id::from(2u128), 2_001).append_event(event(11, "s1", "b"));
        assert_eq!(store.commit(&next).unwrap().transaction_sequence, 2);
    }

    #[test]
    fn recover_transaction_reports_committed_or_absent() {
        let dir = tempfile::tempdir().unwrap();
        let store = open_store(&dir);
        let batch = CommitBatch::new(Id::from(1u128), 2_000).append_event(event(10, "s1", "a"));
        let receipt = store.commit(&batch).unwrap();
        assert_eq!(
            store.recover_transaction(Id::from(1u128)).unwrap(),
            TransactionRecovery::Committed(receipt)
        );
        assert_eq!(
            store.recover_transaction(Id::from(9u128)).unwrap(),
            TransactionRecovery::Absent
        );
    }

    #[test]
    fn validation_rejects_zero_ids_and_oversized_payloads() {
        let dir = tempfile::tempdir().unwrap();
        let store = ControlPlaneStore::builder(dir.path().join("db"))
            .limits(Limits {
                max_event_payload: 4,
                ..Limits::new()
            })
            .open()
            .unwrap();
        let zero_txn = CommitBatch::new(Id::ZERO, 0);
        assert!(matches!(
            store.commit(&zero_txn).unwrap_err(),
            CommitError::Validation(_)
        ));
        let too_big = CommitBatch::new(Id::from(1u128), 0).append_event(Event::with_json_payload(
            Id::from(2u128),
            "s1",
            "t",
            0,
            b"12345",
        ));
        assert!(matches!(
            store.commit(&too_big).unwrap_err(),
            CommitError::Validation(_)
        ));
    }
}
