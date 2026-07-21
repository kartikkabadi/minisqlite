//! Operational tooling: backup, verify, stats, diagnostic export. Phase A stubs:
//! signatures are final, bodies return [`Error::Unimplemented`] until the ops
//! subsystem lands.

use std::collections::BTreeMap;
use std::path::Path;

use rusqlite::Connection;

use crate::error::Error;
use crate::jobs::JobState;

/// A single finding from [`verify`]: which check failed and a human-readable detail.
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

/// Copy the database to `dest_path` using the SQLite backup API. Refuses an existing
/// destination unless `overwrite` is set.
pub(crate) fn backup(_conn: &Connection, _dest_path: &Path, _overwrite: bool) -> Result<(), Error> {
    Err(Error::Unimplemented("ops: backup"))
}

/// Run integrity, foreign-key, migration-checksum, and semantic checks.
pub(crate) fn verify(_conn: &Connection) -> Result<VerifyReport, Error> {
    Err(Error::Unimplemented("ops: verify"))
}

/// Collect store-wide statistics.
pub(crate) fn stats(_conn: &Connection, _db_path: &Path) -> Result<StoreStats, Error> {
    Err(Error::Unimplemented("ops: stats"))
}

/// Produce a redacted diagnostic export (schema, stats, verification findings) as text.
pub(crate) fn diagnostic_export(_conn: &Connection, _db_path: &Path) -> Result<String, Error> {
    Err(Error::Unimplemented("ops: diagnostic_export"))
}

/// Count jobs per state; helper shared by stats and the CLI once implemented.
#[allow(dead_code)]
pub(crate) fn jobs_by_state(_conn: &Connection) -> Result<BTreeMap<JobState, u64>, Error> {
    Err(Error::Unimplemented("ops: jobs_by_state"))
}
