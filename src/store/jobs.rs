//! Job apply, claim, lease, and read functions. Phase A stubs: signatures are final,
//! bodies return unimplemented errors until the jobs subsystem lands.

use rusqlite::{Connection, Transaction};

use crate::error::{ClaimError, Error, LeaseError, RecoveryError};
use crate::id::Id;
use crate::jobs::{
    ClaimOutcome, ClaimRecovery, ClaimRequest, JobAck, JobCancellation, JobInfo, JobResolution,
    JobSpec, JobState, LeaseExtensionReceipt,
};

/// Insert a newly enqueued job row (and maintain `active_partitions`) inside the
/// commit transaction.
pub(crate) fn apply_enqueue(
    _tx: &Transaction<'_>,
    _transaction_id: Id,
    _spec: &JobSpec,
) -> Result<(), Error> {
    Err(Error::Unimplemented("jobs: apply_enqueue"))
}

/// Acknowledge a leased job inside the commit transaction.
pub(crate) fn apply_ack(
    _tx: &Transaction<'_>,
    _transaction_id: Id,
    _now_ms: i64,
    _ack: &JobAck,
) -> Result<(), Error> {
    Err(Error::Unimplemented("jobs: apply_ack"))
}

/// Record a job failure (retry or dead) inside the commit transaction.
pub(crate) fn apply_fail(
    _tx: &Transaction<'_>,
    _transaction_id: Id,
    _now_ms: i64,
    _failure: &crate::jobs::JobFailure,
) -> Result<(), Error> {
    Err(Error::Unimplemented("jobs: apply_fail"))
}

/// Cancel a job inside the commit transaction.
pub(crate) fn apply_cancel(
    _tx: &Transaction<'_>,
    _transaction_id: Id,
    _now_ms: i64,
    _cancellation: &JobCancellation,
) -> Result<(), Error> {
    Err(Error::Unimplemented("jobs: apply_cancel"))
}

/// Resolve an uncertain job inside the commit transaction.
pub(crate) fn apply_resolve(
    _tx: &Transaction<'_>,
    _transaction_id: Id,
    _now_ms: i64,
    _resolution: &JobResolution,
) -> Result<(), Error> {
    Err(Error::Unimplemented("jobs: apply_resolve"))
}

/// Run the full claim algorithm: expired-lease maintenance, round-robin partition
/// selection, lease grants, claim receipts, and cursor advancement — all in one
/// `BEGIN IMMEDIATE` transaction.
pub(crate) fn claim_jobs(
    _conn: &mut Connection,
    _request: &ClaimRequest,
) -> Result<ClaimOutcome, ClaimError> {
    Err(ClaimError::Unimplemented("jobs: claim_jobs"))
}

/// Durably extend an active lease.
pub(crate) fn extend_lease(
    _conn: &mut Connection,
    _job_id: Id,
    _lease_token: Id,
    _new_expiry_ms: i64,
    _now_ms: i64,
) -> Result<LeaseExtensionReceipt, LeaseError> {
    Err(LeaseError::Unimplemented("jobs: extend_lease"))
}

/// Recover an indeterminate claim, reconstructing original lease tokens from
/// `claim_receipts`.
pub(crate) fn recover_claim(
    _conn: &Connection,
    _transaction_id: Id,
) -> Result<ClaimRecovery, RecoveryError> {
    Err(RecoveryError::Unimplemented("jobs: recover_claim"))
}

/// Look up one job by its ID.
pub(crate) fn get_job(_conn: &Connection, _job_id: Id) -> Result<Option<JobInfo>, Error> {
    Err(Error::Unimplemented("jobs: get_job"))
}

/// List jobs, optionally filtered by queue and state, in enqueue order.
pub(crate) fn list_jobs(
    _conn: &Connection,
    _queue: Option<&str>,
    _state: Option<JobState>,
    _limit: usize,
) -> Result<Vec<JobInfo>, Error> {
    Err(Error::Unimplemented("jobs: list_jobs"))
}
