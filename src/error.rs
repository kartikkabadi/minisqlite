use std::fmt;

use crate::id::Id;
use crate::jobs::JobState;

/// The top-level error type for general store operations.
#[derive(Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub enum Error {
    /// A storage-layer failure.
    Storage(StorageError),
    /// A validation check failed before any durable state was changed.
    Validation(ValidationError),
    /// A concurrency or state conflict.
    Conflict(Conflict),
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Error::Storage(e) => write!(f, "{e}"),
            Error::Validation(e) => write!(f, "{e}"),
            Error::Conflict(e) => write!(f, "{e}"),
        }
    }
}

impl std::error::Error for Error {}

impl From<StorageError> for Error {
    fn from(e: StorageError) -> Self {
        Error::Storage(e)
    }
}

impl From<ValidationError> for Error {
    fn from(e: ValidationError) -> Self {
        Error::Validation(e)
    }
}

impl From<Conflict> for Error {
    fn from(e: Conflict) -> Self {
        Error::Conflict(e)
    }
}

/// A failure in the underlying SQLite storage layer or the host filesystem.
#[derive(Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub enum StorageError {
    /// An I/O error from the host filesystem.
    Io(String),
    /// An error reported by SQLite.
    Sqlite(String),
    /// A migration row's stored checksum does not match this build's migration SQL.
    MigrationChecksumMismatch { version: u32 },
    /// The database schema version is newer than this build understands.
    SchemaTooNew { version: u32, supported: u32 },
}

impl fmt::Display for StorageError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            StorageError::Io(msg) => write!(f, "io error: {msg}"),
            StorageError::Sqlite(msg) => write!(f, "sqlite error: {msg}"),
            StorageError::MigrationChecksumMismatch { version } => {
                write!(f, "migration {version} checksum mismatch")
            }
            StorageError::SchemaTooNew { version, supported } => {
                write!(
                    f,
                    "schema version {version} is newer than supported {supported}"
                )
            }
        }
    }
}

impl std::error::Error for StorageError {}

impl StorageError {
    /// Crate-private conversion from the underlying SQLite driver error. Kept out
    /// of the public API so no rusqlite types leak into the crate's contract.
    pub(crate) fn from_sqlite(e: rusqlite::Error) -> Self {
        StorageError::Sqlite(e.to_string())
    }
}

/// A validation check failed before any durable state was changed.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ValidationError(pub String);

impl fmt::Display for ValidationError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "validation error: {}", self.0)
    }
}

impl std::error::Error for ValidationError {}

/// A concurrency or state conflict detected against durable state.
#[derive(Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub enum Conflict {
    /// An expected stream version did not match the durable stream version.
    StreamVersion {
        stream_id: String,
        expected: u64,
        actual: u64,
    },
    /// A projection patch's expected version did not match the durable projection version.
    ProjectionVersion {
        projection: String,
        expected: u64,
        actual: u64,
    },
    /// A job operation requested an invalid state transition.
    JobTransition {
        job_id: Id,
        from: JobState,
        to: JobState,
    },
}

impl fmt::Display for Conflict {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Conflict::StreamVersion {
                stream_id,
                expected,
                actual,
            } => write!(
                f,
                "stream version conflict for {stream_id}: expected {expected}, actual {actual}"
            ),
            Conflict::ProjectionVersion {
                projection,
                expected,
                actual,
            } => write!(
                f,
                "projection version conflict for {projection}: expected {expected}, actual {actual}"
            ),
            Conflict::JobTransition { job_id, from, to } => {
                write!(f, "invalid job transition for {job_id}: {from:?} -> {to:?}")
            }
        }
    }
}

impl std::error::Error for Conflict {}

/// A commit may or may not have become durable. Recover with
/// [`ControlPlaneStore::recover_transaction`](crate::ControlPlaneStore::recover_transaction).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct IndeterminateCommit {
    pub(crate) transaction_id: Id,
    pub(crate) storage_error: String,
}

impl IndeterminateCommit {
    /// The transaction ID whose durability is unknown.
    pub fn transaction_id(&self) -> Id {
        self.transaction_id
    }

    /// The underlying storage failure reported by the COMMIT step.
    pub fn storage_error(&self) -> &str {
        &self.storage_error
    }
}

/// Errors returned by [`ControlPlaneStore::commit`](crate::ControlPlaneStore::commit).
#[derive(Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub enum CommitError {
    /// A concurrency or state conflict; nothing was persisted.
    Conflict(Conflict),
    /// The batch failed static validation; nothing was persisted.
    Validation(ValidationError),
    /// The transaction ID was previously committed with different content.
    DuplicateIdWithDifferentContent,
    /// The commit outcome is unknown; recover with `recover_transaction`.
    Indeterminate(IndeterminateCommit),
    /// A storage failure occurred before the commit step; nothing was persisted.
    Storage(StorageError),
}

impl fmt::Display for CommitError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            CommitError::Conflict(e) => write!(f, "{e}"),
            CommitError::Validation(e) => write!(f, "{e}"),
            CommitError::DuplicateIdWithDifferentContent => {
                write!(f, "transaction id reused with different content")
            }
            CommitError::Indeterminate(i) => write!(
                f,
                "commit {} outcome indeterminate; recover_transaction to verify",
                i.transaction_id
            ),
            CommitError::Storage(e) => write!(f, "{e}"),
        }
    }
}

impl std::error::Error for CommitError {}

impl From<Conflict> for CommitError {
    fn from(e: Conflict) -> Self {
        CommitError::Conflict(e)
    }
}

impl From<ValidationError> for CommitError {
    fn from(e: ValidationError) -> Self {
        CommitError::Validation(e)
    }
}

impl From<StorageError> for CommitError {
    fn from(e: StorageError) -> Self {
        CommitError::Storage(e)
    }
}

impl From<Error> for CommitError {
    fn from(e: Error) -> Self {
        match e {
            Error::Storage(s) => CommitError::Storage(s),
            Error::Validation(v) => CommitError::Validation(v),
            Error::Conflict(c) => CommitError::Conflict(c),
        }
    }
}

/// Errors returned by [`ControlPlaneStore::claim_jobs`](crate::ControlPlaneStore::claim_jobs).
#[derive(Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub enum ClaimError {
    /// The claim's durable outcome is unknown; recover with `recover_claim`.
    Indeterminate(crate::jobs::IndeterminateClaim),
    /// The claim request failed static validation; nothing was persisted.
    Validation(ValidationError),
    /// A concurrency or state conflict; nothing was persisted.
    Conflict(Conflict),
    /// A storage failure occurred before the commit step; nothing was persisted.
    Storage(StorageError),
}

impl fmt::Display for ClaimError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            ClaimError::Indeterminate(i) => write!(
                f,
                "claim {} outcome indeterminate; recover_claim to verify",
                i.transaction_id()
            ),
            ClaimError::Validation(e) => write!(f, "{e}"),
            ClaimError::Conflict(e) => write!(f, "{e}"),
            ClaimError::Storage(e) => write!(f, "{e}"),
        }
    }
}

impl std::error::Error for ClaimError {}

/// Errors returned by recovery APIs (`recover_transaction`, `recover_claim`).
#[derive(Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub enum RecoveryError {
    /// A storage failure prevented recovery; retry after the storage layer is healthy.
    Storage(StorageError),
}

impl fmt::Display for RecoveryError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            RecoveryError::Storage(e) => write!(f, "{e}"),
        }
    }
}

impl std::error::Error for RecoveryError {}

impl From<StorageError> for RecoveryError {
    fn from(e: StorageError) -> Self {
        RecoveryError::Storage(e)
    }
}

/// Errors returned by [`ControlPlaneStore::extend_lease`](crate::ControlPlaneStore::extend_lease).
#[derive(Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub enum LeaseError {
    /// The requested job does not exist.
    JobNotFound(Id),
    /// The supplied lease token is not the job's current lease token.
    InvalidToken { job_id: Id },
    /// The job is not in the `Leased` state (terminal jobs are always rejected).
    NotLeased { job_id: Id, state: JobState },
    /// The requested new expiry is not strictly later than the current expiry.
    ExpiryNotLater {
        job_id: Id,
        current_ms: i64,
        requested_ms: i64,
    },
    /// The lease already expired; an expired lease cannot be revived by extension.
    Expired {
        job_id: Id,
        lease_expires_at_ms: i64,
        now_ms: i64,
    },
    /// A storage failure occurred; the extension may not have persisted.
    Storage(StorageError),
}

impl fmt::Display for LeaseError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            LeaseError::JobNotFound(id) => write!(f, "job {id} not found"),
            LeaseError::InvalidToken { job_id } => {
                write!(f, "invalid lease token for job {job_id}")
            }
            LeaseError::NotLeased { job_id, state } => {
                write!(f, "job {job_id} is not leased (state {state:?})")
            }
            LeaseError::ExpiryNotLater {
                job_id,
                current_ms,
                requested_ms,
            } => write!(
                f,
                "job {job_id} lease expiry {requested_ms} is not later than current {current_ms}"
            ),
            LeaseError::Expired {
                job_id,
                lease_expires_at_ms,
                now_ms,
            } => write!(
                f,
                "job {job_id} lease expired at {lease_expires_at_ms} (now {now_ms}); an expired lease cannot be extended"
            ),
            LeaseError::Storage(e) => write!(f, "{e}"),
        }
    }
}

impl std::error::Error for LeaseError {}

impl From<StorageError> for LeaseError {
    fn from(e: StorageError) -> Self {
        LeaseError::Storage(e)
    }
}
