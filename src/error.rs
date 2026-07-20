use std::fmt;
use std::io;

use crate::id::Id;

/// The error type for all MiniSQLite operations.
#[derive(Debug, Clone, PartialEq)]
pub enum Error {
    /// An unexpected I/O error occurred before any durable state could have changed.
    Io(String),

    /// The file does not begin with the MiniSQLite magic bytes.
    NotMiniSQLite,

    /// The file has the MiniSQLite magic but a major format version this build cannot read.
    UnsupportedVersion { major: u16, minor: u16 },

    /// The file header or a frame checksum does not match.
    Corruption { message: String, offset: u64 },

    /// The store is already owned by another process.
    AlreadyOpen,

    /// The lock could not be acquired.
    LockUnavailable,

    /// A store operation was attempted after a poisoned commit.
    StorePoisoned { transaction_id: Id },

    /// A validation check failed before any durable state was changed.
    Validation(String),

    /// A truncation (repair) may or may not have become durable. Reopen to verify.
    RepairOutcomeUncertain { requested: u64, actual: u64 },

    /// An event or projection stream version conflict.
    Conflict {
        stream_id: String,
        expected: u64,
        actual: u64,
    },

    /// A transaction ID was reused with different content.
    DuplicateIdWithDifferentContent { kind: &'static str, id: Id },

    /// An event ID already exists with different content.
    DuplicateEventId(Id),

    /// A job ID was enqueued again with different content.
    DuplicateJobId(Id),

    /// The supplied lease token is not the current lease for this job.
    InvalidLease { job_id: Id },

    /// The requested job does not exist.
    JobNotFound(Id),

    /// A projection operation was supplied with an incompatible version.
    ProjectionVersionMismatch {
        projection: String,
        current: u64,
        supplied: u64,
    },

    /// The supplied payload, key, value, or metadata exceeded a configured limit.
    PayloadTooLarge {
        kind: &'static str,
        size: usize,
        limit: usize,
    },

    /// A commit may or may not have become durable. The store is poisoned until reopened.
    CommitOutcomeUncertain {
        transaction_id: Id,
        original_file_len: u64,
        source: String,
    },

    /// A projection with this name does not exist.
    ProjectionNotFound(String),

    /// The requested stream does not exist.
    StreamNotFound(String),

    /// The requested event was not found.
    EventNotFound(Id),

    /// The requested transaction was not found.
    TransactionNotFound(Id),

    /// The store was opened with an un-repaired tail and must be repaired before writes.
    StoreNeedsRepair,

    /// An unsupported CLI argument or command was provided.
    Usage(String),
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Error::Io(msg) => write!(f, "io error: {msg}"),
            Error::NotMiniSQLite => write!(f, "file is not a MiniSQLite database"),
            Error::UnsupportedVersion { major, minor } => {
                write!(f, "unsupported format version {major}.{minor}")
            }
            Error::Corruption { message, offset } => {
                write!(f, "corruption at offset {offset}: {message}")
            }
            Error::AlreadyOpen => write!(f, "store is already open in another process"),
            Error::LockUnavailable => write!(f, "could not acquire exclusive file lock"),
            Error::StorePoisoned { transaction_id } => {
                write!(f, "store poisoned by transaction {transaction_id}; reopen to verify")
            }
            Error::Validation(msg) => write!(f, "validation error: {msg}"),
            Error::RepairOutcomeUncertain { requested, actual } => write!(
                f,
                "repair outcome uncertain: requested truncate to {requested}, actual file length {actual}; reopen to verify"
            ),
            Error::Conflict {
                stream_id,
                expected,
                actual,
            } => write!(
                f,
                "stream version conflict for {stream_id}: expected {expected}, actual {actual}"
            ),
            Error::DuplicateIdWithDifferentContent { kind, id } => {
                write!(f, "{kind} {id} was used with different content")
            }
            Error::DuplicateEventId(id) => write!(f, "event {id} already exists"),
            Error::DuplicateJobId(id) => write!(f, "job {id} was enqueued with different content"),
            Error::InvalidLease { job_id } => write!(f, "invalid lease for job {job_id}"),
            Error::JobNotFound(id) => write!(f, "job {id} not found"),
            Error::ProjectionVersionMismatch {
                projection,
                current,
                supplied,
            } => write!(
                f,
                "projection {projection} version mismatch: current {current}, supplied {supplied}"
            ),
            Error::PayloadTooLarge { kind, size, limit } => write!(
                f,
                "{kind} exceeds limit: {size} > {limit}"
            ),
            Error::CommitOutcomeUncertain {
                transaction_id,
                original_file_len,
                source,
            } => write!(
                f,
                "commit {transaction_id} outcome uncertain; original file length {original_file_len}: {source}"
            ),
            Error::ProjectionNotFound(name) => write!(f, "projection {name} not found"),
            Error::StreamNotFound(id) => write!(f, "stream {id} not found"),
            Error::EventNotFound(id) => write!(f, "event {id} not found"),
            Error::TransactionNotFound(id) => write!(f, "transaction {id} not found"),
            Error::StoreNeedsRepair => write!(f, "store has an un-repaired tail; run repair"),
            Error::Usage(msg) => write!(f, "usage error: {msg}"),
        }
    }
}

impl std::error::Error for Error {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        None
    }
}

impl From<io::Error> for Error {
    fn from(err: io::Error) -> Self {
        Error::Io(err.to_string())
    }
}
