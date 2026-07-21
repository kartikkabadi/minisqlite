//! # minisqlite
//!
//! A typed embedded control-plane state kernel on SQLite.
//!
//! One atomic transaction coordinates four concerns that control planes otherwise
//! stitch together by hand:
//!
//! - **Domain events**, appended to versioned streams with optimistic concurrency.
//! - **Materialized projections**, patched with strict version increments.
//! - **Durable jobs**, with partition-ordered claiming, leases, and retries.
//! - **Honest uncertainty**: outcomes that may or may not have persisted are
//!   reported as indeterminate and recovered explicitly, never guessed.
//!
//! Open a store with [`ControlPlaneStore::open`] or [`StoreBuilder`], build a
//! [`CommitBatch`], and call [`ControlPlaneStore::commit`].
#![forbid(unsafe_code)]

mod config;
mod error;
mod event;
mod id;
mod jobs;
mod projection;
mod store;
mod transaction;

pub use config::{Durability, EffectMode, Limits};
pub use error::{
    ClaimError, CommitError, Conflict, Error, IndeterminateCommit, LeaseError, RecoveryError,
    StorageError, ValidationError,
};
pub use event::{Event, PersistedEvent, StreamVersion};
pub use id::{Id, InvalidId};
pub use jobs::{
    ClaimOutcome, ClaimRecovery, ClaimRequest, ClaimedJob, CommittedClaims, IndeterminateClaim,
    JobAck, JobCancellation, JobFailure, JobInfo, JobResolution, JobSpec, JobState, LeaseExtension,
    LeaseExtensionReceipt, MaintenanceReceipt, Resolution,
};
pub use projection::{ProjectionEntry, ProjectionMutation, ProjectionPatch};
pub use store::{
    ControlPlaneStore, MigrationStatus, StoreBuilder, StoreStats, VerifyFinding, VerifyReport,
};
pub use transaction::{
    CommitBatch, CommitReceipt, ExpectedStreamVersion, Operation, TransactionRecovery,
};
