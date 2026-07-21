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
//!
//! ## Recovery surface
//!
//! Indeterminate outcomes are recovered with two targeted APIs rather than a
//! single "verify after reopen" call: [`ControlPlaneStore::recover_transaction`]
//! answers whether a specific commit persisted, and
//! [`ControlPlaneStore::recover_claim`] reconstructs the lease tokens of a claim
//! whose outcome was unknown. Both take the transaction ID the caller already
//! holds, so recovery works from any connection at any later time — including
//! after a process restart — without depending on the state of the writer that
//! observed the failure.
#![forbid(unsafe_code)]

mod config;
mod error;
mod event;
mod id;
mod jobs;
mod projection;
mod sha256;
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
#[cfg(feature = "failpoints")]
pub use store::failpoints;
pub use store::{
    ControlPlaneStore, MigrationStatus, StoreBuilder, StoreStats, VerifyFinding, VerifyReport,
};
pub use transaction::{
    CommitBatch, CommitReceipt, ExpectedStreamVersion, Operation, TransactionRecovery,
};
