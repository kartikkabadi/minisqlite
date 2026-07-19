//! `minisqlite` is an embedded append-only control-plane state engine for local-first AI
//! applications. It is not a SQL database.
//!
//! It provides the primitives a local AI application needs to be durable across restarts:
//!
//! * Atomic commit of domain events
//! * Materialized named-map projections
//! * Durable jobs with leases, retries, and uncertain outcomes
//! * Single-process ownership of one primary data file
//! * Explicit crash recovery
//!
//! # Example
//!
//! ```rust,no_run
//! use minisqlite::{CommitBatch, Durability, Event, StoreBuilder};
//!
//! let store = StoreBuilder::new("app.mini")
//!     .durability(Durability::Strict)
//!     .open()
//!     .unwrap();
//!
//! let tx = minisqlite::Id::new();
//! let event = Event::with_json_payload(
//!     minisqlite::Id::new(),
//!     "thread:abc",
//!     "thread.created",
//!     b"{}",
//! );
//! store.commit(CommitBatch::new(tx, 0).append_event(event)).unwrap();
//! ```

pub(crate) mod codec;
pub(crate) mod storage;

pub mod config;
pub mod error;
pub mod event;
pub mod id;
pub mod jobs;
pub mod projection;
pub mod store;
pub mod transaction;

pub use config::{Durability, EffectMode, Limits};
pub use error::Error;
pub use event::{Event, PersistedEvent, StreamVersion};
pub use id::{Id, InvalidId};
pub use jobs::{ClaimRequest, ClaimedJob, JobSpec, JobState, Resolution};
pub use projection::ProjectionEntry;
pub use store::{Store, StoreBuilder, StoreStats};
pub use transaction::{CommitBatch, CommitReceipt};

pub type Result<T> = std::result::Result<T, Error>;
