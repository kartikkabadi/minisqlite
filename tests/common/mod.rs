//! Shared helpers for the contract test suite. Tests exercise only the public
//! `ControlPlaneStore` API against fresh file-backed temporary databases.
#![allow(dead_code)]

use std::path::{Path, PathBuf};

use minisqlite::{CommitBatch, ControlPlaneStore, Event, Id};
use tempfile::TempDir;

/// Create a fresh temporary directory for a file-backed database.
pub fn temp_dir() -> TempDir {
    tempfile::tempdir().expect("create temp dir")
}

/// The database path inside a temp dir.
pub fn db_path(dir: &TempDir) -> PathBuf {
    dir.path().join("contract.db")
}

/// Open a store at `path` with default configuration.
pub fn open(path: &Path) -> ControlPlaneStore {
    ControlPlaneStore::open(path).expect("open store")
}

/// Open a fresh store inside `dir`.
pub fn open_in(dir: &TempDir) -> ControlPlaneStore {
    open(&db_path(dir))
}

/// A deterministic small ID for tests.
pub fn id(n: u128) -> Id {
    Id::from(n)
}

/// A minimal event with a deterministic ID.
pub fn event(event_id: u128, stream: &str, event_type: &str) -> Event {
    Event::with_json_payload(id(event_id), stream, event_type, 1_000, b"{}")
}

/// A batch containing a single event.
pub fn single_event_batch(txn_id: u128, event_id: u128, stream: &str) -> CommitBatch {
    CommitBatch::new(id(txn_id), 2_000).append_event(event(event_id, stream, "test"))
}

/// Hand-rolled xorshift64* PRNG for deterministic pseudo-random tests (no deps).
pub struct Prng(u64);

impl Prng {
    pub fn new(seed: u64) -> Self {
        Self(seed.max(1))
    }

    pub fn next_u64(&mut self) -> u64 {
        let mut x = self.0;
        x ^= x << 13;
        x ^= x >> 7;
        x ^= x << 17;
        self.0 = x;
        x.wrapping_mul(0x2545_F491_4F6C_DD1D)
    }

    /// Uniform-ish value in `0..n` (n must be > 0).
    pub fn below(&mut self, n: u64) -> u64 {
        self.next_u64() % n
    }
}
