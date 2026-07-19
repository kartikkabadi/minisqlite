# Changelog

## 0.3.0-alpha.1

- Rewrote `minisqlite` as an append-only control-plane state engine.
- New `MINISQL3` file format with 64-byte file header, 64-byte transaction frame header, and 32-byte frame trailer, CRC32 protected.
- Public API: `Store`, `StoreBuilder`, `CommitBatch`, `CommitReceipt`, `Event`, `PersistedEvent`, `ProjectionEntry`, `JobSpec`, `ClaimRequest`, `ClaimedJob`, `JobState`, `Resolution`, `Durability`, `EffectMode`, `Limits`.
- Atomically commit events, projection operations, and job operations in one transaction frame.
- Durable jobs with lease tokens, partition ordering, retries, dead-letter, and explicit uncertain-outcome resolution.
- Recovery scanner with safe tail truncation and hard failure on mid-file corruption.
- Operational CLI: `doctor`, `verify`, `stats`, `events`, `projections`, `jobs`, `export`, `backup`.
- `examples/synara_control_plane.rs` demonstrating the six required Synara-shaped flows.
- New documentation in `docs/` and an updated `README.md`.

## 0.2.1

- Polished public API and library documentation
- Fixed all `cargo clippy` warnings and applied `cargo fmt`
- Added `hex_encode` helper for consistent blob rendering
- Improved README with crates.io badges, install instructions, and product positioning

## 0.2.0

- Initial public release of `minisqlite`
- Page-based storage with a custom `MiniSQL2` file format
- SQL parser and executor supporting DDL, DML, `SELECT`, joins, aggregates, transactions, `PRAGMA`, and dot commands
- Pure Rust, zero external dependencies
- Library + CLI crate structure
- Examples and integration tests
