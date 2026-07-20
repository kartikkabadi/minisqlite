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
- Added `tests/invalid_job_transitions.rs` and `tests/projection_ops.rs`.
- `Event::with_json_payload` now requires caller-supplied `occurred_at_ms`; no hidden clock.
- `Store` flushes on `Drop`; removed dead `JobInternalState::Uncertain` variant; optimized projection replace no-op detection.
- `Store` uses `RwLock` so reads can run concurrently while writes remain serialized.
- Lease tokens are generated with `Id::new()` so they are not reused across process restarts.
- Recovery replay no longer re-runs `Limits` validation against configured values; the hard frame-size bound is the recovery guard.
- `DataFile::sync` now respects the `Memory` durability mode.
- `ops_to_records` now simulates job-state transitions within a batch so `LeaseJob` followed by `FailJob` in one atomic commit uses the updated attempt count.
- `Store::jobs` now returns a `JobInfo` snapshot with `attempt`, `worker_id`, `lease_expires_at_ms`, `retry_after_ms`, and `terminal_at_ms`.
- CLI `jobs list` and `export` include the new `JobInfo` fields.
- `fail_job` normalizes an explicit retry time equal to the default (`now_ms + 1000`) and stores the effective retry time on disk, so idempotent re-commits round-trip.
- `JobInfo` omits `worker_id`, `lease_expires_at_ms`, and `retry_after_ms` for terminal jobs.
- `max_attempts == 0` is rejected at validation time.
- Added transaction-level `correlation_id` and `metadata` via `CommitBatch::with_correlation_id` and `with_metadata`, persisted as the first `TransactionMeta` record in a frame and returned on `CommitReceipt` and `get_transaction`.
- Added `tests/integration.rs` round-trip test for transaction-level metadata.
- Refactored job state transitions into `JobStateRecord` methods (`lease`, `acknowledge`, `fail`, `cancel`, `resolve`) so validation, record encoding, and recovery replay all share one state machine.
- Recovery replay now fails closed when a committed job record references a missing job, has a stale lease token, or has an inconsistent terminal flag.
- Refactored projection operations (`put`, `delete`, `clear`, `replace`, prefix/range scans) into `ProjectionState` methods so `store.rs` no longer duplicates the BTreeMap logic.
- Removed the `projections get` CLI subcommand; the spec only requires `projections list/scan`, and `Store::get_projection` is still available in the library.
- `claim_jobs` now enforces partition ordering by claiming at most one ready job per partition per call.
- `Record::JobFail` now stores and validates the attempt count on disk.
- `Store::backup` fsyncs the destination parent directory on Unix after the atomic rename.
- `Store::apply_commit` applies the staged delta before inserting the batch into the idempotency index so a failure cannot leave a receiptless batch.
- `PersistedEvent::frame_offset` is now `pub(crate)` and no longer emitted in JSON CLI output, so internal file offsets are not exposed as stable public identifiers.
- `README.md` install instructions now reference building from `feat/control-plane-state-engine` instead of `crates.io`, because `v0.3.0-alpha.1` is not yet published.
- Removed the last avoidable `unwrap` in the CLI JSON stats path.
- Reviewed Socket Security alerts for `cargo/libc@0.2.186` and `cargo/zerocopy@0.8.54` and documented the triage in `docs/SECURITY.md` and `docs/DEPENDENCIES.md`; both are well-known transitive dependencies and the obfuscation alerts are false positives.
- Added `tests/security.rs` (symlink rejection and owner-only file permissions on Unix) and `tests/limits.rs` (bounds and validation tests).
- Refreshed `docs/PERFORMANCE.md` numbers from a release benchmark run and updated `docs/FINAL_REPORT.md` with latest fuzz counts and test coverage.

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
