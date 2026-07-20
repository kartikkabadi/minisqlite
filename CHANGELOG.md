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
- Recovery replay no longer re-runs configured `Limits` validation; it enforces immutable invariants (non-zero IDs, `max_attempts > 0`, valid lease tokens, attempt sequence, `lease_expires_at_ms > claimed_at_ms`) and uses the hard `MAX_RECORDS_PER_FRAME` bound to avoid unbounded allocation.
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
- Added `Record::JobExpire` and `Op::InternalExpireJob` so expired final-attempt job maintenance is a fixed-size record independent of `max_summary_len` and `max_frame_size`.
- `claim_jobs` now builds one atomic `CommitBatch` for all maintenance and lease ops, with explicit byte and record budgeting under `Limits`.
- `StoreBuilder::verify()` performs a read-only scan without locking or modifying the file; `Store::repair()` is the explicit write path that truncates a torn tail.
- `open_existing` no longer initializes a zero-byte file or truncates a torn tail; it sets `needs_repair` and blocks writes until repair.
- `Store::backup` refuses an existing destination, copies only the durable valid prefix, and scans the temporary copy before the atomic rename.
- `O_NOFOLLOW` is now sourced from the audited `libc` crate instead of a hand-copied constant.
- `Limits::validate` enforces that `max_frame_size` can hold at least one `JobExpire` record plus frame overhead.
- Added a pinned Rust 1.89 MSRV CI lane in `.github/workflows/ci.yml`.
- Added adversarial regression tests in `tests/review6.rs` and `tests/review6_fuzz.rs` covering all Review #6 findings.
- Replaced `fs2` advisory locking with `std::fs::File::lock`/`try_lock` (Rust 1.89+), removing the `fs2` dependency.
- Replaced `proptest`/`tempfile` with `fastrand` and a small `tests/common/mod.rs` `TempDir` helper, removing the `rand`/`getrandom`/`ppv-lite86`/`zerocopy` dev-dependency subtree and the Socket Security `zerocopy` alert.
- Removed the `fuzz/` crate's `libfuzzer-sys` build dependency and folded the same coverage into deterministic `#[test]` fuzz targets in `tests/fuzz_targets.rs`.
- `O_NOFOLLOW` is sourced from the audited `libc` crate; `Cargo.lock` contains `libc` but no `zerocopy`; `docs/SECURITY.md` and `docs/DEPENDENCIES.md` updated accordingly.
- Added `tests/security.rs` (symlink rejection and owner-only file permissions on Unix) and `tests/limits.rs` (bounds and validation tests).
- Refreshed `docs/PERFORMANCE.md` numbers from a release benchmark run and updated `docs/FINAL_REPORT.md` with latest fuzz counts and test coverage.
- `Limits::validate` now rejects `max_records_per_transaction` above the hard `MAX_RECORDS_PER_FRAME` ceiling; `Store::commit` enforces the same ceiling before writing.
- `record::decode_records` bounds allocation by payload geometry before reserving, rejecting `expected_count` that cannot fit.
- `StoreBuilder::verify` and `Store::verify` replay every frame through the full semantic validation path and return `StoreNeedsRepair` for structurally torn tails.
- `Store::claim_jobs` budgets maintenance and candidate leases using exact encoded record sizes and progresses per partition, eliminating previous starvation and 1-byte over-estimate.
- `JobStateRecord::expire` rejects non-`Idempotent` `EffectMode`.
- `Store::backup` uses `hard_link` + `remove_file` for atomic no-replace publication, so dangling symlinks and destination races cannot overwrite an existing backup.
- `DataFile::truncate` reports `RepairOutcomeUncertain` when `fsync` after `set_len` fails, with requested and actual file length.
- `StoreInner::replay_frame` wraps all semantic reconstruction/validation failures as `Error::Corruption { offset }` carrying the offending frame offset.
- Synchronized `docs/RECOVERY.md`, `docs/ARCHITECTURE.md`, and `docs/INVARIANTS.md` with the structural-vs-semantic failure distinction and verify contract.
- Added `tests/review7.rs` adversarial regression tests covering all ten Review #7 findings.
- Added hard format ceiling `MAX_REPLACE_ENTRIES_PER_RECORD = 1_000_000` in `record.rs`; `Limits::max_replace_entries` is validated against it and `ProjectionReplace` decoding checks the count before allocation, using `try_reserve_exact` bounded by payload geometry.
- `Store::claim_jobs` now returns `ClaimOutcome::Committed`/`Uncertain` so callers recover the proposed transaction ID and `ClaimedJob` values (including lease tokens) after an uncertain commit.
- `Store::backup` refuses a poisoned store and reports post-link / post-publication / parent-sync failures as `BackupOutcomeUncertain`.
- `storage::file::rename_no_replace` is stage-aware and supports `backup-after-link` and `backup-after-publication` failpoints.
- `DataFile::truncate` has a `truncate-sync-error` failpoint in the `Strict` sync block; the `RepairOutcomeUncertain` path is exercised under `Durability::Strict`.
- Duplicate `EnqueueJob` operations in `validate_job_ops`/`ops_to_records` are treated consistently, preserving existing/simulated lease state for identical enqueues.
- `examples/benchmark.rs` uses a random-suffixed directory created with `std::fs::create_dir` and removes only the directory this invocation creates.
- Documented strict lexicographic partition ordering (no round-robin fairness) in `docs/JOBS.md` and `docs/INVARIANTS.md`.
- Added `tests/review8.rs` adversarial regression tests covering all Review #8 findings.

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
