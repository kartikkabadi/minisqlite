# MiniSQLite Control-Plane Engine — Final Build Report

## Outcome

All PR review findings have been addressed and the full verification suite passes. The PR remains open and unmerged per the review instruction.

`minisqlite` has been rebuilt as a from-scratch, append-only control-plane state engine for local-first AI applications. The legacy SQL engine has been deleted and replaced by an original single-file, CRC32-framed journal with atomic transactions, materialized projections, durable jobs with leases/retries/uncertain outcomes, explicit crash recovery, and an operational CLI.

## Branch

`feat/control-plane-state-engine`

## Pull request

https://github.com/kartikkabadi/minisqlite/pull/9

## Product delivered

* Atomic `CommitBatch` of events, projection mutations, and job operations.
* Ordered domain events with global sequence and per-stream version checks.
* Named ordered-map projections with versioned put/delete/clear/replace, prefix/range scans.
* Durable jobs: enqueue, claim with `worker_id` and lease token, ack, fail with retry, cancel, and explicit uncertain-resolution. `Store::jobs` returns a `JobInfo` snapshot with `attempt`, `worker_id`, `lease_expires_at_ms`, `retry_after_ms`, and `terminal_at_ms`.
* `CommitBatch::with_correlation_id` and `with_metadata` persist optional transaction-level context as the first `TransactionMeta` record in a frame; `CommitReceipt` and `get_transaction` return them.
* Strict vs Memory durability modes.
* `MINISQL3` file format with `MINIFRAM` frame headers and `FRAMETRL` trailers, CRC32 via `crc32fast`.
* Recovery scanner that validates frames, truncates torn tails, and fails closed on mid-file corruption.
* Operational CLI: `doctor`, `verify`, `stats`, `events`, `projections`, `jobs`, `export`, `backup`.
* Cross-platform advisory file locking via `std::fs::File::lock`/`try_lock` (Rust 1.89+).
* `examples/synara_control_plane.rs` demonstrating the six required Synara-shaped flows.

## Major deletions

* SQL parser and tokenizer (`src/sql.rs`, `src/executor.rs`).
* B+ tree (`src/btree.rs` / legacy module).
* Pager, WAL, catalog, functions, types (`src/pager.rs`, `src/wal.rs`, `src/catalog.rs`, `src/types.rs`, `src/functions.rs`).
* Old SQL-facing tests and `test.sql`.

## Architecture

* **File format**: 64-byte file header + 64-byte frame header + encoded records + 32-byte trailer. Checksums cover header, payload, and trailer. Hard `MAX_FRAME_SIZE = 64 MiB`.
* **Commit path**: validate batch → check idempotency and stream versions → encode records → append frame → sync (Strict) → apply to in-memory state atomically.
* **Recovery path**: validate header → scan frames sequentially → decode each frame within the hard frame-size bound → rebuild transaction/event/projection/job indexes → truncate incomplete tail. Configured `Limits` do not affect replay of committed frames.
* **Projection model**: in-memory `BTreeMap` keyed by projection name, each holding an ordered `BTreeMap` of keys to values. Versions are monotonic.
* **Job model**: `JobStateRecord` tracks spec, internal state, lease token, attempt, expiry, and retry time. Public `JobState` is derived at query time.
* **Concurrency model**: one process owns the store via an advisory lock. Writes serialize through an `RwLock` write guard; reads take read guards and may run concurrently. No async runtime dependency.

## Guarantees proved

* A `CommitBatch` is visible entirely or not at all.
* Global event sequence and per-stream versions are monotonic.
* Event and transaction IDs are unique; same logical content is idempotent, different content returns a typed conflict.
* Frame-level checksum integrity is enforced; torn trailing frames are truncated; mid-file corruption fails closed.
* Projection version conflicts fail fast and atomic put/delete/clear/replace is visible with its transaction.
* At most one active lease per job; stale tokens cannot ack/fail a newer lease. New IDs are 128 bits from the OS CSPRNG and are not reused across processes or restarts.
* Partition-ordered job claiming survives concurrent callers.
* Idempotent expired leases become reclaimable; non-idempotent expired leases become uncertain and are not silently retried.
* Uncertain outcomes are reported and can be resolved durably.
* Reopen reconstructs identical in-memory state from durable frames, even if the configured `Limits` have changed.
* Transaction-level `correlation_id` and `metadata` survive commit and reopen.
* `CommitReceipt.stream_versions` are deterministically sorted by stream name and stable across process restarts.
* Terminal `JobFail` records are stably idempotent across reopen.
* Expired idempotent job leases stop being reclaimed once `max_attempts` is reached.
* CLI `export --format jsonl` emits valid JSON with hex keys/values, and projection scan JSON preserves arbitrary binary keys.
* Strict creation fsyncs the directory entry on Unix, and the primary file is created with restrictive permissions before any data is written.
* The single-owner lock is held on the primary data file itself; no separate lock file is used.
* Parent directories created by the store are set to `0o700` on Unix; primary files are `0o600`; existing symlinks for the primary path are rejected.
* Reads can run concurrently while writes remain serialized.

## Guarantees not yet proved

* Power-loss durability beyond what the OS, file system, and storage device provide.
* Encryption at rest.
* Exactly-once external side effects; the engine records outcomes and requires idempotency keys or explicit resolution.
* Distributed or multi-process write coordination.
* Correctness under arbitrary large clock jumps (timestamps are caller-supplied).

## Verification

| Command | Result |
|---|---|
| `cargo fmt --all -- --check` | Passed |
| `cargo clippy --all-targets --all-features -- -D warnings` | Passed |
| `cargo test --all-targets --all-features` | Passed |
| `cargo test --doc --all-features` | Passed |
| `cargo package --allow-dirty` | Passed |

CI matrix: `ubuntu-latest`, `macos-latest`, `windows-latest`.

## Crash matrix

Process-level failpoint tests in `tests/crash.rs` cover each boundary. The recovered state is always a valid committed prefix; no partial transaction is visible; earlier committed state is never lost.

| Failpoint | Expected recovered state | Result |
|---|---|---|
| before append | old state; child aborts | Passed |
| partial header | old state; child aborts | Passed |
| during payload | old state; child aborts | Passed |
| after payload | old state; child aborts | Passed |
| after trailer | old or new state (full write without fsync); child aborts | Passed |
| before sync | old or new state (full write without fsync); child aborts | Passed |
| after sync | new state after reopen; child aborts | Passed |
| before memory apply | new state after reopen; child aborts | Passed |
| after memory apply | new state after reopen; child aborts | Passed |
| disk-full short write | old state, commit returns Io error | Passed |
| sync failure | old state, commit returns Io error | Passed |
| rollback failure | old state, commit returns CommitOutcomeUncertain | Passed |

## Fuzzing and property tests

| Target | Result |
|---|---|
| File header decoding (`codec::frame` fastrand) | Passed |
| Frame decoding (`codec::frame` fastrand) | Passed |
| Record decoding (`codec::record` fastrand) | Passed |
| Recovery scanning with random trailing bytes (`storage::recovery` fastrand) | Passed |
| Model-based store comparison (`tests/property.rs` with fastrand) | Passed |
| Job lifecycle property test (`tests/job_property.rs` with fastrand) | Passed |
| CLI end-to-end smoke test (`tests/cli.rs`) | Passed |
| Projection operation tests (`tests/projection_ops.rs`) | Passed |
| Invalid job-transition tests (`tests/invalid_job_transitions.rs`) | Passed |
| Bounds and limit validation (`tests/limits.rs`) | Passed |
| Symlink rejection and file permissions (`tests/security.rs`) | Passed |
| Partition-ordered job claiming (`tests/integration.rs`) | Passed |

## Fuzz targets

The four required fuzz targets are provided as deterministic `#[test]` harnesses in `tests/fuzz_targets.rs`:

| Target | Source | Result (deterministic seeded runs) |
|---|---|---|
| `header_decode` | `tests/fuzz_targets.rs` | Passed (1024 seeds, no panics) |
| `frame_decode` | `tests/fuzz_targets.rs` | Passed (1024 seeds, no panics) |
| `record_decode` | `tests/fuzz_targets.rs` | Passed (1024 seeds, no panics) |
| `recovery_scan` | `tests/fuzz_targets.rs` | Passed (256 seeds, no panics) |

These replaced the `libfuzzer-sys`/`fuzz/` harness to remove `libc` from the build dependency tree while keeping the same decoder coverage.

## Complexity

* Production lines added / deleted in `src/`: approximately **+5,541 / -4,858**.
* Public API items: approximately **70** exported types/methods.
* Direct runtime dependencies: `crc32fast`, `serde` (optional, exact `1.0.229`, default), `serde_json` (optional, default).
* Persistent file types: one primary `.mini` data file. The advisory lock is held on the data file itself, so no separate lock file is created.
* Features removed: SQL, B+ tree, pager, WAL, catalog, query execution, DDL.
* Hardening pass: explicit `occurred_at_ms` in `Event::with_json_payload`, removed dead `JobInternalState::Uncertain` variant, `Store` now flushes on `Drop`, projection replace no longer clones the whole map to detect no-ops, `Store` uses `RwLock` for concurrent reads, IDs are generated from a 128-bit OS CSPRNG (no dependency on counter/clock), recovery no longer re-runs configured `Limits` validation, `DataFile::sync` respects `Memory` durability, `ops_to_records` simulates job-state transitions within a batch, `Store::jobs` returns a `JobInfo` snapshot, `fail_job` normalizes default retry times for clean round-trips, `max_attempts == 0` is rejected, transaction-level `correlation_id`/`metadata` are persisted as the first `TransactionMeta` record, all job transitions (lease/ack/fail/cancel/resolve) are centralized in `JobStateRecord`, projection operations (`put`/`delete`/`clear`/`replace`/scans) are centralized in `ProjectionState`, the CLI `projections get` subcommand was removed because the spec only requires `projections list/scan`, `PersistedEvent::frame_offset` is now `pub(crate)` and is no longer emitted in JSON CLI output so internal file offsets are not exposed as stable public identifiers, `README.md` install instructions now reference building from the feature branch because `v0.3.0-alpha.1` is not yet published, the last avoidable `unwrap` in the CLI JSON stats path was replaced with explicit error handling, Socket Security alerts for `cargo/libc` and `cargo/zerocopy` were resolved by keeping the dependency tree free of those crates (`fs2` replaced by `std::fs::File::lock`, `proptest`/`tempfile` replaced by `fastrand` and a custom `TempDir` helper, `libfuzzer-sys`/`fuzz/` removed and replaced with deterministic `#[test]` fuzz targets, and ID generation uses `/dev/urandom` on Unix and `BCryptGenRandom` on Windows instead of `getrandom`/`libc`), the uncertain-commit recovery test now asserts that reopen leaves the store un-poisoned, the sidecar `.mini.lock` file was deleted and the lock is now held on the primary data file, `tests/security.rs` verifies symlink rejection and owner-only file permissions on Unix, `tests/limits.rs` exercises configured bounds, `claim_jobs` now claims at most one ready job per partition per call so earlier nonterminal jobs block later jobs in the same partition, `Record::JobFail` stores and validates the attempt count, `apply_commit` applies the staged delta before inserting into the idempotency index so a failure cannot leave a receiptless batch, and `Store::backup` fsyncs the destination parent directory on Unix.

## Synara-shaped demonstration

`examples/synara_control_plane.rs` demonstrates:

* **Flow A**: Create a thread and project its initial state; receive global sequence and stream version.
* **Flow B**: Request a provider turn, append `thread.turn-requested`, update the projection to `queued`, and enqueue one provider job partitioned by thread ID in one transaction.
* **Flow C**: Claim the provider job, perform the work, then atomically append `thread.turn-completed`, set the projection to `idle`, and acknowledge the job. Stale lease tokens are rejected.
* **Flow D**:
  * Idempotent effect: a job with an external idempotency key is reclaimed after lease expiry and acknowledged.
  * Non-idempotent effect: a job with `EffectMode::UncertainOnLeaseExpiry` becomes `Uncertain` after expiry, is not silently retried, and is explicitly resolved as succeeded.
* **Flow E**: Schedule a future-dated loop job with `not_before_ms`, close and reopen the store, and claim the job only after `not_before_ms` passes.
* **Flow F**: Read the `thread:abc` event stream, rebuild the `threads` projection from those events, and atomically replace its current contents without a SQL migration.

## Known limitations

* Alpha format; `v0.3.0-alpha.1`.
* Single owning process.
* No encryption at rest.
* No cloud sync, replication, or distributed consensus.
* No multi-process writes.
* No automatic snapshots or compaction.
* No exactly-once external effects; idempotency keys and explicit resolution are required.
* Bounded control-plane data workload; not a general-purpose blob store.
* Not production-ready.

## Final review-fix pass

Per PR comment IDs 4732347323 and 4732434245, the branch was audited against the merge-blocking findings, with focused regression tests added for each and `docs/FINAL_REPORT.md` corrected to match what the tests prove.

* Branch: `feat/control-plane-state-engine` (head SHA is the latest commit on this branch)
* Merge conflict with `main`: none
* Full verification suite (run on the box at `2026-07-20 06:05 UTC`):
  * `cargo fmt --all -- --check` — passed
  * `cargo clippy --all-targets --all-features -- -D warnings` — passed
  * `cargo test --all-targets --all-features` — passed
  * `cargo test --all-targets` (default features) — passed
  * `cargo test --doc --all-features` — passed
  * `cargo package --allow-dirty` — passed
  * `cargo run --example synara_control_plane --release` — passed
  * `cargo run --example benchmark --release` — passed
* Correctness fixes from this pass:
  * `FrameHeader::decode` fails closed on unsupported frame format versions.
  * `Record::decode` enforces record format version, flags, and fully consumed body.
  * `Store::backup` rejects the primary path / filesystem aliases / symlinks, uses a collision-resistant sibling temp file created with `create_new`, fsyncs the temp copy with `Durability::Strict`, and fsyncs the destination parent directory after the atomic `rename`.
  * `ProjectionDelete` on a missing projection now materializes an empty projection at the supplied version.
  * `CommitBatch` rejects exact duplicate event IDs within a single batch.
  * `Store::claim_jobs` rejects non-positive `lease_ms` and uses `checked_add` for `lease_expires_at_ms` and `attempt`.
  * `storage::recovery::scan` only treats a tail as truncated when `file_len - offset < FRAME_HEADER_SIZE`; once enough bytes are reported by metadata, read errors are propagated as I/O errors instead of recovery.
  * `DataFile::read_at` supports an injected `header-read-error` failpoint for testing read-failure paths.
  * `JobStateRecord::fail` uses `checked_add` for the default `retry_after_ms = now_ms + 1000` and returns a validation error on overflow.
  * `CommitBatch::fail_job` no longer pre-normalizes `retry_after_ms`; normalization is performed in the fallible `ops_to_records` / `op_from_record` paths, and `CommitBatch::logical_eq` treats `None` and `Some(now_ms + 1000)` as logically equal.
* Test evidence from this pass:
  * `tests/crash.rs` splits the failpoint matrix by expected recovered state and asserts the child process aborts for abort failpoints.
  * `tests/crash.rs` adds `header_read_error_does_not_truncate_store`, proving a transient frame-header read error does not truncate valid committed data.
  * `tests/integration.rs` adds `backup_rejects_primary_path_and_preserves_store`, `backup_temp_path_cannot_collide_with_primary_file`, and `backup_does_not_remove_preexisting_temp_file`.
  * `tests/integration.rs` adds `fail_job_default_retry_overflow_is_rejected` and `fail_job_explicit_default_retry_is_idempotent_across_reopen`.
  * `tests/integration.rs` adds `duplicate_event_id_in_same_batch_is_rejected_and_idempotent_across_reopen`.
  * `tests/projection_ops.rs` adds `delete_on_missing_projection_materializes_empty_projection`.
  * `tests/invalid_job_transitions.rs` adds `claim_jobs_rejects_non_positive_lease` and `claim_jobs_rejects_lease_arithmetic_overflow`.
* `Cargo.lock` contains no `libc` or `zerocopy`.

## Review #3 P1 fix pass

Per PR comment ID 4732599741, the 13 merge-blocking findings were addressed and focused regression tests were added for each.

* Branch: `feat/control-plane-state-engine` (head `7fc2c03fe0323d0f553665183c691b6497f1a6ce`)
* Merge conflict with `main`: none
* Full verification suite (run on the Devin host at `2026-07-20 10:34 UTC`):
  * `cargo fmt --all -- --check` — passed
  * `cargo clippy --all-targets --all-features -- -D warnings` — passed
  * `cargo test --all-targets --all-features` — passed
  * `cargo test --doc --all-features` — passed
  * `cargo package --allow-dirty` — passed
  * `cargo run --example synara_control_plane --release` — passed
  * `cargo run --example benchmark --release` — passed
* P1 correctness fixes:
  1. Removed caller-selectable lock paths; single-owner advisory locking is now performed directly on the primary data file inside `DataFile`.
  2. `JobStateRecord::is_ready_at` returns `false` when `attempt >= max_attempts`, so expired idempotent leases stop being reclaimed.
  3. `JobStateRecord::fail` materializes the effective `retry_after_ms` in the `JobFail` record and `op_from_record` normalizes it, making terminal `JobFail` idempotent across reopen.
  4. `CommitReceipt.stream_versions` uses `BTreeMap` ordering, producing deterministic ordering across process restarts.
  5. `Id::new()` now reads 128 bits from the OS CSPRNG (`/dev/urandom` on Unix, `BCryptGenRandom` on Windows) and rejects `Id::ZERO`, providing real cross-process/restart uniqueness without adding `libc` or `getrandom` to the dependency tree.
  6. `FileHeader::decode` enforces `header_length` and `flags == 0`; `FrameHeader::decode` rejects unknown frame versions; `replay_frame` compares `records.len()` to `frame.header.record_count`.
  7. CLI `export --format jsonl` builds the JSON document through `serde_json` with hex-encoded binary keys/values, so it is always valid JSON and contains a complete snapshot of events, projections, and jobs.
  8. `DataFile` creates parent directories with `create_private_dirs`/`sync_ancestors`, fsyncing each directory level on `Strict` creation and backup so a newly opened store survives power loss.
  9. Unix primary files are created `0o600` and parent directories `0o700` before any data is written; `chmod`/`chown` errors fail open and surface as I/O errors instead of being silently ignored.
  10. `JobStateRecord::fail`/`cancel`/`resolve` clear `worker_id`, `lease_expires_at_ms`, and `retry_after_ms` for terminal/finalized states; `JobInfo` exposes `result_digest`/`error_summary`.
  11. `tests/p1_regression.rs` adds adversarial before/after-reopen and multi-process regression tests for every P1; `tests/property.rs` continues the model-based comparison; `tests/fuzz_targets.rs` provides deterministic seeded decoder fuzz coverage.
  12. CLI `projections scan` JSON and `export` use `hex(&key)` and `hex(&value)` instead of `String::from_utf8_lossy`, preserving arbitrary binary keys.
  13. `examples/synara_control_plane.rs` only deletes the default `synara.mini` path when no explicit path argument is supplied.
* Test evidence from this pass:
  * `tests/p1_regression.rs` covers all 13 P1 findings, including `same_process_second_open_is_rejected` and `second_process_open_is_rejected` (using `src/bin/lock_holder.rs`).
  * `src/store.rs` adds a unit test `mismatched_frame_record_count_is_rejected` that builds a corrupt `Frame` with `record_count = 2` and one record and proves the store refuses to open.
  * `tests/security.rs` verifies primary-file owner-only permissions and symlink rejection on Unix.
  * `tests/cli.rs` validates `export --format jsonl` and projection scan JSON output with hex payloads.
* `Cargo.lock` contains no `libc`, `wasi`, or `zerocopy`.

## Verdict

**Fixes applied — do not merge yet.** All P1/P2 findings are addressed, the full verification suite passes, and `docs/FINAL_REPORT.md` claims only what the tests prove. The PR remains open and unmerged per the review instruction.
