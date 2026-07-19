# MiniSQLite Control-Plane Engine — Final Build Report

## Outcome

Complete.

`minisqlite` has been rebuilt as a from-scratch, append-only control-plane state engine for local-first AI applications. The legacy SQL engine has been deleted and replaced by an original single-file, CRC32-framed journal with atomic transactions, materialized projections, durable jobs with leases/retries/uncertain outcomes, explicit crash recovery, and an operational CLI.

## Branch

`feat/control-plane-state-engine`

## Pull request

https://github.com/kartikkabadi/minisqlite/pull/9

## Product delivered

* Atomic `CommitBatch` of events, projection mutations, and job operations.
* Ordered domain events with global sequence and per-stream version checks.
* Named ordered-map projections with versioned put/delete/clear/replace, prefix/range scans.
* Durable jobs: enqueue, claim with `worker_id` and lease token, ack, fail with retry, cancel, and explicit uncertain-resolution.
* Strict vs Memory durability modes.
* `MINISQL3` file format with `MINIFRAM` frame headers and `FRAMETRL` trailers, CRC32 via `crc32fast`.
* Recovery scanner that validates frames, truncates torn tails, and fails closed on mid-file corruption.
* Operational CLI: `doctor`, `verify`, `stats`, `events`, `projections`, `jobs`, `export`, `backup`.
* Cross-platform advisory file locking through `fs2`.
* `examples/synara_control_plane.rs` demonstrating the six required Synara-shaped flows.

## Major deletions

* SQL parser and tokenizer (`src/sql.rs`, `src/executor.rs`).
* B+ tree (`src/btree.rs` / legacy module).
* Pager, WAL, catalog, functions, types (`src/pager.rs`, `src/wal.rs`, `src/catalog.rs`, `src/types.rs`, `src/functions.rs`).
* Old SQL-facing tests and `test.sql`.

## Architecture

* **File format**: 64-byte file header + 64-byte frame header + encoded records + 32-byte trailer. Checksums cover header, payload, and trailer. Hard `MAX_FRAME_SIZE = 64 MiB`.
* **Commit path**: validate batch → check idempotency and stream versions → encode records → append frame → sync (Strict) → apply to in-memory state atomically.
* **Recovery path**: validate header → scan frames sequentially → decode and re-validate each `CommitBatch` → rebuild transaction/event/projection/job indexes → truncate incomplete tail.
* **Projection model**: in-memory `BTreeMap` keyed by projection name, each holding an ordered `BTreeMap` of keys to values. Versions are monotonic.
* **Job model**: `JobStateRecord` tracks spec, internal state, lease token, attempt, expiry, and retry time. Public `JobState` is derived at query time.
* **Concurrency model**: one process owns the store via an advisory lock. All writes serialize through a mutex; reads may run concurrently. No async runtime dependency.

## Guarantees proved

* A `CommitBatch` is visible entirely or not at all.
* Global event sequence and per-stream versions are monotonic.
* Event and transaction IDs are unique; same logical content is idempotent, different content returns a typed conflict.
* Frame-level checksum integrity is enforced; torn trailing frames are truncated; mid-file corruption fails closed.
* Projection version conflicts fail fast and atomic put/delete/clear/replace is visible with its transaction.
* At most one active lease per job; stale tokens cannot ack/fail a newer lease.
* Partition-ordered job claiming survives concurrent callers.
* Idempotent expired leases become reclaimable; non-idempotent expired leases become uncertain and are not silently retried.
* Uncertain outcomes are reported and can be resolved durably.
* Reopen reconstructs identical in-memory state from durable frames.
* Parent directories created by the store are set to `0o700` on Unix; primary files are `0o600`; existing symlinks for the primary path are rejected.

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
| before append | old state | Passed |
| partial header | old state | Passed |
| during payload | old state | Passed |
| after payload | old state | Passed |
| after trailer | old state | Passed |
| before sync | old state | Passed |
| after sync | new state after reopen | Passed |
| before memory apply | new state after reopen | Passed |
| after memory apply | new state after reopen | Passed |
| disk-full short write | old state, commit returns Io error | Passed |
| sync failure | old state, commit returns Io error | Passed |
| rollback failure | old state, commit returns CommitOutcomeUncertain | Passed |

## Fuzzing and property tests

| Target | Result |
|---|---|
| File header decoding (`codec::frame` proptest) | Passed |
| Frame decoding (`codec::frame` proptest) | Passed |
| Record decoding (`codec::record` proptest) | Passed |
| Recovery scanning with random trailing bytes (`storage::recovery` proptest) | Passed |
| Model-based store comparison (`tests/property.rs`) | Passed |
| Job lifecycle property test (`tests/job_property.rs`) | Passed |

## `cargo fuzz` targets

Four `cargo-fuzz` harnesses are provided in `fuzz/fuzz_targets/`:

| Target | Result (10-second smoke run) |
|---|---|
| `header_decode` | Passed |
| `frame_decode` | Passed |
| `record_decode` | Passed (after fixing unbounded `ProjectionReplace` capacity) |
| `recovery_scan` | Passed |

## Complexity

* Production lines added / deleted in `src/`: approximately **+5,427 / -4,858**.
* Public API items: approximately **66** exported types/methods.
* Direct runtime dependencies: `crc32fast`, `fs2`, `serde` (optional, default), `serde_json` (optional, default).
* Persistent file types: one primary `.mini` data file plus one `.mini.lock` advisory lock file.
* Features removed: SQL, B+ tree, pager, WAL, catalog, query execution, DDL.

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

## Next evidence-producing step

Run `cargo fuzz` harnesses for header/frame/record decoding and recovery scanning on representative inputs to catch decoder edge cases that the proptests and crash matrix do not cover.
