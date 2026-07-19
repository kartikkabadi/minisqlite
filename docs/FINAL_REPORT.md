# MiniSQLite Control-Plane Engine — Final Build Report

## Outcome

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

## Major deletions

* SQL parser and tokenizer (`src/sql.rs`, `src/executor.rs`).
* B+ tree (`src/btree.rs` / legacy module).
* Pager, WAL, catalog, functions, types (`src/pager.rs`, `src/wal.rs`, `src/catalog.rs`, `src/types.rs`, `src/functions.rs`).
* Old SQL-facing tests and `test.sql`.

## Architecture

* **File format**: 64-byte file header + 64-byte frame header + encoded records + 32-byte trailer. Checksums cover header, payload, and trailer. Hard `MAX_FRAME_SIZE = 64 MiB`.
* **Commit path**: validate batch → encode records → append frame → sync (Strict) → apply to in-memory state atomically.
* **Recovery path**: validate header → scan frames sequentially → decode and re-validate each `CommitBatch` → rebuild transaction/event/projection/job indexes → truncate incomplete tail.
* **Projection model**: in-memory `BTreeMap` keyed by projection name, each holding an ordered `BTreeMap` of keys to values. Versions are monotonic.
* **Job model**: `JobStateRecord` tracks spec, internal state, lease token, attempt, expiry, and retry time. Public `JobState` is derived at query time.
* **Concurrency model**: one process owns the store via an advisory lock. All writes serialize through a mutex; reads may run concurrently. No async runtime dependency.

## Guarantees proved

* Atomic visibility of a `CommitBatch`.
* Monotonic global event sequence and per-stream versions.
* Event and transaction ID uniqueness with same-content idempotency.
* Frame-level checksum integrity and torn-tail truncation.
* Mid-file corruption fails closed.
* Projection version conflicts fail fast.
* At most one active lease per job; stale tokens cannot ack/fail newer leases.
* Partition-ordered job claiming.
* Idempotent expired leases become reclaimable; non-idempotent expired leases become uncertain.
* Uncertain outcomes are reported and can be resolved durably.
* Reopen reconstructs identical in-memory state from durable frames.

## Guarantees not yet proved

* Power-loss durability is bounded by OS and storage device behavior (documented).
* Encryption at rest is not provided.
* No exactly-once external effects; the engine only records outcomes.
* No distributed/multi-process write coordination.

## Verification

All quality gates pass:

```bash
cargo fmt --all -- --check
cargo clippy --all-targets --all-features -- -D warnings
cargo test --all-targets --all-features
cargo test --doc --all-features
cargo package --allow-dirty
```

CI matrix: `ubuntu-latest`, `macos-latest`, `windows-latest`.

## Crash matrix

Process-level failpoint tests in `tests/crash.rs` cover:

| Failpoint | Test |
|-----------|------|
| before-append | `crash_before_append_recovers` |
| partial-header | `crash_partial_header_recovers` |
| during-payload | `crash_during_payload_recovers` |
| after-payload | `crash_after_payload_recovers` |
| after-trailer | `crash_after_trailer_recovers` |
| before-sync | `crash_before_sync_recovers` |
| after-sync | `crash_after_sync_recovers` |
| before-memory-apply | `crash_before_memory_apply_recovers` |
| after-memory-apply | `crash_after_memory_apply_recovers` |

## Fuzzing and property tests

* `codec::frame` proptests for arbitrary file-header and frame bytes.
* `codec::record` proptest for arbitrary record payload bytes.
* `storage::recovery` proptest for arbitrary trailing bytes after a valid header.
* `tests/property.rs` model-based test comparing store behavior to an in-memory reference.
* `tests/job_property.rs` proptest covering enqueue/claim/acknowledge.

## Complexity

* `src/` change: **+5,427 / -4,858** lines.
* Public API surface: approximately **66** exported types/methods.
* Direct runtime dependencies: `crc32fast`, `fs2`, `serde` (optional), `serde_json` (optional, default).
* Persistent file types: one primary `.mini` data file plus one `.mini.lock` advisory lock file.
* Features removed: SQL, B+ tree, pager, WAL, catalog, query execution, DDL.

## Synara-shaped demonstration

`examples/synara_control_plane.rs` demonstrates:

* **Flow A**: Create a thread and project its initial state.
* **Flow B**: Request a provider turn, append an event, update the projection, and enqueue a provider job in one transaction.
* **Flow C**: Claim the job, then acknowledge it after completing the provider work.
* **Flow D**: Enqueue a non-idempotent job, let its lease expire, observe the uncertain state, and resolve it as succeeded.
* **Flow E**: Schedule a future-dated loop job, close and reopen the store, then claim it only after `not_before_ms`.
* **Flow F**: Rebuild the `threads` projection from the event stream and atomically replace it.

## Limitations

* Alpha format; `v0.3.0-alpha.1`.
* Single owning process.
* No encryption at rest.
* No cloud sync, replication, or distributed consensus.
* No multi-process writes.
* No automatic snapshots or compaction.
* No exactly-once external effects.
* Bounded control-plane data workload; not a general-purpose blob store.
* Not production-ready.

## Follow-ups

Based on measured evidence from `examples/benchmark.rs`, 100k event commit/reopen stays well under one second on reference hardware, so snapshots/compaction are deferred until real workloads exceed startup budgets. Future work can add `cargo fuzz` harnesses, more failure-mode property tests, and optional compression if measured file size becomes the bottleneck.
