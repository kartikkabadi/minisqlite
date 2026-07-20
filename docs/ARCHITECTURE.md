# Architecture

## Layer diagram

```text
┌──────────────────────────────────────┐
│ Application / example / CLI            │
│   Domain commands, workers, UI         │
└──────────────┬───────────────────────┘
               │ CommitBatch / public API
┌──────────────▼───────────────────────┐
│ Public API (src/store.rs)            │
│   validation, idempotency,           │
│   stream-version checks,             │
│   projection/job op validation       │
└──────────────┬───────────────────────┘
               │ staged transaction
┌──────────────▼───────────────────────┐
│ Append-only storage kernel           │
│   file, frame, record encoding       │
│   recovery scanner, checksums        │
└──────────────────────────────────────┘
```

## Modules

* `config` — `Durability`, `EffectMode`, `Limits`.
* `error` — typed `Error` for validation, corruption, conflicts, etc.
* `event` — `Event`, `PersistedEvent`, `StreamVersion`.
* `id` — 128-bit `Id` from the OS CSPRNG (`/dev/urandom` on Unix, `BCryptGenRandom` on Windows), rejecting the all-zero ID.
* `jobs` — `JobSpec`, `ClaimRequest`, `ClaimedJob`, `JobState`, `Resolution`.
* `projection` — `ProjectionEntry`, `ProjectionState` (internal).
* `transaction` — `CommitBatch`, `CommitReceipt`, `Op`.
* `store` — `Store`, `StoreBuilder`, `StoreStats`.
* `codec` — `Writer`/`Reader`, header/frame/record encoding, CRC32.
* `storage` — `DataFile`, `Lock`, `recovery` scanner.
* `main` — operational CLI with manual argument parsing, including `repair` and a streaming JSON export.

## Concurrency model

One process owns the store.
A `RwLock<StoreInner>` serializes writes and allows concurrent readers.
Readers take a read lock briefly and clone data; writers take a write lock.
The single-owner advisory lock is held directly on the primary data file via `std::fs::File::lock`/`try_lock` (Rust 1.89+). No separate lock file is used.

## Durability path

1. Validate immutable invariants (non-zero IDs, `max_attempts > 0`) and configured `Limits`.
2. Validate projection operations against a borrowed base plus a per-batch overlay/delta (no deep clone of `ProjectionState`); validate job operations.
3. Encode records to the transaction payload.
4. Check transaction/event idempotency.
5. Append one frame: header + payload + trailer.
6. Sync the file in `Strict` mode.
7. Apply records to memory.
8. Return a stable `CommitReceipt`.

## Recovery

Reopening scans frames sequentially from the file header.
Each frame header and trailer are validated.
The declared record count is bounded by `MAX_RECORDS_PER_FRAME` before decoding.
Decoded records are bounded by measured in-memory cost (`MAX_RECORD_MEMORY`, `MAX_TRANSACTION_MEMORY`), and decode allocations are fallible (`try_reserve`).
Frame lengths are validated as `u64` before `usize` conversion with checked arithmetic.
A complete valid prefix is replayed; a torn trailing frame is either truncated (`open`) or left for explicit `Store::repair` (`open_existing`).
A corrupted mid-file frame causes a hard failure so an operator can investigate.
`StoreBuilder::verify()` performs a read-only full semantic replay in a transient store and fails closed on torn tails and semantic corruption. Verification observes a stable snapshot so an active writer cannot cause spurious failures.
