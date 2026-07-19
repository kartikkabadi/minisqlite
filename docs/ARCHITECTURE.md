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
* `id` — 128-bit `Id` from nanosecond time + atomic counter.
* `jobs` — `JobSpec`, `ClaimRequest`, `ClaimedJob`, `JobState`, `Resolution`.
* `projection` — `ProjectionEntry`, `ProjectionState` (internal).
* `transaction` — `CommitBatch`, `CommitReceipt`, `Op`.
* `store` — `Store`, `StoreBuilder`, `StoreStats`.
* `codec` — `Writer`/`Reader`, header/frame/record encoding, CRC32.
* `storage` — `DataFile`, `Lock`, `recovery` scanner.
* `main` — operational CLI with manual argument parsing.

## Concurrency model

One process owns the store.
A `RwLock<StoreInner>` serializes writes and allows concurrent readers.
Readers take a read lock briefly and clone data; writers take a write lock.
A separate `.lock` file provides advisory single-owner locking via `fs2`.

## Durability path

1. Validate the `CommitBatch` against memory state.
2. Validate projection operations and job operations.
3. Encode records to the transaction payload.
4. Check transaction/event idempotency.
5. Append one frame: header + payload + trailer.
6. Sync the file in `Strict` mode.
7. Apply records to memory.
8. Return a stable `CommitReceipt`.

## Recovery

Reopening scans frames sequentially from the file header.
Each frame header and trailer are validated.
A complete valid prefix is replayed; a torn trailing frame is truncated and reported.
A corrupted mid-file frame causes a hard failure so an operator can investigate.
