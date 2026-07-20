# Core Invariants

These invariants are encoded in the implementation and exercised by tests.

## Persistence

1. A transaction is visible entirely or not at all.
2. A frame without a valid commit trailer is not committed.
3. A committed event is immutable.
4. A transaction ID cannot identify different content.
5. An event ID cannot identify different content.
6. Global event sequence never decreases.
7. Stream version never decreases.
8. Expected stream versions are checked before append.
9. The store never silently skips committed mid-file corruption.
10. An incomplete final frame cannot corrupt earlier state.
11. `open_existing` and `StoreBuilder::verify` are non-mutating.
12. `StoreBuilder::verify` replays every frame through the full semantic validation path.
13. `StoreBuilder::open` auto-repairs a structurally torn tail; `Store::repair` is the explicit public write path for an `open_existing` store with `needs_repair`. `StoreBuilder::open_existing` and `StoreBuilder::verify` never truncate.
14. Backup refuses an existing destination and validates the temporary copy before the atomic rename.
15. `Limits::max_records_per_transaction` and `Limits::max_replace_entries` cannot exceed the hard format ceilings.
16. Uncertain truncate outcomes are reported as `RepairOutcomeUncertain`.
17. Uncertain claim outcomes are reported as `ClaimOutcome::Uncertain` carrying the proposed transaction ID and the claimed jobs (including their lease tokens).
18. Backup publication ambiguity (post-link or post-publication / parent-sync failure) is reported as `BackupOutcomeUncertain` rather than a plain I/O error.
19. `claim_jobs` uses durable round-robin partition fairness: per-queue cursors and per-partition head indexes are rebuilt from the journal on replay, so fairness is stable across reopen.
20. Decoded record memory is bounded by measured in-memory cost: `MAX_RECORD_MEMORY` (96 MiB) per record and `MAX_TRANSACTION_MEMORY` (256 MiB) per transaction frame, independent of the on-disk frame size.
21. Decode/encode allocations are fallible (`try_reserve`/`try_reserve_exact`); allocation failure is a typed `Validation` error, never an abort.
22. Frame lengths are validated as `u64` against `MAX_FRAME_SIZE` before any `usize` conversion; offset arithmetic is checked. Targets with pointers narrower than 32 bits are rejected at compile time.
23. `Store::verify` and `StoreBuilder::verify` observe a stable snapshot; an active writer cannot cause spurious verification failures.
24. `Store::repair` (and the `repair` CLI) reports current length, last valid offset, and bytes removed; uncertain truncation is surfaced as `RepairOutcomeUncertain`, and complete-frame corruption is refused rather than repaired.

## Projections

1. Projection mutations become visible atomically with their transaction.
2. Projection version changes are explicit and checked.
3. Full projection replacement is atomic.
4. Reopen reconstructs the same projection state.
5. Current projected state never includes a mutation from an uncommitted frame.

## Jobs

1. An enqueued job cannot silently disappear.
2. At most one current lease token exists per job.
3. A stale lease token cannot acknowledge or fail a newer lease.
4. Partition ordering is deterministic within a queue; across partitions, claims rotate round-robin from a durable per-queue cursor.
5. Idempotent expired jobs can be reclaimed only when `EffectMode::Idempotent` was chosen explicitly; the default is `UncertainOnLeaseExpiry`.
6. Non-idempotent expired jobs become uncertain.
7. Uncertain jobs are not silently retried.
8. Terminal jobs cannot return to pending without an explicit supported resolution.
9. Job enqueue can be atomic with its causal domain event.
10. `claim_jobs` atomically commits maintenance and candidate leases in one batch, bounded by `max_records_per_transaction` and `max_frame_size`.
11. Expired final-attempt job maintenance uses a fixed-size `JobExpire` record independent of `max_summary_len`.
12. Claiming from an empty queue is `ClaimOutcome::Noop`: no durable transaction is written and no transaction ID is allocated.

## API

1. Validation errors do not mutate disk or memory.
2. A successful commit has a stable receipt.
3. Uncertain commit outcomes are reported as uncertain.
4. A poisoned store rejects further writes; a store with an un-repaired tail rejects writes. Every `StorePoisoned` error carries the original poisoning transaction ID, not the ID of the rejected write.
5. JSON CLI mode writes only machine-readable output to stdout; human prose goes to stderr.
6. Projection mutations in a batch are validated against a borrowed base plus a per-batch overlay/delta; validation never deep-clones `ProjectionState`.
7. CLI JSON export streams in bounded pages (events by global sequence, projections entry by entry, jobs paginated); it is a diagnostic dump, not a byte-exact restorable snapshot.
