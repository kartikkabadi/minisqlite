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
13. `Store::repair` is the only public write path that truncates a torn tail.
14. Backup refuses an existing destination and validates the temporary copy before the atomic rename.
15. `Limits::max_records_per_transaction` cannot exceed the hard frame record ceiling.
16. Uncertain truncate outcomes are reported as `RepairOutcomeUncertain`.

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
4. Partition ordering is deterministic within a queue.
5. Idempotent expired jobs can be reclaimed.
6. Non-idempotent expired jobs become uncertain.
7. Uncertain jobs are not silently retried.
8. Terminal jobs cannot return to pending without an explicit supported resolution.
9. Job enqueue can be atomic with its causal domain event.
10. `claim_jobs` atomically commits maintenance and candidate leases in one batch, bounded by `max_records_per_transaction` and `max_frame_size`.
11. Expired final-attempt job maintenance uses a fixed-size `JobExpire` record independent of `max_summary_len`.

## API

1. Validation errors do not mutate disk or memory.
2. A successful commit has a stable receipt.
3. Uncertain commit outcomes are reported as uncertain.
4. A poisoned store rejects further writes; a store with an un-repaired tail rejects writes.
5. JSON CLI mode writes only machine-readable output to stdout; human prose goes to stderr.
