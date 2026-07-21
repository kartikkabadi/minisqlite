# Scope: next release

Defines what the SQLite-backed control-plane kernel release will and will not
contain. See [ADR-001](./ADR-001.md) and [ROADMAP.md](./ROADMAP.md).

## In scope

- SQLite as the sole production storage substrate (single file, WAL mode).
- Transaction IDs and idempotent resubmission (canonical request digests;
  duplicate transaction IDs with identical content return the original
  receipt, with different content fail deterministically).
- Typed, append-only event streams with per-stream versions and a global
  sequence.
- Expected stream-version conflict detection with typed conflict errors.
- Projection patches: one patch atomically advances one projection version
  (`new_version == expected_version + 1`) and may contain multiple
  mutations (put, delete, clear, replace).
- Durable jobs with a typed state machine (Pending, Leased, RetryWait,
  Uncertain, Succeeded, Dead, Cancelled).
- Leases with tokens, worker IDs, and expiries.
- Retries with backoff and a maximum attempt count.
- Cancellation.
- Uncertain outcomes: indeterminate commits and claims return only a
  transaction ID (never executable work), with typed recovery APIs
  (committed / absent / still indeterminate). Effect modes make retry
  behavior explicit: reconciliation-required jobs (the default) become
  `Uncertain` on lease expiry and are never silently retried; only
  explicitly idempotent jobs re-enter the retry path.
- Claim recovery: reconstructing committed claims (with original lease
  tokens) from claim receipts after an indeterminate claim.
- Heartbeats / lease renewal: token-checked lease extension primitives in
  the core, with an optional runtime helper.
- Live online backup (safe during writes, restorable, integrity-checked).
- Diagnostics: doctor, verify, stats, and paginated non-restorable
  diagnostic export with payload redaction by default.
- Synara integration: one provider-turn vertical slice behind a feature
  flag, with failure drills and an operator view for uncertain jobs.
- A typed public kernel API for a single writer process.
- Crash-injection, property, model-based, concurrency, and integrity
  testing.
- Documentation, examples, and a tagged release.

## Out of scope (non-goals)

- **No distributed consensus** — single node only; no Raft/Paxos.
- **No multi-writer processes** — exactly one writer process owns the database.
- **No generic workflow DSL** — jobs are typed Rust, not a workflow language.
- **No cron engine** — no built-in time-based scheduling.
- **No remote server** — embedded library only; no network protocol.
- **No replication** — no log shipping or follower nodes.
- **No vector search** — no embeddings or similarity indexes.
- **No arbitrary SQL public API** — consumers use the typed kernel API, never
  raw SQL.
- **No plugin architecture** — no dynamic extension loading.
- **No public multi-backend API** — storage is a private internal module
  (see ADR-001 on deferring the backend abstraction).
- **No custom compaction engine** — SQLite's `VACUUM`/checkpointing suffices.
- **No automatic sharding** — one database file per kernel instance.

Items in the non-goals list require a new ADR before any implementation work.
