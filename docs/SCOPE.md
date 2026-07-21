# Scope: next release

Defines what the SQLite-backed control-plane kernel release will and will not
contain. See [ADR-001](./ADR-001.md) and [ROADMAP.md](./ROADMAP.md).

## In scope

- SQLite as the sole production storage substrate (single file, WAL mode).
- Typed, append-only event log with a monotonic sequence invariant.
- Deterministic projections with tracked positions and full rebuild support.
- Durable job queue with at-least-once execution, retries, and backoff.
- A typed public kernel API for a single writer process.
- Explicit representation of uncertainty (in-flight jobs, unacknowledged
  writes, projection rebuild states).
- Crash-injection, property, and integrity testing.
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
