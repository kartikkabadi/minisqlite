# minisqlite

[![Crates.io](https://img.shields.io/crates/v/minisqlite?logo=rust&label=crates.io)](https://crates.io/crates/minisqlite)
[![Docs.rs](https://img.shields.io/docsrs/minisqlite?logo=rust&label=docs.rs)](https://docs.rs/minisqlite)
[![CI](https://github.com/kartikkabadi/minisqlite/actions/workflows/ci.yml/badge.svg)](https://github.com/kartikkabadi/minisqlite/actions/workflows/ci.yml)

A typed embedded control-plane state kernel that atomically coordinates domain events, materialized state, durable work, and uncertain external effects.

One SQLite-backed transaction coordinates four concerns that control planes otherwise stitch together by hand:

- **Domain events** appended to versioned streams with optimistic concurrency.
- **Materialized projections** patched with strict version increments.
- **Durable jobs** with partition-ordered claiming, leases, retries, and cancellation.
- **Honest uncertainty**: outcomes that may or may not have persisted are reported as indeterminate and recovered explicitly, never guessed.

It is built for local-first, single-node control planes — agent harnesses, desktop AI clients, workflow orchestrators — where domain history, current state, and asynchronous side effects must commit together.

## Install

```toml
[dependencies]
minisqlite = "0.3.0-alpha.1"
```

## Usage

Open a store, commit an event, a projection patch, and a job in one atomic batch, then claim the job:

```rust
use minisqlite::{
    ClaimOutcome, ClaimRequest, CommitBatch, ControlPlaneStore, Event, Id, JobSpec,
    ProjectionPatch,
};

let store = ControlPlaneStore::open("control-plane.db")?;

let batch = CommitBatch::new(Id::from(1u128), 1_000)
    .append_event(Event::with_json_payload(
        Id::from(2u128),
        "thread-42",
        "thread.turn-requested",
        1_000,
        br#"{"prompt":"hello"}"#,
    ))
    .apply_projection_patch(
        ProjectionPatch::new("threads", 0).put(b"thread-42".to_vec(), b"queued".to_vec()),
    )
    .enqueue_job(JobSpec::reconcilable(
        Id::from(3u128),
        "provider-turns",
        "thread-42",
        b"call provider".to_vec(),
    ));
let receipt = store.commit(&batch)?;

match store.claim_jobs(&ClaimRequest {
    queue: "provider-turns".into(),
    worker_id: "worker-1".into(),
    now_ms: 2_000,
    lease_ms: 30_000,
    limit: 1,
})? {
    ClaimOutcome::Committed(claims) => {
        for job in claims.jobs() {
            // perform the external effect, then acknowledge with job.lease_token
        }
    }
    ClaimOutcome::MaintenanceCommitted(_) => { /* expired-lease maintenance only; poll again */ }
    ClaimOutcome::Noop => { /* queue empty */ }
}
```

Indeterminate commits and claims are surfaced as typed errors (`CommitError::Indeterminate`, `ClaimError::Indeterminate`) that carry no executable work (no payloads or lease tokens). Recover them explicitly with `recover_transaction` and `recover_claim`.

## CLI

An operational CLI ships with the crate:

```bash
minisqlite doctor --db control-plane.db
minisqlite verify --db control-plane.db
minisqlite stats --db control-plane.db
minisqlite events tail --db control-plane.db --limit 10
minisqlite jobs list --db control-plane.db --queue provider-turns
minisqlite backup backup.db --db control-plane.db
```

## Design

- [docs/ADR-001.md](https://github.com/kartikkabadi/minisqlite/blob/a34637409aff5b6bcef1444df075ad86982ea998/docs/ADR-001.md) — why SQLite is the production storage substrate
- [docs/ROADMAP.md](https://github.com/kartikkabadi/minisqlite/blob/a34637409aff5b6bcef1444df075ad86982ea998/docs/ROADMAP.md) — delivery phases and exit criteria
- [docs/SCOPE.md](https://github.com/kartikkabadi/minisqlite/blob/a34637409aff5b6bcef1444df075ad86982ea998/docs/SCOPE.md) — what is included and explicitly excluded

Explicit non-goals: SQL exposed through the public API, distributed consensus, multi-process writers, replication, a workflow DSL, and a public multi-backend storage abstraction.

The pre-0.3 custom SQL engine is preserved for reference at the [`v0.2.1`](https://github.com/kartikkabadi/minisqlite/tree/v0.2.1) tag, and the append-only journal engine on the [`archive/append-only-journal-v1`](https://github.com/kartikkabadi/minisqlite/tree/archive/append-only-journal-v1) branch.

## Test

```bash
cargo test --all-targets --all-features
```

## Changelog

See [CHANGELOG.md](CHANGELOG.md).

## License

MIT
