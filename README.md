# minisqlite

[![Crates.io](https://img.shields.io/crates/v/minisqlite?logo=rust&label=crates.io)](https://crates.io/crates/minisqlite)
[![Docs.rs](https://img.shields.io/docsrs/minisqlite?logo=rust&label=docs.rs)](https://docs.rs/minisqlite)
[![CI](https://github.com/kartikkabadi/minisqlite/actions/workflows/ci.yml/badge.svg)](https://github.com/kartikkabadi/minisqlite/actions/workflows/ci.yml)

**One commit. Whole truth.**

minisqlite commits your events, your current state, and your background jobs in one atomic SQLite transaction — and when a crash makes an outcome unknowable, it says "I don't know" and gives you tools to find out, instead of guessing.

## Before / after

Without minisqlite, an app that records what happened, updates its state, and schedules follow-up work does it in three steps that can each fail independently:

```rust
// Before: three writes, three failure points, no shared atomicity.
event_log.append("thread.turn-requested", payload)?;   // crash here: state and job lost
state.set("thread-42", "queued")?;                     // crash here: job lost
job_queue.enqueue("provider-turns", "call provider")?; // did it enqueue? retry and risk duplicates
```

With minisqlite, all three land in one transaction — or none of them do:

```rust
// After: one commit, all-or-nothing.
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
```

## What you get

- **Events**: append-only, versioned streams with optimistic concurrency.
- **State**: materialized projections patched with strict version increments.
- **Jobs**: durable queues with partition-ordered claiming, leases, retries, and cancellation.
- **Honest recovery**: outcomes that may or may not have persisted are reported as indeterminate and recovered explicitly, never guessed.

It is built for local-first, single-node apps — agent harnesses, desktop AI clients, workflow orchestrators — where history, current state, and asynchronous side effects must commit together.

## Install

`0.3.0-alpha.1` is not yet published to crates.io. Until it is released, use a git dependency:

```toml
[dependencies]
minisqlite = { git = "https://github.com/kartikkabadi/minisqlite" }
```

Once `0.3.0-alpha.1` is published, the crates.io dependency applies:

```toml
[dependencies]
minisqlite = "0.3.0-alpha.1"
```

## Usage

Open a store, commit an event, a projection patch, and a job in one atomic batch (as above), then claim the job:

```rust
use minisqlite::{ClaimOutcome, ClaimRequest, ControlPlaneStore};

let store = ControlPlaneStore::open("control-plane.db")?;

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

## Why not X?

- **Why not Postgres with an outbox table?** If you are running a server anyway, do that. minisqlite is for single-process, local-first apps where a database server is operational overhead you don't want.
- **Why not Redis or a hosted job queue?** Those queue jobs, but they can't commit a job atomically with the event and state change that caused it, so you're back to two-phase glue code and duplicate-delivery handling.
- **Why not plain rusqlite and your own schema?** You can — minisqlite is that schema plus the parts that take the longest to get right: versioned streams, lease-based claiming, and explicit recovery of in-doubt outcomes after a crash.

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

## Test

```bash
cargo test --all-targets --all-features
```

## Changelog

See [CHANGELOG.md](CHANGELOG.md).

## History

Before 0.3, minisqlite was a from-scratch SQL engine; the last such release is [`v0.2.1`](https://github.com/kartikkabadi/minisqlite/tree/v0.2.1), and the earlier append-only journal engine lives on [`archive/append-only-journal-v1`](https://github.com/kartikkabadi/minisqlite/tree/archive/append-only-journal-v1).

## License

MIT
