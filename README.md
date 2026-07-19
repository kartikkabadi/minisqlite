# minisqlite

[![Crates.io](https://img.shields.io/crates/v/minisqlite?logo=rust&label=crates.io)](https://crates.io/crates/minisqlite)
[![Docs.rs](https://img.shields.io/docsrs/minisqlite?logo=rust&label=docs.rs)](https://docs.rs/minisqlite)
[![CI](https://github.com/kartikkabadi/minisqlite/actions/workflows/ci.yml/badge.svg)](https://github.com/kartikkabadi/minisqlite/actions/workflows/ci.yml)

> **A from-scratch embedded state engine for local AI control planes.**
>
> Atomically record events, materialize current state, and queue durable work in one append-only local file—without SQL or a database server.

## What it is

`minisqlite` is a tiny Rust library and CLI for local-first AI control-plane state.
It stores ordered events, materialized projections, and durable jobs in a single `.mini` file.

## Who it is for

Developers building desktop, CLI, or single-node edge applications that need:

* durable event history,
* derived read models,
* asynchronous work with leases and retries,
* clean crash recovery,
* no separate database server.

## What problem it solves

Control-plane applications usually assemble persistence from many pieces:
an event log, a state store, a job queue, idempotency keys, retry logic, recovery scripts, and migration tooling.

`minisqlite` replaces that with one append-only transaction model:

```text
events + projected state + durable work
```

A single `CommitBatch` can append a domain event, update a projection, and enqueue a job atomically.
A single reopen replays committed frames and restores the same state.

## What it deliberately does not do

* SQL queries or query planning.
* Multi-process writers.
* Distributed replication or consensus.
* Vector search, workflow DSLs, dashboards, or background schedulers.
* Automatic snapshots or compaction in the first version.
* Encryption at rest.

## Current status

`v0.3.0-alpha.1` is a correctness-first rewrite.
The old SQL engine has been removed and replaced by the append-only control-plane kernel.
The API and file format may change.

## Install

```bash
cargo install minisqlite
```

Or add as a library dependency:

```toml
[dependencies]
minisqlite = "0.3.0-alpha.1"
```

## Quick example

```rust
use minisqlite::{CommitBatch, Durability, Event, Id, StoreBuilder};

fn main() {
    let store = StoreBuilder::new("app.mini")
        .durability(Durability::Strict)
        .open()
        .unwrap();

    let event = Event::with_json_payload(
        Id::new(),
        "user:42",
        "user.created",
        br#"{"name":"Ada"}"#,
    );

    store
        .commit(
            CommitBatch::new(Id::new(), 0)
                .append_event(event)
                .projection_put("users", 1, b"user:42".to_vec(), br#"{"name":"Ada"}"#.to_vec()),
        )
        .unwrap();

    let users = store.scan_projection_prefix("users", b"").unwrap();
    println!("{:?}", users);
}
```

See [`examples/synara_control_plane.rs`](examples/synara_control_plane.rs) for a complete reference application showing threads, provider turns, loop scheduling, and projection rebuilding.

## CLI

```bash
minisqlite app.mini doctor
minisqlite app.mini stats
minisqlite app.mini events tail 50
minisqlite app.mini events stream user:42
minisqlite app.mini projections list
minisqlite app.mini projections scan users
minisqlite app.mini jobs list
minisqlite app.mini export --format jsonl > snapshot.jsonl
minisqlite app.mini backup app-backup.mini
```

## Crash recovery guarantee

* The durable representation is an append-only sequence of self-checksummed transaction frames.
* Normal commits never overwrite old state.
* A torn final frame is safely truncated on reopen.
* Mid-file corruption is reported as a hard error.
* `Strict` mode calls `fsync` before returning success.

## Limitations

* Single process owns the store.
* No SQL or ad-hoc queries.
* One `.mini` file grows append-only.
* No encryption at rest; file permissions are set to `0o600` on Unix.

See [`docs/LIMITATIONS.md`](docs/LIMITATIONS.md) and [`docs/SECURITY.md`](docs/SECURITY.md) for details.

## Documentation

* [`docs/PRODUCT.md`](docs/PRODUCT.md) — product definition.
* [`docs/ARCHITECTURE.md`](docs/ARCHITECTURE.md) — module and concurrency model.
* [`docs/FORMAT.md`](docs/FORMAT.md) — on-disk file format.
* [`docs/INVARIANTS.md`](docs/INVARIANTS.md) — core guarantees.
* [`docs/RECOVERY.md`](docs/RECOVERY.md) — recovery behavior.
* [`docs/JOBS.md`](docs/JOBS.md) — durable jobs.
* [`docs/SECURITY.md`](docs/SECURITY.md) — threat model and known limits.
* [`docs/DEPENDENCIES.md`](docs/DEPENDENCIES.md) — dependency budget.
* [`docs/SYNARA_CASE_STUDY.md`](docs/SYNARA_CASE_STUDY.md) — reference workload walkthrough.
* [`docs/LIMITATIONS.md`](docs/LIMITATIONS.md) — explicit deletions and limitations.

## Test

```bash
cargo fmt --all -- --check
cargo clippy --all-targets --all-features -- -D warnings
cargo test --all-targets --all-features
cargo test --doc --all-features
cargo package --allow-dirty
```

## License

MIT
