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

`v0.3.0-alpha.1` is not yet published. Build and install from this branch:

```bash
git clone https://github.com/kartikkabadi/minisqlite.git
cd minisqlite
git checkout feat/control-plane-state-engine
cargo install --path .
```

Or add as a library dependency from this branch:

```toml
[dependencies]
minisqlite = { git = "https://github.com/kartikkabadi/minisqlite", branch = "feat/control-plane-state-engine" }
```

## Quick example

```rust
use minisqlite::{CommitBatch, Durability, Event, Id, StoreBuilder};

fn main() {
    let store = StoreBuilder::new("app.mini")
        .durability(Durability::Strict)
        .open()
        .unwrap();

    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_millis() as i64;

    let event = Event::with_json_payload(
        Id::new().unwrap(),
        "user:42",
        "user.created",
        now,
        br#"{"name":"Ada"}"#,
    );

    store
        .commit(
            CommitBatch::new(Id::new().unwrap(), now)
                .append_event(event)
                .projection_put("users", 1, b"user:42".to_vec(), br#"{"name":"Ada"}"#.to_vec())
                .enqueue_job(minisqlite::JobSpec::new(
                    minisqlite::Id::new().unwrap(),
                    "emails",
                    "welcome",
                    b"user:42".to_vec(),
                )),
        )
        .unwrap();

    let users = store.scan_projection_prefix("users", b"").unwrap();
    println!("{:?}", users);

    let claimed = store
        .claim_jobs(minisqlite::ClaimRequest {
            queue: "emails".into(),
            worker_id: "worker-1".into(),
            now_ms: now,
            lease_ms: 30_000,
            limit: 1,
        })
        .unwrap();
    println!("claimed {:?}", claimed);
}
```

See [`examples/synara_control_plane.rs`](examples/synara_control_plane.rs) for a complete reference application showing threads, provider turns, loop scheduling, and projection rebuilding.

## CLI

```bash
minisqlite doctor app.mini
minisqlite verify app.mini
minisqlite stats app.mini
minisqlite events tail app.mini 50
minisqlite events stream app.mini user:42
minisqlite projections list app.mini
minisqlite projections scan app.mini users
minisqlite jobs list app.mini
minisqlite export app.mini --format jsonl > snapshot.jsonl
minisqlite repair app.mini
minisqlite backup app.mini app-backup.mini
```

`verify` scans the file read-only without modifying it. `doctor` reports whether the store needs explicit `repair` after an unclean shutdown. `repair` truncates a torn tail, reporting the current length, last valid offset, and bytes removed (`--force` skips confirmation; JSON output is supported); it refuses complete-frame corruption. `export` streams a bounded-memory JSONL diagnostic dump; it is not a byte-exact restorable snapshot.

## Crash recovery guarantee

* The durable representation is an append-only sequence of self-checksummed transaction frames.
* Normal commits never overwrite old state.
* A torn final frame is safely truncated on `open`; `open_existing` leaves it for explicit `Store::repair` so verification is separate from repair.
* `StoreBuilder::verify()` performs a read-only scan without locking or modifying the file.
* Mid-file corruption is reported as a hard error.
* `Strict` mode calls `fsync` before returning success.
* Replay enforces immutable invariants (non-zero IDs, `max_attempts > 0`, valid lease tokens and attempt sequence, `lease_expires_at_ms > claimed_at_ms`).
* Replay memory is bounded by hard per-record and per-transaction in-memory ceilings with fallible allocation.

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

CI also runs a pinned Rust 1.89 MSRV lane on Ubuntu.

## License

MIT
