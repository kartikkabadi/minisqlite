# MiniSQLite Product Definition

## What it is

A tiny, embeddable Rust library and CLI for local-first AI control-plane state.
One primary `.mini` file stores ordered events, materialized projections, and durable jobs.

## Who it is for

Developers building desktop, CLI, or single-node edge applications that need:

* durable event history,
* derived read models,
* asynchronous work with leases and retries,
* clean crash recovery,
* no separate database server.

## What problem it solves

A control-plane kernel usually gets assembled from many separate pieces:

* an event log,
* a state store,
* a job queue,
* idempotency keys,
* retry/dead-letter logic,
* recovery and backup scripts.

MiniSQLite replaces that scaffolding with a single append-only transaction model:

```text
events + projected state + durable work
```

A single `CommitBatch` can append domain events, update a projection, and enqueue a job atomically.
A single reopen replays committed frames and restores the same state.

## What it deliberately does not do

* SQL queries or query planning.
* Multi-process writers.
* Distributed replication or consensus.
* Vector search, workflow DSLs, dashboards, or background schedulers.
* Automatic snapshots or compaction in the first version.
* Encryption at rest.
* Production-grade crash testing for every storage device.

## Alpha status

`v0.3.0-alpha.1` is a correctness-first rewrite.
The file format, public API, and CLI are intentionally small and may change as usage teaches us what is essential.

## First valuable outcome

The `examples/synara_control_plane.rs` reference application shows a thread lifecycle:

1. Create a thread with an event and a projection.
2. Request provider work, atomically updating state and enqueuing a job.
3. Claim the job, complete it, and acknowledge it.
4. Handle an uncertain external effect without silent retry.
5. Schedule a durable loop with future jobs.
6. Rebuild a projection from event history without a migration.

## Honest durability

`Strict` mode calls `fsync` before reporting success.
This is the strongest promise the OS and storage device can make in a portable way.
It does not survive deliberate file tampering or kernel bugs.
