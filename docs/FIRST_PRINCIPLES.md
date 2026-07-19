# First-Principles Design

## Customer

A developer building a local-first AI desktop or CLI application.

## Painful job

Persist user-visible state and coordinate long-running side effects across restarts.

## Broken current workflow

The developer starts with a generic database or files, then separately builds:

* An event store
* Sequence allocation
* Stream versions
* Idempotency
* Materialized read models
* Projection cursors
* Durable jobs
* Job leases
* Retries
* Dead-letter handling
* Uncertain-outcome handling
* Schema migrations
* Projection rebuilding
* Backup procedures
* Crash recovery
* Diagnostic tooling
* Runtime-specific database wrappers

## Consequences

More application code, more state machines, more migration risk, more impossible intermediate states, more startup and upgrade failures, duplicated side effects, lost queued work, projections that disagree with source data, and poor diagnostics when the local database fails.

## Product promise

> MiniSQLite replaces that persistence scaffolding with a small, embedded, append-only state kernel.

## Physics and system truths

1. **Storage is not instantaneous.** A commit may be interrupted before, during, or after any byte, header, payload, trailer, or sync. Normal commits must avoid in-place mutation so that an interrupted append leaves previously committed state intact.
2. **External side effects cannot honestly be called exactly-once.** If a worker succeeds externally but crashes before acknowledging locally, the system cannot know the outcome without an idempotency key or a queryable external identity. MiniSQLite supports idempotency keys and explicit uncertain outcomes instead of silent retry.
3. **The source of truth should be immutable.** Mutable tables require more coordination, migration, and recovery machinery. The durable representation is an ordered append-only sequence of committed transaction frames; current state is derived or materialized from those frames.
4. **The first workload is single-machine and single-owner.** One process owns the store. Writes are serialized inside that process. Multiple in-process readers are allowed. There is no distributed consensus, replication, leader election, or multi-primary coordination.
5. **Access patterns are known.** Read events after a sequence, read one stream by ID and version, get/scan projected keys by collection, find ready jobs, claim jobs by queue and partition, and inspect failures/uncertain jobs. Arbitrary relational queries are not needed.
6. **Large files are not application state.** MiniSQLite stores references and metadata, not repository contents, terminal recordings, images, or artifacts.
