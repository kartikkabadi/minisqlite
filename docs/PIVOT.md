# Product Pivot: From SQLite-like SQL to Control-Plane State Engine

## What is being deleted

The existing implementation was a generic, SQLite-like SQL engine with:

* SQL tokenizer, parser, AST, and executor
* Relational tables, indexes, constraints, and joins
* Aggregates, `GROUP BY`, `ORDER BY`, `LIMIT`, `OFFSET`, `DISTINCT`
* A SQLite-compatible syntax aspiration
* A 4 KB page-based B+ tree storage layer
* Decorative WAL code
* Interactive SQL shell / REPL
* Generic `VACUUM`, DDL, and ORM positioning

These are deleted on the feature branch, not moved to a `legacy/` directory.

## Why delete them

The target customer is not asking for a smaller SQL database. The target customer is a developer building a local-first AI application who needs durable control-plane state: ordered events, materialized current state, and durable background work. A generic SQL engine forces the developer to build the control plane on top, which is exactly the scaffolding that causes startup failures, missing projections, and duplicated side effects in practice.

Generic relational features have a high *Software Idiot Index* for this wedge: large implementation and operational complexity for unique outcomes that can be achieved with a much smaller, purpose-built append-only kernel.

## What is being kept

* One primary data file
* Append-only transaction frames
* Atomic batches
* Idempotent transaction IDs
* Optimistic stream versions
* Materialized named maps
* Durable jobs and leases
* An inspection and recovery CLI

## What is deliberately out of scope

* SQL or any query language
* SQLite file compatibility
* Distributed consensus, replication, or multi-process writes
* Vector search
* Built-in model calls
* Workflow-language runtime
* Web dashboard
* Automatic background scheduler
* Snapshots and compaction (deferred until replay is measured)
* Encryption at rest
* Cloud synchronization
* Blob storage

The first product is not "a database." It is a durable local control-plane kernel.
