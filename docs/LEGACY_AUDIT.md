# Legacy Module Audit

This audit classifies every major component in the pre-pivot `minisqlite` repository. The default decision is `delete` because the product is no longer a SQL engine.

| Module/File | Decision | Rationale |
| --- | --- | --- |
| `src/btree.rs` | delete | In-memory B+ tree used for generic table/index storage. The new engine uses append-only frames and in-memory projections, not a B+ tree. |
| `src/catalog.rs` | delete | SQL catalog of tables, indexes, and columns. No SQL schema exists in the new kernel. |
| `src/executor.rs` | delete | SQL statement executor. Replaced by an event/projection/job transaction kernel. |
| `src/functions.rs` | delete | SQL scalar and aggregate functions. No SQL surface. |
| `src/pager.rs` | delete | SQLite-style 4 KB page manager. New storage is append-only, page-free, and frame-based. |
| `src/sql.rs` | delete | SQL tokenizer, parser, and AST. The new product has no query language. |
| `src/types.rs` | delete | SQL `Value` enum and type system. New kernel uses opaque byte payloads; types live in application domain code. |
| `src/wal.rs` | delete | Decorative WAL stub. New durability is provided by append-only transaction frames and explicit sync. |
| `src/lib.rs` | rewrite | Re-export the new public API: `Store`, `StoreBuilder`, `CommitBatch`, `Event`, `JobSpec`, etc. |
| `src/main.rs` | rewrite | Replace SQL REPL with an operational CLI: `doctor`, `verify`, `stats`, `events`, `projections`, `jobs`, `export`, `backup`. |
| `Cargo.toml` | update | Bump to `0.3.0-alpha.1`, update description, keywords, categories, add justified dependencies. |
| `tests/integration.rs` | rewrite | New integration tests for commit, recovery, projections, jobs, and the Synara-shaped flow. |
| `README.md` | rewrite | State the pivot and the new use case. |
| `CHANGELOG.md` | update | Record the breaking pivot. |

## Reused components

No major legacy module is reused. The only reused ideas are:

* Safe Rust and a small dependency budget.
* A single primary data file.
* Explicit, caller-supplied timestamps.

Everything else is rebuilt from the storage primitives upward.
