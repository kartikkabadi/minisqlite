# minisqlite

[![Crates.io](https://img.shields.io/crates/v/minisqlite?logo=rust&label=crates.io)](https://crates.io/crates/minisqlite)
[![Docs.rs](https://img.shields.io/docsrs/minisqlite?logo=rust&label=docs.rs)](https://docs.rs/minisqlite)
[![CI](https://github.com/kartikkabadi/minisqlite/actions/workflows/ci.yml/badge.svg)](https://github.com/kartikkabadi/minisqlite/actions/workflows/ci.yml)

A minimal, from-scratch SQLite-like relational database engine written in Rust.

> **Direction:** the project is pivoting to a SQLite-backed control-plane
> state kernel. See [ADR-001](docs/ADR-001.md) for the storage substrate
> decision, plus the [roadmap](docs/ROADMAP.md) and [scope](docs/SCOPE.md).
>
> **Fate of the SQL engine:** the from-scratch SQL engine and its public SQL
> API described below are frozen at **v0.2.1** — preserved on
> [crates.io](https://crates.io/crates/minisqlite/0.2.1) and in this
> repository's history — and are **removed in the 0.3 control-plane pivot**.
> The 0.3 kernel links against C SQLite and exposes a typed API, not
> arbitrary SQL. (The intermediate custom-journal rewrite is archived at
> `archive/append-only-journal-v1`, tag `journal-v1-experimental`.)

The remainder of this README describes the **v0.2.1 SQL engine**: a page-based storage engine with a custom file format, zero external dependencies, pure safe Rust.

## Install

```bash
cargo install minisqlite
```

Or add it as a library dependency:

```toml
[dependencies]
minisqlite = "0.2.1"
```

## Features

- 4096-byte page-based storage with a custom file format (`MiniSQL2`)
- In-memory B+tree-like tables serialized to linked pages
- Custom recursive-descent SQL tokenizer and parser
- DDL: `CREATE TABLE`, `CREATE INDEX`, `ALTER TABLE ADD COLUMN`, `DROP TABLE`, `DROP INDEX`
- DML: `INSERT`, `UPDATE`, `DELETE` with `OR REPLACE`
- Queries: `SELECT` with joins, `WHERE`, `GROUP BY`, `HAVING`, `ORDER BY`, `LIMIT`/`OFFSET`, `DISTINCT`, aggregates, `CASE`, `CAST`, `BETWEEN`, `LIKE`, `IN`
- Transactions: `BEGIN`, `COMMIT`, `ROLLBACK`
- Dot commands: `.tables`, `.schema`, `.indexes`, `.dump`, `.stats`, `.help`, `.quit`
- `PRAGMA table_info(name)` and `VACUUM`
- No external dependencies (Rust standard library only)

## CLI

```bash
minisqlite mydb.db
```

Or from source:

```bash
cargo run -- mydb.db
```

## Library

```rust
use minisqlite::{Database, ExecuteResult};

let mut db = Database::open("mydb.db").unwrap();
db.execute_sql("CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT)")
    .unwrap();
db.execute_sql("INSERT INTO users (name) VALUES ('Alice')")
    .unwrap();

if let ExecuteResult::Rows { header, rows } =
    db.execute_sql("SELECT * FROM users").unwrap()
{
    println!("{:?}", header);
    for row in rows {
        println!("{:?}", row);
    }
}
```

See [`examples/embed.rs`](examples/embed.rs) for a runnable example.

## Test

```bash
cargo test
cargo run -- test.db < test.sql
```

## Changelog

See [CHANGELOG.md](CHANGELOG.md).

## License

MIT
