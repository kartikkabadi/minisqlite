# minisqlite

A minimal, from-scratch SQLite-like relational database engine written in Rust.

`minisqlite` is intentionally tiny: zero external dependencies, pure safe Rust, and a page-based storage engine with a custom file format. It is built for situations where pulling in C SQLite is overkill or impossible:

- **WASM / browser targets** – no `libsqlite3-sys` to emscripten.
- **Embedded / IoT** – easy to audit, easy to cross-compile.
- **Education and prototyping** – the whole engine fits in a few thousand lines and a single crate.
- **Serverless edge functions** – self-contained file storage with no native shared library.

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

## Build

```bash
cargo build
```

## CLI

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

## License

MIT
