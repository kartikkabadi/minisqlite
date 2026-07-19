# minisqlite

A minimal, from-scratch SQLite-like relational database engine written in Rust.

## Features

- 4096-byte page-based storage with a custom file format
- In-memory B+tree-like tables serialized to linked pages
- Custom recursive-descent SQL tokenizer and parser
- DDL: `CREATE TABLE`, `CREATE INDEX`, `ALTER TABLE ADD COLUMN`, `DROP TABLE`, `DROP INDEX`
- DML: `INSERT`, `UPDATE`, `DELETE` with `OR REPLACE`
- Queries: `SELECT` with joins, `WHERE`, `GROUP BY`, `HAVING`, `ORDER BY`, `LIMIT`/`OFFSET`, `DISTINCT`, aggregates, `CASE`, `CAST`, `BETWEEN`, `LIKE`, `IN`
- Transactions: `BEGIN`, `COMMIT`, `ROLLBACK`
- Dot commands: `.tables`, `.schema`, `.indexes`, `.stats`, `.help`, `.quit`
- `PRAGMA table_info(name)` and `VACUUM`
- No external dependencies (Rust standard library only)

## Build

```bash
cargo build
```

## Run

```bash
cargo run -- mydb.db
```

## Example

```sql
CREATE TABLE employees (
  id INTEGER PRIMARY KEY,
  name TEXT,
  department TEXT,
  salary REAL
);

INSERT INTO employees (name, department, salary) VALUES
  ('Alice', 'Engineering', 100000),
  ('Bob', 'Engineering', 120000),
  ('Charlie', 'Sales', 80000);

SELECT department, AVG(salary) AS avg_sal
FROM employees
GROUP BY department
ORDER BY avg_sal DESC;

.quit
```

## Test

A sample SQL script is included:

```bash
cargo run -- test.db < test.sql
```
