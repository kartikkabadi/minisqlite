# Changelog

## 0.2.1

- Polished public API and library documentation
- Fixed all `cargo clippy` warnings and applied `cargo fmt`
- Added `hex_encode` helper for consistent blob rendering
- Improved README with crates.io badges, install instructions, and product positioning

## 0.2.0

- Initial public release of `minisqlite`
- Page-based storage with a custom `MiniSQL2` file format
- SQL parser and executor supporting DDL, DML, `SELECT`, joins, aggregates, transactions, `PRAGMA`, and dot commands
- Pure Rust, zero external dependencies
- Library + CLI crate structure
- Examples and integration tests
