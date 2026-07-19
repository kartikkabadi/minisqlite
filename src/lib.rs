//! `minisqlite` is a minimal, from-scratch SQLite-like relational database engine written in Rust.
//!
//! It is intentionally tiny: zero external dependencies, pure safe Rust, and a single-threaded,
//! page-based storage engine. Use it when you need a small, hackable, embeddable SQL engine
//! without linking to C SQLite.
//!
//! # Example
//!
//! ```rust,no_run
//! use minisqlite::Database;
//!
//! let mut db = Database::open("mydb.db").unwrap();
//! // ... execute SQL statements via the (currently internal) executor
//! ```

pub mod btree;
pub mod catalog;
pub mod executor;
pub mod functions;
pub mod pager;
pub mod sql;
pub mod types;
pub mod wal;

pub use executor::{Database, ExecuteResult};
pub use sql::Parser;
pub use types::Value;
