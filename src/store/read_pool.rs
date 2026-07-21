//! A small pool of read-only SQLite connections (plan §7.1).
//!
//! Reads run on pooled read-only connections so they never contend with the
//! single writer for its mutex, and WAL mode lets them proceed during writes.

use std::path::PathBuf;
use std::sync::Mutex;

use rusqlite::Connection;

use crate::error::StorageError;
use crate::store::connection;

/// Default number of idle read connections retained by the pool.
pub(crate) const DEFAULT_READ_POOL_SIZE: usize = 4;

#[derive(Debug)]
pub(crate) struct ReadPool {
    path: PathBuf,
    idle: Mutex<Vec<Connection>>,
    max_idle: usize,
}

impl ReadPool {
    pub(crate) fn new(path: PathBuf, max_idle: usize) -> Self {
        Self {
            path,
            idle: Mutex::new(Vec::new()),
            max_idle,
        }
    }

    /// Take a read-only connection out of the pool, opening a new one when the
    /// pool is empty. Return it with [`ReadPool::put`] when done.
    pub(crate) fn take(&self) -> Result<Connection, StorageError> {
        match self.lock().pop() {
            Some(conn) => Ok(conn),
            None => connection::open_reader(&self.path),
        }
    }

    /// Return a connection to the pool (up to `max_idle` retained).
    pub(crate) fn put(&self, conn: Connection) {
        let mut idle = self.lock();
        if idle.len() < self.max_idle {
            idle.push(conn);
        }
    }

    fn lock(&self) -> std::sync::MutexGuard<'_, Vec<Connection>> {
        // Vec<Connection> holds no invariants across panics; recover the inner value.
        match self.idle.lock() {
            Ok(guard) => guard,
            Err(poisoned) => poisoned.into_inner(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn create_db(path: &std::path::Path) {
        let conn = connection::open(path, crate::config::Durability::Strict).unwrap();
        conn.execute_batch("CREATE TABLE t (x INTEGER); INSERT INTO t VALUES (7);")
            .unwrap();
    }

    #[test]
    fn pool_reuses_connections_up_to_max_idle() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("db");
        create_db(&path);
        let pool = ReadPool::new(path, 2);
        let a = pool.take().unwrap();
        let b = pool.take().unwrap();
        let c = pool.take().unwrap();
        for conn in [&a, &b, &c] {
            let x: i64 = conn.query_row("SELECT x FROM t", [], |r| r.get(0)).unwrap();
            assert_eq!(x, 7);
        }
        pool.put(a);
        pool.put(b);
        pool.put(c);
        assert_eq!(pool.lock().len(), 2); // third connection was discarded
    }

    #[test]
    fn pooled_connections_are_read_only() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("db");
        create_db(&path);
        let pool = ReadPool::new(path, 1);
        let reader = pool.take().unwrap();
        assert!(reader.execute("INSERT INTO t VALUES (8)", []).is_err());
        pool.put(reader);
    }

    #[test]
    fn missing_database_is_an_error() {
        let dir = tempfile::tempdir().unwrap();
        let pool = ReadPool::new(dir.path().join("missing"), 1);
        assert!(pool.take().is_err());
    }
}
