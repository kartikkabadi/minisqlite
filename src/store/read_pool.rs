//! A small pool of read-only SQLite connections (plan §7.1).
//!
//! Reads run on pooled read-only connections so they never contend with the
//! single writer for its mutex, and WAL mode lets them proceed during writes.

use std::ops::Deref;
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

    /// Borrow a read-only connection, opening a new one when the pool is empty.
    /// The connection returns to the pool on drop (up to `max_idle` retained).
    pub(crate) fn get(&self) -> Result<PooledReader<'_>, StorageError> {
        let reused = self.lock().pop();
        let conn = match reused {
            Some(conn) => conn,
            None => connection::open_reader(&self.path)?,
        };
        Ok(PooledReader {
            conn: Some(conn),
            pool: self,
        })
    }

    fn lock(&self) -> std::sync::MutexGuard<'_, Vec<Connection>> {
        // Vec<Connection> holds no invariants across panics; recover the inner value.
        match self.idle.lock() {
            Ok(guard) => guard,
            Err(poisoned) => poisoned.into_inner(),
        }
    }
}

/// A read-only connection borrowed from a [`ReadPool`].
pub(crate) struct PooledReader<'a> {
    conn: Option<Connection>,
    pool: &'a ReadPool,
}

impl Deref for PooledReader<'_> {
    type Target = Connection;

    fn deref(&self) -> &Connection {
        self.conn
            .as_ref()
            .expect("connection present until dropped")
    }
}

impl Drop for PooledReader<'_> {
    fn drop(&mut self) {
        if let Some(conn) = self.conn.take() {
            let mut idle = self.pool.lock();
            if idle.len() < self.pool.max_idle {
                idle.push(conn);
            }
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
        {
            let a = pool.get().unwrap();
            let b = pool.get().unwrap();
            let c = pool.get().unwrap();
            for conn in [&a, &b, &c] {
                let x: i64 = conn.query_row("SELECT x FROM t", [], |r| r.get(0)).unwrap();
                assert_eq!(x, 7);
            }
        }
        assert_eq!(pool.lock().len(), 2); // third connection was discarded
    }

    #[test]
    fn pooled_connections_are_read_only() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("db");
        create_db(&path);
        let pool = ReadPool::new(path, 1);
        let reader = pool.get().unwrap();
        assert!(reader.execute("INSERT INTO t VALUES (8)", []).is_err());
    }

    #[test]
    fn missing_database_is_an_error() {
        let dir = tempfile::tempdir().unwrap();
        let pool = ReadPool::new(dir.path().join("missing"), 1);
        assert!(pool.get().is_err());
    }
}
