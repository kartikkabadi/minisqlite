use std::path::Path;

use rusqlite::Connection;

use crate::config::Durability;
use crate::error::StorageError;

/// Open (or create) the SQLite database at `path` and apply the kernel's pragmas.
pub(crate) fn open(path: &Path, durability: Durability) -> Result<Connection, StorageError> {
    let conn = Connection::open(path).map_err(StorageError::from_sqlite)?;
    apply_pragmas(&conn, durability)?;
    Ok(conn)
}

pub(crate) fn apply_pragmas(conn: &Connection, durability: Durability) -> Result<(), StorageError> {
    // journal_mode returns the resulting mode as a row, so it needs pragma_update-style query.
    let _mode: String = conn
        .query_row("PRAGMA journal_mode=WAL", [], |row| row.get(0))
        .map_err(StorageError::from_sqlite)?;
    conn.execute_batch(&format!(
        "PRAGMA foreign_keys=ON;\n\
         PRAGMA trusted_schema=OFF;\n\
         PRAGMA busy_timeout=5000;\n\
         PRAGMA synchronous={};",
        durability.synchronous_pragma()
    ))
    .map_err(StorageError::from_sqlite)?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn pragma_i64(conn: &Connection, name: &str) -> i64 {
        conn.query_row(&format!("PRAGMA {name}"), [], |row| row.get(0))
            .unwrap()
    }

    #[test]
    fn strict_durability_maps_to_synchronous_full() {
        let dir = tempfile::tempdir().unwrap();
        let conn = open(&dir.path().join("db"), Durability::Strict).unwrap();
        assert_eq!(pragma_i64(&conn, "synchronous"), 2); // FULL
    }

    #[test]
    fn relaxed_durability_maps_to_synchronous_normal() {
        let dir = tempfile::tempdir().unwrap();
        let conn = open(&dir.path().join("db"), Durability::Relaxed).unwrap();
        assert_eq!(pragma_i64(&conn, "synchronous"), 1); // NORMAL
    }

    #[test]
    fn base_pragmas_applied() {
        let dir = tempfile::tempdir().unwrap();
        let conn = open(&dir.path().join("db"), Durability::Strict).unwrap();
        let mode: String = conn
            .query_row("PRAGMA journal_mode", [], |row| row.get(0))
            .unwrap();
        assert_eq!(mode, "wal");
        assert_eq!(pragma_i64(&conn, "foreign_keys"), 1);
        assert_eq!(pragma_i64(&conn, "trusted_schema"), 0);
        assert_eq!(pragma_i64(&conn, "busy_timeout"), 5000);
    }
}
