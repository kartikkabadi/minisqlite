//! Projection apply and read functions.
//!
//! Keys are opaque binary blobs ordered by SQLite's BLOB comparison (bytewise,
//! shorter-is-smaller on ties), which matches lexicographic byte order. Prefix
//! scans never rely on text collation: they are compiled to
//! `key >= prefix AND key < upper_bound`, where the upper bound is the prefix
//! with its last non-0xFF byte incremented (trailing 0xFF bytes dropped). A
//! prefix consisting entirely of 0xFF bytes has no finite upper bound, so the
//! scan is open-ended above.

use rusqlite::{params, Connection, OptionalExtension, Transaction};

use crate::error::{Conflict, Error, StorageError};
use crate::projection::{ProjectionEntry, ProjectionMutation, ProjectionPatch};

/// Apply one validated projection patch inside the commit transaction.
pub(crate) fn apply_projection_patch(
    tx: &Transaction<'_>,
    patch: &ProjectionPatch,
) -> Result<(), Error> {
    patch.validate()?;
    let actual = version_query(tx, &patch.projection)?;
    if actual != patch.expected_version {
        return Err(Error::Conflict(Conflict::ProjectionVersion {
            projection: patch.projection.clone(),
            expected: patch.expected_version,
            actual,
        }));
    }
    tx.execute(
        "INSERT INTO projection_meta (projection, version) VALUES (?1, ?2) ON CONFLICT(projection) DO UPDATE SET version = excluded.version",
        params![patch.projection, patch.new_version as i64],
    )
    .map_err(StorageError::from)?;
    for mutation in &patch.mutations {
        match mutation {
            ProjectionMutation::Put { key, value } => {
                tx.execute(
                    "INSERT INTO projection_entries (projection, key, value) VALUES (?1, ?2, ?3) ON CONFLICT(projection, key) DO UPDATE SET value = excluded.value",
                    params![patch.projection, key, value],
                )
                .map_err(StorageError::from)?;
            }
            ProjectionMutation::Delete { key } => {
                tx.execute(
                    "DELETE FROM projection_entries WHERE projection = ?1 AND key = ?2",
                    params![patch.projection, key],
                )
                .map_err(StorageError::from)?;
            }
            ProjectionMutation::Clear => {
                clear_entries(tx, &patch.projection)?;
            }
            ProjectionMutation::Replace { entries } => {
                clear_entries(tx, &patch.projection)?;
                for entry in entries {
                    tx.execute(
                        "INSERT INTO projection_entries (projection, key, value) VALUES (?1, ?2, ?3)",
                        params![patch.projection, entry.key, entry.value],
                    )
                    .map_err(StorageError::from)?;
                }
            }
        }
    }
    Ok(())
}

fn clear_entries(tx: &Transaction<'_>, projection: &str) -> Result<(), Error> {
    tx.execute(
        "DELETE FROM projection_entries WHERE projection = ?1",
        [projection],
    )
    .map_err(StorageError::from)?;
    Ok(())
}

fn version_query(conn: &Connection, projection: &str) -> Result<u64, Error> {
    let version: Option<i64> = conn
        .query_row(
            "SELECT version FROM projection_meta WHERE projection = ?1",
            [projection],
            |row| row.get(0),
        )
        .optional()
        .map_err(StorageError::from)?;
    Ok(version.unwrap_or(0) as u64)
}

/// The current version of a projection (0 when it does not exist).
pub(crate) fn projection_version(conn: &Connection, projection: &str) -> Result<u64, Error> {
    version_query(conn, projection)
}

/// Get one projection entry by key.
pub(crate) fn projection_get(
    conn: &Connection,
    projection: &str,
    key: &[u8],
) -> Result<Option<Vec<u8>>, Error> {
    conn.query_row(
        "SELECT value FROM projection_entries WHERE projection = ?1 AND key = ?2",
        params![projection, key],
        |row| row.get(0),
    )
    .optional()
    .map_err(|e| Error::Storage(StorageError::from(e)))
}

/// The smallest byte string strictly greater than every key starting with
/// `prefix`: the prefix with trailing 0xFF bytes dropped and the last remaining
/// byte incremented. `None` when no finite upper bound exists (empty prefix or
/// all bytes are 0xFF).
fn prefix_upper_bound(prefix: &[u8]) -> Option<Vec<u8>> {
    let mut bound = prefix.to_vec();
    while let Some(&last) = bound.last() {
        if last == 0xFF {
            bound.pop();
        } else {
            *bound.last_mut().expect("nonempty") = last + 1;
            return Some(bound);
        }
    }
    None
}

/// Scan entries with keys starting with `prefix`, in key order.
pub(crate) fn projection_scan_prefix(
    conn: &Connection,
    projection: &str,
    prefix: &[u8],
    limit: usize,
) -> Result<Vec<ProjectionEntry>, Error> {
    projection_scan_prefix_page(conn, projection, prefix, None, limit)
}

/// Paginated prefix scan: entries with keys starting with `prefix` and, when
/// `after` is given, strictly greater than `after`, in key order.
pub(crate) fn projection_scan_prefix_page(
    conn: &Connection,
    projection: &str,
    prefix: &[u8],
    after: Option<&[u8]>,
    limit: usize,
) -> Result<Vec<ProjectionEntry>, Error> {
    scan(
        conn,
        projection,
        Some(prefix),
        prefix_upper_bound(prefix).as_deref(),
        after,
        limit,
    )
}

/// Range scan: entries with `start <= key < end` (either bound optional) and,
/// when `after` is given, strictly greater than `after`, in key order.
pub(crate) fn projection_scan_range(
    conn: &Connection,
    projection: &str,
    start: Option<&[u8]>,
    end: Option<&[u8]>,
    after: Option<&[u8]>,
    limit: usize,
) -> Result<Vec<ProjectionEntry>, Error> {
    scan(conn, projection, start, end, after, limit)
}

fn scan(
    conn: &Connection,
    projection: &str,
    start: Option<&[u8]>,
    end: Option<&[u8]>,
    after: Option<&[u8]>,
    limit: usize,
) -> Result<Vec<ProjectionEntry>, Error> {
    let mut sql = String::from("SELECT key, value FROM projection_entries WHERE projection = ?1");
    let mut args: Vec<&dyn rusqlite::ToSql> = vec![&projection];
    if let Some(start) = &start {
        args.push(start);
        sql.push_str(&format!(" AND key >= ?{}", args.len()));
    }
    if let Some(end) = &end {
        args.push(end);
        sql.push_str(&format!(" AND key < ?{}", args.len()));
    }
    if let Some(after) = &after {
        args.push(after);
        sql.push_str(&format!(" AND key > ?{}", args.len()));
    }
    let limit_arg = limit as i64;
    args.push(&limit_arg);
    sql.push_str(&format!(" ORDER BY key LIMIT ?{}", args.len()));

    let mut stmt = conn.prepare(&sql).map_err(StorageError::from)?;
    let rows = stmt
        .query_map(args.as_slice(), |row| {
            Ok(ProjectionEntry {
                key: row.get(0)?,
                value: row.get(1)?,
            })
        })
        .map_err(StorageError::from)?;
    let mut entries = Vec::new();
    for row in rows {
        entries.push(row.map_err(StorageError::from)?);
    }
    Ok(entries)
}

/// List all projections and their versions.
pub(crate) fn projections_list(conn: &Connection) -> Result<Vec<(String, u64)>, Error> {
    let mut stmt = conn
        .prepare("SELECT projection, version FROM projection_meta ORDER BY projection")
        .map_err(StorageError::from)?;
    let rows = stmt
        .query_map([], |row| {
            Ok((row.get::<_, String>(0)?, row.get::<_, i64>(1)? as u64))
        })
        .map_err(StorageError::from)?;
    let mut list = Vec::new();
    for row in rows {
        list.push(row.map_err(StorageError::from)?);
    }
    Ok(list)
}

/// The number of entries in a projection (0 when it does not exist).
pub(crate) fn projection_entry_count(conn: &Connection, projection: &str) -> Result<u64, Error> {
    let count: i64 = conn
        .query_row(
            "SELECT COUNT(*) FROM projection_entries WHERE projection = ?1",
            [projection],
            |row| row.get(0),
        )
        .map_err(StorageError::from)?;
    Ok(count as u64)
}

#[cfg(test)]
mod tests {
    use super::prefix_upper_bound;

    #[test]
    fn upper_bound_increments_last_byte() {
        assert_eq!(prefix_upper_bound(b"ab"), Some(b"ac".to_vec()));
        assert_eq!(prefix_upper_bound(&[0x01, 0x02]), Some(vec![0x01, 0x03]));
    }

    #[test]
    fn upper_bound_drops_trailing_ff() {
        assert_eq!(prefix_upper_bound(&[0x01, 0xFF]), Some(vec![0x02]));
        assert_eq!(prefix_upper_bound(&[0x01, 0xFF, 0xFF]), Some(vec![0x02]));
    }

    #[test]
    fn upper_bound_absent_for_all_ff_or_empty() {
        assert_eq!(prefix_upper_bound(&[0xFF]), None);
        assert_eq!(prefix_upper_bound(&[0xFF, 0xFF]), None);
        assert_eq!(prefix_upper_bound(&[]), None);
    }
}
