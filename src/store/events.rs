use rusqlite::{Connection, OptionalExtension, Row, Transaction};

use crate::error::StorageError;
use crate::event::{Event, PersistedEvent};
use crate::id::Id;

/// Insert one event row at the given stream version. Called from the commit pipeline.
pub(crate) fn insert_event(
    tx: &Transaction<'_>,
    transaction_id: Id,
    event: &Event,
    stream_version: u64,
) -> Result<(), StorageError> {
    tx.execute(
        "INSERT INTO events (event_id, transaction_id, stream_id, stream_version, event_type, schema_version, occurred_at_ms, causation_id, correlation_id, payload, metadata) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11)",
        rusqlite::params![
            event.event_id.as_bytes().as_slice(),
            transaction_id.as_bytes().as_slice(),
            event.stream_id,
            stream_version as i64,
            event.event_type,
            event.schema_version,
            event.occurred_at_ms,
            event.causation_id.map(|id| id.0.to_vec()),
            event.correlation_id.map(|id| id.0.to_vec()),
            event.payload,
            event.metadata,
        ],
    ).map_err(StorageError::from_sqlite)?;
    Ok(())
}

/// Events with a global sequence strictly greater than `after`, oldest first.
pub(crate) fn events_after(
    conn: &Connection,
    after: u64,
    limit: usize,
) -> Result<Vec<PersistedEvent>, StorageError> {
    let mut stmt = conn.prepare(
        "SELECT global_sequence, event_id, transaction_id, stream_id, stream_version, event_type, schema_version, occurred_at_ms, causation_id, correlation_id, payload, metadata FROM events WHERE global_sequence > ?1 ORDER BY global_sequence LIMIT ?2",
    ).map_err(StorageError::from_sqlite)?;
    let rows = stmt
        .query_map(
            rusqlite::params![after as i64, limit as i64],
            row_to_persisted_event,
        )
        .map_err(StorageError::from_sqlite)?;
    collect(rows)
}

/// Events for one stream with a stream version of at least `from_version`, oldest first.
pub(crate) fn stream_events(
    conn: &Connection,
    stream_id: &str,
    from_version: u64,
    limit: usize,
) -> Result<Vec<PersistedEvent>, StorageError> {
    let mut stmt = conn.prepare(
        "SELECT global_sequence, event_id, transaction_id, stream_id, stream_version, event_type, schema_version, occurred_at_ms, causation_id, correlation_id, payload, metadata FROM events WHERE stream_id = ?1 AND stream_version >= ?2 ORDER BY stream_version LIMIT ?3",
    ).map_err(StorageError::from_sqlite)?;
    let rows = stmt
        .query_map(
            rusqlite::params![stream_id, from_version as i64, limit as i64],
            row_to_persisted_event,
        )
        .map_err(StorageError::from_sqlite)?;
    collect(rows)
}

/// Look up one event by its ID.
pub(crate) fn get_event(
    conn: &Connection,
    event_id: Id,
) -> Result<Option<PersistedEvent>, StorageError> {
    let event = conn
        .query_row(
            "SELECT global_sequence, event_id, transaction_id, stream_id, stream_version, event_type, schema_version, occurred_at_ms, causation_id, correlation_id, payload, metadata FROM events WHERE event_id = ?1",
            [event_id.as_bytes().as_slice()],
            row_to_persisted_event,
        )
        .optional().map_err(StorageError::from_sqlite)?;
    Ok(event)
}

/// The current durable version of a stream (0 when the stream does not exist).
pub(crate) fn stream_version(conn: &Connection, stream_id: &str) -> Result<u64, StorageError> {
    let version: Option<i64> = conn
        .query_row(
            "SELECT current_version FROM streams WHERE stream_id = ?1",
            [stream_id],
            |row| row.get(0),
        )
        .optional()
        .map_err(StorageError::from_sqlite)?;
    Ok(version.unwrap_or(0) as u64)
}

fn collect(
    rows: impl Iterator<Item = Result<PersistedEvent, rusqlite::Error>>,
) -> Result<Vec<PersistedEvent>, StorageError> {
    let mut out = Vec::new();
    for row in rows {
        out.push(row.map_err(StorageError::from_sqlite)?);
    }
    Ok(out)
}

fn row_to_persisted_event(row: &Row<'_>) -> Result<PersistedEvent, rusqlite::Error> {
    let global_sequence: i64 = row.get(0)?;
    let stream_version: i64 = row.get(4)?;
    Ok(PersistedEvent {
        transaction_id: blob_to_id(row.get(2)?),
        global_sequence: global_sequence as u64,
        stream_version: stream_version as u64,
        event: Event {
            event_id: blob_to_id(row.get(1)?),
            stream_id: row.get(3)?,
            event_type: row.get(5)?,
            schema_version: row.get(6)?,
            occurred_at_ms: row.get(7)?,
            causation_id: row.get::<_, Option<Vec<u8>>>(8)?.map(blob_to_id),
            correlation_id: row.get::<_, Option<Vec<u8>>>(9)?.map(blob_to_id),
            payload: row.get(10)?,
            metadata: row.get(11)?,
        },
    })
}

fn blob_to_id(blob: Vec<u8>) -> Id {
    let mut bytes = [0u8; 16];
    if blob.len() == 16 {
        bytes.copy_from_slice(&blob);
    }
    Id::from_bytes(bytes)
}
