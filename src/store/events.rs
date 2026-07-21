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

/// The most recent `limit` events, oldest first.
pub(crate) fn last_events(
    conn: &Connection,
    limit: usize,
) -> Result<Vec<PersistedEvent>, StorageError> {
    let mut stmt = conn.prepare(
        "SELECT global_sequence, event_id, transaction_id, stream_id, stream_version, event_type, schema_version, occurred_at_ms, causation_id, correlation_id, payload, metadata FROM events ORDER BY global_sequence DESC LIMIT ?1",
    ).map_err(StorageError::from_sqlite)?;
    let rows = stmt
        .query_map([limit as i64], row_to_persisted_event)
        .map_err(StorageError::from_sqlite)?;
    let mut events = collect(rows)?;
    events.reverse();
    Ok(events)
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
        .optional()
        .map_err(StorageError::from_sqlite)?;
    event.transpose()
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
    rows: impl Iterator<Item = Result<Result<PersistedEvent, StorageError>, rusqlite::Error>>,
) -> Result<Vec<PersistedEvent>, StorageError> {
    let mut out = Vec::new();
    for row in rows {
        out.push(row.map_err(StorageError::from_sqlite)??);
    }
    Ok(out)
}

fn row_to_persisted_event(
    row: &Row<'_>,
) -> Result<Result<PersistedEvent, StorageError>, rusqlite::Error> {
    let global_sequence: i64 = row.get(0)?;
    let stream_version: i64 = row.get(4)?;
    let transaction_id: Vec<u8> = row.get(2)?;
    let event_id: Vec<u8> = row.get(1)?;
    let stream_id: String = row.get(3)?;
    let event_type: String = row.get(5)?;
    let schema_version: u32 = row.get(6)?;
    let occurred_at_ms: i64 = row.get(7)?;
    let causation_id: Option<Vec<u8>> = row.get(8)?;
    let correlation_id: Option<Vec<u8>> = row.get(9)?;
    let payload: Vec<u8> = row.get(10)?;
    let metadata: Vec<u8> = row.get(11)?;
    Ok((|| {
        Ok(PersistedEvent {
            transaction_id: blob_to_id(transaction_id)?,
            global_sequence: global_sequence as u64,
            stream_version: stream_version as u64,
            event: Event {
                event_id: blob_to_id(event_id)?,
                stream_id,
                event_type,
                schema_version,
                occurred_at_ms,
                causation_id: causation_id.map(blob_to_id).transpose()?,
                correlation_id: correlation_id.map(blob_to_id).transpose()?,
                payload,
                metadata,
            },
        })
    })())
}

fn blob_to_id(blob: Vec<u8>) -> Result<Id, StorageError> {
    let bytes: [u8; 16] = blob
        .try_into()
        .map_err(|_| StorageError::Sqlite("corrupt 16-byte id column".into()))?;
    Ok(Id::from_bytes(bytes))
}
