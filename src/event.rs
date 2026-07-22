use crate::id::Id;

/// A domain event as seen by the application before it is persisted.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Event {
    /// Globally unique identifier for the event.
    pub event_id: Id,
    /// The stream the event is appended to.
    pub stream_id: String,
    /// Application-defined type name of the event.
    pub event_type: String,
    /// Version of the payload schema, for consumer-side evolution.
    pub schema_version: u32,
    /// Caller-supplied wall-clock time at which the event occurred.
    pub occurred_at_ms: i64,
    /// ID of the event or command that directly caused this event, if any.
    pub causation_id: Option<Id>,
    /// ID linking related events across streams, if any.
    pub correlation_id: Option<Id>,
    /// Opaque event payload bytes.
    pub payload: Vec<u8>,
    /// Opaque metadata bytes stored alongside the payload.
    pub metadata: Vec<u8>,
}

impl Event {
    /// Construct an event with all fields.
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        event_id: Id,
        stream_id: impl Into<String>,
        event_type: impl Into<String>,
        schema_version: u32,
        occurred_at_ms: i64,
        causation_id: Option<Id>,
        correlation_id: Option<Id>,
        payload: &[u8],
        metadata: &[u8],
    ) -> Self {
        Self {
            event_id,
            stream_id: stream_id.into(),
            event_type: event_type.into(),
            schema_version,
            occurred_at_ms,
            causation_id,
            correlation_id,
            payload: payload.to_vec(),
            metadata: metadata.to_vec(),
        }
    }

    /// Helper for a small JSON-ish payload with caller-supplied wall-clock time.
    pub fn with_json_payload(
        event_id: Id,
        stream_id: impl Into<String>,
        event_type: impl Into<String>,
        occurred_at_ms: i64,
        payload: &[u8],
    ) -> Self {
        Self::new(
            event_id,
            stream_id,
            event_type,
            1,
            occurred_at_ms,
            None,
            None,
            payload,
            &[],
        )
    }
}

/// An event after it has been assigned a global sequence and stream version.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PersistedEvent {
    /// The transaction that committed the event.
    pub transaction_id: Id,
    /// The event's position in the store-wide total order.
    pub global_sequence: u64,
    /// The event's position within its stream.
    pub stream_version: u64,
    /// The event as supplied by the application.
    pub event: Event,
}

/// The current version of a stream.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StreamVersion {
    /// The stream being described.
    pub stream_id: String,
    /// The stream's current version (0 if no events).
    pub version: u64,
}

impl StreamVersion {
    /// Create a stream version marker.
    pub fn new(stream_id: impl Into<String>, version: u64) -> Self {
        Self {
            stream_id: stream_id.into(),
            version,
        }
    }
}
