use crate::id::Id;

/// A domain event as seen by the application before it is persisted.
#[derive(Debug, Clone, PartialEq, Eq)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub struct Event {
    pub event_id: Id,
    pub stream_id: String,
    pub event_type: String,
    pub schema_version: u32,
    pub occurred_at_ms: i64,
    pub causation_id: Option<Id>,
    pub correlation_id: Option<Id>,
    pub payload: Vec<u8>,
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

impl StreamVersion {
    /// Create a stream version marker.
    pub fn new(stream_id: impl Into<String>, version: u64) -> Self {
        Self {
            stream_id: stream_id.into(),
            version,
        }
    }
}

/// An event after it has been assigned a global sequence and stream version.
#[derive(Debug, Clone, PartialEq, Eq)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub struct PersistedEvent {
    pub transaction_id: Id,
    pub global_sequence: u64,
    pub stream_version: u64,
    pub event: Event,
    /// File offset of the transaction frame containing this event.
    pub frame_offset: u64,
}

/// The current version of a stream.
#[derive(Debug, Clone, PartialEq, Eq)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub struct StreamVersion {
    pub stream_id: String,
    pub version: u64,
}
