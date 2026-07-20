use crate::codec::frame::{FRAME_HEADER_SIZE, FRAME_TRAILER_SIZE, MAX_FRAME_SIZE};
use crate::codec::record::{MAX_RECORDS_PER_FRAME, MAX_REPLACE_ENTRIES_PER_RECORD};

/// Controls how strictly a commit synchronizes to durable storage.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub enum Durability {
    /// Synchronize the appended frame before reporting success. This is the default.
    #[default]
    Strict,
    /// Do not synchronize. Useful for tests or ephemeral instances. Must be explicitly selected.
    Memory,
}

impl Durability {
    pub(crate) fn requires_sync(self) -> bool {
        matches!(self, Durability::Strict)
    }
}

/// Whether a job's external effect can safely be retried.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub enum EffectMode {
    /// The effect is idempotent. An expired lease may be claimed and retried.
    #[default]
    Idempotent,
    /// The effect is non-idempotent. An expired lease must be explicitly resolved.
    UncertainOnLeaseExpiry,
}

/// Size and shape limits for a store.
///
/// These are intentionally conservative. The first workload is bounded control-plane metadata,
/// not arbitrary blobs. Limits are enforced before any bytes are written.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub struct Limits {
    /// Maximum event payload in bytes.
    pub max_event_payload: usize,
    /// Maximum event metadata in bytes.
    pub max_metadata: usize,
    /// Maximum projection key in bytes.
    pub max_projection_key: usize,
    /// Maximum projection value in bytes.
    pub max_projection_value: usize,
    /// Maximum job payload in bytes.
    pub max_job_payload: usize,
    /// Maximum records in a single transaction frame.
    pub max_records_per_transaction: usize,
    /// Maximum total transaction frame size in bytes.
    pub max_frame_size: usize,
    /// Maximum length of any UTF-8 string field (stream ID, event type, queue, partition, etc.).
    pub max_string_len: usize,
    /// Maximum error summary or diagnostic string length.
    pub max_summary_len: usize,
    /// Maximum number of entries in a projection replacement.
    pub max_replace_entries: usize,
}

impl Default for Limits {
    fn default() -> Self {
        Self {
            max_event_payload: 1 << 20,    // 1 MiB
            max_metadata: 64 << 10,        // 64 KiB
            max_projection_key: 1 << 20,   // 1 MiB
            max_projection_value: 4 << 20, // 4 MiB
            max_job_payload: 1 << 20,      // 1 MiB
            max_records_per_transaction: 1024,
            max_frame_size: 16 << 20, // 16 MiB
            max_string_len: 4096,
            max_summary_len: 4096,
            max_replace_entries: 10_000,
        }
    }
}

impl Limits {
    /// Returns the default limits.
    pub const fn new() -> Self {
        Self {
            max_event_payload: 1 << 20,
            max_metadata: 64 << 10,
            max_projection_key: 1 << 20,
            max_projection_value: 4 << 20,
            max_job_payload: 1 << 20,
            max_records_per_transaction: 1024,
            max_frame_size: 16 << 20,
            max_string_len: 4096,
            max_summary_len: 4096,
            max_replace_entries: 10_000,
        }
    }

    pub(crate) fn validate_event(
        &self,
        payload_len: usize,
        metadata_len: usize,
    ) -> crate::Result<()> {
        if payload_len > self.max_event_payload {
            return Err(crate::Error::PayloadTooLarge {
                kind: "event payload",
                size: payload_len,
                limit: self.max_event_payload,
            });
        }
        self.validate_metadata(metadata_len)?;
        Ok(())
    }

    pub(crate) fn validate_metadata(&self, len: usize) -> crate::Result<()> {
        if len > self.max_metadata {
            return Err(crate::Error::PayloadTooLarge {
                kind: "metadata",
                size: len,
                limit: self.max_metadata,
            });
        }
        Ok(())
    }

    pub(crate) fn validate_projection_key(&self, len: usize) -> crate::Result<()> {
        if len > self.max_projection_key {
            return Err(crate::Error::PayloadTooLarge {
                kind: "projection key",
                size: len,
                limit: self.max_projection_key,
            });
        }
        Ok(())
    }

    pub(crate) fn validate_projection_value(&self, len: usize) -> crate::Result<()> {
        if len > self.max_projection_value {
            return Err(crate::Error::PayloadTooLarge {
                kind: "projection value",
                size: len,
                limit: self.max_projection_value,
            });
        }
        Ok(())
    }

    pub(crate) fn validate_job_payload(&self, len: usize) -> crate::Result<()> {
        if len > self.max_job_payload {
            return Err(crate::Error::PayloadTooLarge {
                kind: "job payload",
                size: len,
                limit: self.max_job_payload,
            });
        }
        Ok(())
    }

    pub(crate) fn validate_string(&self, field: &'static str, s: &str) -> crate::Result<()> {
        if s.len() > self.max_string_len {
            return Err(crate::Error::PayloadTooLarge {
                kind: field,
                size: s.len(),
                limit: self.max_string_len,
            });
        }
        Ok(())
    }

    pub(crate) fn validate_summary(&self, s: &str) -> crate::Result<()> {
        if s.len() > self.max_summary_len {
            return Err(crate::Error::PayloadTooLarge {
                kind: "error summary",
                size: s.len(),
                limit: self.max_summary_len,
            });
        }
        Ok(())
    }

    /// Validate that the configured limits fit within the hard frame-size bound and are
    /// internally consistent.
    pub fn validate(&self) -> crate::Result<()> {
        if self.max_frame_size > MAX_FRAME_SIZE {
            return Err(crate::Error::Validation(format!(
                "max_frame_size {} exceeds hard limit {}",
                self.max_frame_size, MAX_FRAME_SIZE
            )));
        }
        // The minimum frame must hold at least one fixed-size internal record (e.g.
        // `JobExpire`) in addition to header and trailer.
        const MIN_RECORD_BYTES: usize = 64;
        if self.max_frame_size < FRAME_HEADER_SIZE + FRAME_TRAILER_SIZE + MIN_RECORD_BYTES {
            return Err(crate::Error::Validation(format!(
                "max_frame_size {} is smaller than the minimum frame size",
                self.max_frame_size
            )));
        }
        if self.max_records_per_transaction == 0 {
            return Err(crate::Error::Validation(
                "max_records_per_transaction must be greater than 0".into(),
            ));
        }
        if self.max_records_per_transaction > MAX_RECORDS_PER_FRAME as usize {
            return Err(crate::Error::Validation(format!(
                "max_records_per_transaction {} exceeds hard frame record ceiling {}",
                self.max_records_per_transaction, MAX_RECORDS_PER_FRAME
            )));
        }
        if self.max_records_per_transaction > u32::MAX as usize {
            return Err(crate::Error::Validation(
                "max_records_per_transaction exceeds u32::MAX".into(),
            ));
        }
        if self.max_replace_entries == 0 {
            return Err(crate::Error::Validation(
                "max_replace_entries must be greater than 0".into(),
            ));
        }
        if self.max_replace_entries > MAX_REPLACE_ENTRIES_PER_RECORD as usize {
            return Err(crate::Error::Validation(format!(
                "max_replace_entries {} exceeds hard format ceiling {}",
                self.max_replace_entries, MAX_REPLACE_ENTRIES_PER_RECORD
            )));
        }
        Ok(())
    }
}
