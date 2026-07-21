use crate::error::ValidationError;

/// Controls how strictly a commit synchronizes to durable storage.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum Durability {
    /// `PRAGMA synchronous=FULL`. Commits survive power loss. This is the default.
    #[default]
    Strict,
    /// `PRAGMA synchronous=NORMAL`. Commits survive process crashes; a power loss
    /// may lose the most recent commits but never corrupts the database (WAL mode).
    Relaxed,
}

impl Durability {
    /// The SQLite `synchronous` pragma value for this durability level.
    pub(crate) fn synchronous_pragma(self) -> &'static str {
        match self {
            Durability::Strict => "FULL",
            Durability::Relaxed => "NORMAL",
        }
    }
}

/// Whether a job's external effect can safely be retried without reconciliation.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum EffectMode {
    /// The effect is non-idempotent. An expired lease moves the job to `Uncertain`
    /// and must be explicitly resolved. This is the safe default.
    #[default]
    RequiresReconciliation,
    /// The effect is idempotent. An expired lease may be claimed and retried.
    /// The caller should still supply an `idempotency_key` for external effects.
    ExplicitlyIdempotent,
}

impl EffectMode {
    /// Stable integer encoding used in the `jobs` table.
    pub(crate) fn encode(self) -> i64 {
        match self {
            EffectMode::RequiresReconciliation => 0,
            EffectMode::ExplicitlyIdempotent => 1,
        }
    }

    /// Decode the stable integer encoding used in the `jobs` table.
    #[allow(dead_code)] // used once job reads are implemented
    pub(crate) fn decode(value: i64) -> Option<Self> {
        match value {
            0 => Some(EffectMode::RequiresReconciliation),
            1 => Some(EffectMode::ExplicitlyIdempotent),
            _ => None,
        }
    }
}

/// Size and shape limits for a store.
///
/// These are intentionally conservative. The workload is bounded control-plane metadata,
/// not arbitrary blobs. Limits are enforced before any bytes are written.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Limits {
    /// Maximum event payload in bytes.
    pub max_event_payload: usize,
    /// Maximum event or transaction metadata in bytes.
    pub max_metadata: usize,
    /// Maximum projection key in bytes.
    pub max_projection_key: usize,
    /// Maximum projection value in bytes.
    pub max_projection_value: usize,
    /// Maximum job payload in bytes.
    pub max_job_payload: usize,
    /// Maximum operations in a single commit batch.
    pub max_operations_per_commit: usize,
    /// Maximum length of any UTF-8 string field (stream ID, event type, queue, partition, etc.).
    pub max_string_len: usize,
    /// Maximum error summary or diagnostic string length.
    pub max_summary_len: usize,
    /// Maximum number of mutations in a projection replacement.
    pub max_replace_entries: usize,
}

impl Default for Limits {
    fn default() -> Self {
        Self::new()
    }
}

impl Limits {
    /// Returns the default limits.
    pub const fn new() -> Self {
        Self {
            max_event_payload: 1 << 20,    // 1 MiB
            max_metadata: 64 << 10,        // 64 KiB
            max_projection_key: 1 << 20,   // 1 MiB
            max_projection_value: 4 << 20, // 4 MiB
            max_job_payload: 1 << 20,      // 1 MiB
            max_operations_per_commit: 1024,
            max_string_len: 4096,
            max_summary_len: 4096,
            max_replace_entries: 10_000,
        }
    }

    fn too_large(kind: &str, size: usize, limit: usize) -> ValidationError {
        ValidationError(format!("{kind} exceeds limit: {size} > {limit}"))
    }

    pub(crate) fn validate_event(
        &self,
        payload_len: usize,
        metadata_len: usize,
    ) -> Result<(), ValidationError> {
        if payload_len > self.max_event_payload {
            return Err(Self::too_large(
                "event payload",
                payload_len,
                self.max_event_payload,
            ));
        }
        self.validate_metadata(metadata_len)
    }

    pub(crate) fn validate_metadata(&self, len: usize) -> Result<(), ValidationError> {
        if len > self.max_metadata {
            return Err(Self::too_large("metadata", len, self.max_metadata));
        }
        Ok(())
    }

    pub(crate) fn validate_projection_key(&self, len: usize) -> Result<(), ValidationError> {
        if len > self.max_projection_key {
            return Err(Self::too_large(
                "projection key",
                len,
                self.max_projection_key,
            ));
        }
        Ok(())
    }

    pub(crate) fn validate_projection_value(&self, len: usize) -> Result<(), ValidationError> {
        if len > self.max_projection_value {
            return Err(Self::too_large(
                "projection value",
                len,
                self.max_projection_value,
            ));
        }
        Ok(())
    }

    pub(crate) fn validate_job_payload(&self, len: usize) -> Result<(), ValidationError> {
        if len > self.max_job_payload {
            return Err(Self::too_large("job payload", len, self.max_job_payload));
        }
        Ok(())
    }

    pub(crate) fn validate_string(&self, field: &str, s: &str) -> Result<(), ValidationError> {
        if s.len() > self.max_string_len {
            return Err(Self::too_large(field, s.len(), self.max_string_len));
        }
        Ok(())
    }

    pub(crate) fn validate_summary(&self, s: &str) -> Result<(), ValidationError> {
        if s.len() > self.max_summary_len {
            return Err(Self::too_large(
                "error summary",
                s.len(),
                self.max_summary_len,
            ));
        }
        Ok(())
    }

    /// Validate that the configured limits are internally consistent.
    pub fn validate(&self) -> Result<(), ValidationError> {
        if self.max_operations_per_commit == 0 {
            return Err(ValidationError(
                "max_operations_per_commit must be greater than 0".into(),
            ));
        }
        if self.max_replace_entries == 0 {
            return Err(ValidationError(
                "max_replace_entries must be greater than 0".into(),
            ));
        }
        Ok(())
    }
}
