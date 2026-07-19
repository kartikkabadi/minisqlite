use std::collections::VecDeque;

use crate::event::{Event, StreamVersion};
use crate::id::Id;
use crate::jobs::{JobSpec, Resolution};
use crate::projection::ProjectionEntry;

/// A builder for one atomic commit.
///
/// A `CommitBatch` contains all the records that must become durable together:
/// events, projection mutations, and job state transitions. The store validates the
/// entire batch before appending a single frame.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CommitBatch {
    pub(crate) transaction_id: Id,
    pub(crate) now_ms: i64,
    pub(crate) expected_stream_versions: Vec<(String, u64)>,
    pub(crate) ops: VecDeque<Op>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum Op {
    AppendEvent(Event),
    ProjectionPut {
        projection: String,
        version: u64,
        key: Vec<u8>,
        value: Vec<u8>,
    },
    ProjectionDelete {
        projection: String,
        version: u64,
        key: Vec<u8>,
    },
    ProjectionClear {
        projection: String,
        new_version: u64,
    },
    ProjectionReplace {
        projection: String,
        new_version: u64,
        entries: Vec<ProjectionEntry>,
    },
    EnqueueJob(JobSpec),
    AckJob {
        job_id: Id,
        lease_token: Id,
        result_digest: Option<Vec<u8>>,
    },
    FailJob {
        job_id: Id,
        lease_token: Id,
        error_summary: String,
        retry_after_ms: Option<i64>,
    },
    CancelJob {
        job_id: Id,
        lease_token: Option<Id>,
    },
    ResolveJob {
        job_id: Id,
        resolution: Resolution,
    },
    LeaseJob {
        job_id: Id,
        lease_token: Id,
        worker_id: String,
        attempt: u32,
        lease_expires_at_ms: i64,
    },
}

/// Receipt returned after a successful commit.
#[derive(Debug, Clone, PartialEq, Eq)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub struct CommitReceipt {
    pub transaction_id: Id,
    pub transaction_sequence: u64,
    pub first_event_sequence: Option<u64>,
    pub last_event_sequence: Option<u64>,
    pub stream_versions: Vec<StreamVersion>,
    pub job_ids: Vec<Id>,
    pub frame_offset: u64,
}

impl CommitBatch {
    /// Start a new commit batch.
    pub fn new(transaction_id: Id, now_ms: i64) -> Self {
        Self {
            transaction_id,
            now_ms,
            expected_stream_versions: Vec::new(),
            ops: VecDeque::new(),
        }
    }

    /// Require that `stream_id` is currently at `version` before this commit succeeds.
    pub fn expect_stream_version(mut self, stream_id: impl Into<String>, version: u64) -> Self {
        self.expected_stream_versions
            .push((stream_id.into(), version));
        self
    }

    /// Append an event.
    pub fn append_event(mut self, event: Event) -> Self {
        self.ops.push_back(Op::AppendEvent(event));
        self
    }

    /// Put a key into a projection.
    pub fn projection_put(
        mut self,
        projection: impl Into<String>,
        version: u64,
        key: Vec<u8>,
        value: Vec<u8>,
    ) -> Self {
        self.ops.push_back(Op::ProjectionPut {
            projection: projection.into(),
            version,
            key,
            value,
        });
        self
    }

    /// Delete a key from a projection.
    pub fn projection_delete(
        mut self,
        projection: impl Into<String>,
        version: u64,
        key: Vec<u8>,
    ) -> Self {
        self.ops.push_back(Op::ProjectionDelete {
            projection: projection.into(),
            version,
            key,
        });
        self
    }

    /// Clear a projection and set its version.
    pub fn projection_clear(mut self, projection: impl Into<String>, new_version: u64) -> Self {
        self.ops.push_back(Op::ProjectionClear {
            projection: projection.into(),
            new_version,
        });
        self
    }

    /// Atomically replace the entire contents of a projection.
    pub fn projection_replace(
        mut self,
        projection: impl Into<String>,
        new_version: u64,
        entries: impl IntoIterator<Item = ProjectionEntry>,
    ) -> Self {
        self.ops.push_back(Op::ProjectionReplace {
            projection: projection.into(),
            new_version,
            entries: entries.into_iter().collect(),
        });
        self
    }

    /// Enqueue a durable job.
    pub fn enqueue_job(mut self, job: JobSpec) -> Self {
        self.ops.push_back(Op::EnqueueJob(job));
        self
    }

    /// Acknowledge a completed job. Requires the current lease token.
    pub fn acknowledge_job(
        mut self,
        job_id: Id,
        lease_token: Id,
        result_digest: Option<Vec<u8>>,
    ) -> Self {
        self.ops.push_back(Op::AckJob {
            job_id,
            lease_token,
            result_digest,
        });
        self
    }

    /// Record a job failure. The store will decide retry or dead based on `max_attempts`.
    pub fn fail_job(
        mut self,
        job_id: Id,
        lease_token: Id,
        error_summary: impl Into<String>,
        retry_after_ms: Option<i64>,
    ) -> Self {
        self.ops.push_back(Op::FailJob {
            job_id,
            lease_token,
            error_summary: error_summary.into(),
            retry_after_ms,
        });
        self
    }

    /// Cancel a job.
    pub fn cancel_job(mut self, job_id: Id, lease_token: Option<Id>) -> Self {
        self.ops.push_back(Op::CancelJob {
            job_id,
            lease_token,
        });
        self
    }

    /// Resolve an uncertain job outcome.
    pub fn resolve_uncertain_job(mut self, job_id: Id, resolution: Resolution) -> Self {
        self.ops.push_back(Op::ResolveJob { job_id, resolution });
        self
    }

    /// Lease a job to a worker. Used internally by `Store::claim_jobs`.
    pub(crate) fn lease_job(
        mut self,
        job_id: Id,
        lease_token: Id,
        worker_id: impl Into<String>,
        attempt: u32,
        lease_expires_at_ms: i64,
    ) -> Self {
        self.ops.push_back(Op::LeaseJob {
            job_id,
            lease_token,
            worker_id: worker_id.into(),
            attempt,
            lease_expires_at_ms,
        });
        self
    }
}
