use crate::event::Event;
use crate::id::Id;
use crate::jobs::{
    JobAck, JobCancellation, JobFailure, JobResolution, JobSpec, LeaseExtension, Resolution,
};
use crate::projection::{ProjectionMutation, ProjectionPatch};

/// One logical operation within a [`CommitBatch`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Operation {
    /// Append a domain event to its stream.
    AppendEvent(Event),
    /// Apply a versioned patch to a projection.
    ProjectionPatch(ProjectionPatch),
    /// Enqueue a durable job.
    EnqueueJob(JobSpec),
    /// Acknowledge a completed job.
    AckJob(JobAck),
    /// Record a job failure.
    FailJob(JobFailure),
    /// Cancel a job.
    CancelJob(JobCancellation),
    /// Resolve an uncertain job outcome.
    ResolveJob(JobResolution),
    /// Extend an active lease.
    ExtendLease(LeaseExtension),
}

/// A precondition that `stream_id` is at exactly `version` before the commit applies.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ExpectedStreamVersion {
    /// The stream whose version is checked.
    pub stream_id: String,
    /// The exact version the stream must be at.
    pub version: u64,
}

/// A builder for one atomic commit.
///
/// A `CommitBatch` contains everything that must become durable together: events,
/// projection patches, and job state transitions. The store validates the entire
/// batch before writing anything.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CommitBatch {
    pub(crate) transaction_id: Id,
    pub(crate) committed_at_ms: i64,
    pub(crate) correlation_id: Option<Id>,
    pub(crate) metadata: Vec<u8>,
    pub(crate) expected_stream_versions: Vec<ExpectedStreamVersion>,
    pub(crate) operations: Vec<Operation>,
}

impl CommitBatch {
    /// Start a new commit batch. `committed_at_ms` is the caller-supplied wall-clock
    /// commit time recorded in the transaction row.
    pub fn new(transaction_id: Id, committed_at_ms: i64) -> Self {
        Self {
            transaction_id,
            committed_at_ms,
            correlation_id: None,
            metadata: Vec::new(),
            expected_stream_versions: Vec::new(),
            operations: Vec::new(),
        }
    }

    /// Attach an optional correlation id to the transaction.
    pub fn with_correlation_id(mut self, correlation_id: Id) -> Self {
        self.correlation_id = Some(correlation_id);
        self
    }

    /// Attach optional opaque metadata to the transaction.
    pub fn with_metadata(mut self, metadata: Vec<u8>) -> Self {
        self.metadata = metadata;
        self
    }

    /// Require that `stream_id` is currently at `version` before this commit succeeds.
    pub fn expect_stream_version(mut self, stream_id: impl Into<String>, version: u64) -> Self {
        self.expected_stream_versions.push(ExpectedStreamVersion {
            stream_id: stream_id.into(),
            version,
        });
        self
    }

    /// Append an event.
    pub fn append_event(mut self, event: Event) -> Self {
        self.operations.push(Operation::AppendEvent(event));
        self
    }

    /// Apply a projection patch.
    pub fn apply_projection_patch(mut self, patch: ProjectionPatch) -> Self {
        self.operations.push(Operation::ProjectionPatch(patch));
        self
    }

    /// Enqueue a durable job.
    pub fn enqueue_job(mut self, job: JobSpec) -> Self {
        self.operations.push(Operation::EnqueueJob(job));
        self
    }

    /// Acknowledge a completed job. Requires the current lease token.
    pub fn acknowledge_job(
        mut self,
        job_id: Id,
        lease_token: Id,
        result_digest: Option<Vec<u8>>,
    ) -> Self {
        self.operations.push(Operation::AckJob(JobAck {
            job_id,
            lease_token,
            result_digest,
        }));
        self
    }

    /// Record a job failure. The store decides retry or dead based on `max_attempts`.
    /// `retry_after_ms` is `None` to use the default, resolved at commit time.
    pub fn fail_job(
        mut self,
        job_id: Id,
        lease_token: Id,
        error_summary: impl Into<String>,
        retry_after_ms: Option<i64>,
    ) -> Self {
        self.operations.push(Operation::FailJob(JobFailure {
            job_id,
            lease_token,
            error_summary: error_summary.into(),
            retry_after_ms,
        }));
        self
    }

    /// Cancel a job.
    pub fn cancel_job(mut self, job_id: Id, lease_token: Option<Id>) -> Self {
        self.operations.push(Operation::CancelJob(JobCancellation {
            job_id,
            lease_token,
        }));
        self
    }

    /// Resolve an uncertain job outcome.
    pub fn resolve_uncertain_job(mut self, job_id: Id, resolution: Resolution) -> Self {
        self.operations
            .push(Operation::ResolveJob(JobResolution { job_id, resolution }));
        self
    }

    /// Extend an active lease atomically with the rest of the batch. Requires the
    /// current lease token; `committed_at_ms` is used as "now" for expiry checks.
    pub fn extend_lease(mut self, job_id: Id, lease_token: Id, new_expiry_ms: i64) -> Self {
        self.operations.push(Operation::ExtendLease(LeaseExtension {
            job_id,
            lease_token,
            new_expiry_ms,
        }));
        self
    }

    /// The transaction id of this batch.
    pub fn transaction_id(&self) -> Id {
        self.transaction_id
    }

    /// Compute the canonical request digest for idempotent resubmission checks.
    ///
    /// The digest is SHA-256 over a canonical, length-prefixed byte serialization of
    /// (committed_at_ms, correlation_id, metadata, expected stream versions in the order
    /// given, ordered operations), so collisions are cryptographically infeasible even
    /// for adversarial content. The serialization uses fixed-width big-endian integers
    /// and explicit tags, so it is stable across process runs and platforms.
    pub(crate) fn request_digest(&self) -> [u8; 32] {
        let mut d = Digest::new();
        d.write_i64(self.committed_at_ms);
        d.write_opt_id(self.correlation_id);
        d.write_bytes(&self.metadata);
        d.write_u64(self.expected_stream_versions.len() as u64);
        for expected in &self.expected_stream_versions {
            d.write_str(&expected.stream_id);
            d.write_u64(expected.version);
        }
        d.write_u64(self.operations.len() as u64);
        for op in &self.operations {
            digest_operation(&mut d, op);
        }
        d.finish()
    }
}

fn digest_operation(d: &mut Digest, op: &Operation) {
    match op {
        Operation::AppendEvent(e) => {
            d.write_u8(1);
            d.write_id(e.event_id);
            d.write_str(&e.stream_id);
            d.write_str(&e.event_type);
            d.write_u64(u64::from(e.schema_version));
            d.write_i64(e.occurred_at_ms);
            d.write_opt_id(e.causation_id);
            d.write_opt_id(e.correlation_id);
            d.write_bytes(&e.payload);
            d.write_bytes(&e.metadata);
        }
        Operation::ProjectionPatch(p) => {
            d.write_u8(2);
            d.write_str(&p.projection);
            d.write_u64(p.expected_version);
            d.write_u64(p.new_version);
            d.write_u64(p.mutations.len() as u64);
            for m in &p.mutations {
                match m {
                    ProjectionMutation::Put { key, value } => {
                        d.write_u8(1);
                        d.write_bytes(key);
                        d.write_bytes(value);
                    }
                    ProjectionMutation::Delete { key } => {
                        d.write_u8(2);
                        d.write_bytes(key);
                    }
                    ProjectionMutation::Clear => d.write_u8(3),
                    ProjectionMutation::Replace { entries } => {
                        d.write_u8(4);
                        d.write_u64(entries.len() as u64);
                        for entry in entries {
                            d.write_bytes(&entry.key);
                            d.write_bytes(&entry.value);
                        }
                    }
                }
            }
        }
        Operation::EnqueueJob(j) => {
            d.write_u8(3);
            d.write_id(j.job_id);
            d.write_str(&j.queue);
            d.write_str(&j.partition_key);
            d.write_bytes(&j.payload);
            d.write_i64(j.not_before_ms);
            d.write_u64(u64::from(j.max_attempts));
            d.write_u8(j.effect_mode.encode() as u8);
            match &j.idempotency_key {
                Some(key) => {
                    d.write_u8(1);
                    d.write_str(key);
                }
                None => d.write_u8(0),
            }
        }
        Operation::AckJob(a) => {
            d.write_u8(4);
            d.write_id(a.job_id);
            d.write_id(a.lease_token);
            match &a.result_digest {
                Some(digest) => {
                    d.write_u8(1);
                    d.write_bytes(digest);
                }
                None => d.write_u8(0),
            }
        }
        Operation::FailJob(f) => {
            d.write_u8(5);
            d.write_id(f.job_id);
            d.write_id(f.lease_token);
            d.write_str(&f.error_summary);
            match f.retry_after_ms {
                Some(ms) => {
                    d.write_u8(1);
                    d.write_i64(ms);
                }
                None => d.write_u8(0),
            }
        }
        Operation::CancelJob(c) => {
            d.write_u8(6);
            d.write_id(c.job_id);
            d.write_opt_id(c.lease_token);
        }
        Operation::ResolveJob(r) => {
            d.write_u8(7);
            d.write_id(r.job_id);
            d.write_u8(match r.resolution {
                Resolution::Retry => 1,
                Resolution::MarkSucceeded => 2,
                Resolution::MarkDead => 3,
            });
        }
        Operation::ExtendLease(e) => {
            d.write_u8(8);
            d.write_id(e.job_id);
            d.write_id(e.lease_token);
            d.write_i64(e.new_expiry_ms);
        }
    }
}

/// SHA-256 incremental hasher over canonical length-prefixed input.
struct Digest {
    state: crate::sha256::Sha256,
}

impl Digest {
    fn new() -> Self {
        Self {
            state: crate::sha256::Sha256::new(),
        }
    }

    fn write_raw(&mut self, bytes: &[u8]) {
        self.state.update(bytes);
    }

    fn write_u8(&mut self, v: u8) {
        self.write_raw(&[v]);
    }

    fn write_u64(&mut self, v: u64) {
        self.write_raw(&v.to_be_bytes());
    }

    fn write_i64(&mut self, v: i64) {
        self.write_raw(&v.to_be_bytes());
    }

    fn write_bytes(&mut self, bytes: &[u8]) {
        self.write_u64(bytes.len() as u64);
        self.write_raw(bytes);
    }

    fn write_str(&mut self, s: &str) {
        self.write_bytes(s.as_bytes());
    }

    fn write_id(&mut self, id: Id) {
        self.write_raw(id.as_bytes());
    }

    fn write_opt_id(&mut self, id: Option<Id>) {
        match id {
            Some(id) => {
                self.write_u8(1);
                self.write_id(id);
            }
            None => self.write_u8(0),
        }
    }

    fn finish(self) -> [u8; 32] {
        self.state.finish()
    }
}

/// Receipt returned after a successful commit. Carries no physical storage details.
///
/// Receipts are only ever constructed by the store, so holding one is proof of a
/// durable commit.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct CommitReceipt {
    pub(crate) transaction_id: Id,
    pub(crate) transaction_sequence: u64,
    pub(crate) committed_at_ms: i64,
}

impl CommitReceipt {
    /// The committed transaction's ID.
    pub fn transaction_id(&self) -> Id {
        self.transaction_id
    }

    /// The store-wide monotonic sequence assigned to the transaction.
    pub fn transaction_sequence(&self) -> u64 {
        self.transaction_sequence
    }

    /// The caller-supplied commit timestamp recorded with the transaction.
    pub fn committed_at_ms(&self) -> i64 {
        self.committed_at_ms
    }
}

/// Result of recovering an indeterminate commit.
///
/// SQLite commits are atomic, so recovery against a healthy store always resolves
/// to a definite outcome; there is no "still indeterminate" state.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum TransactionRecovery {
    /// The transaction committed durably.
    Committed(CommitReceipt),
    /// The transaction did not commit; it is safe to resubmit.
    Absent,
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sample_batch() -> CommitBatch {
        CommitBatch::new(Id::from(7u128), 1_000)
            .with_correlation_id(Id::from(9u128))
            .with_metadata(b"meta".to_vec())
            .expect_stream_version("s1", 3)
            .append_event(Event::with_json_payload(
                Id::from(11u128),
                "s1",
                "created",
                999,
                b"{}",
            ))
            .enqueue_job(JobSpec::reconcilable(
                Id::from(12u128),
                "q",
                "p",
                b"job".to_vec(),
            ))
    }

    #[test]
    fn digest_is_deterministic() {
        assert_eq!(
            sample_batch().request_digest(),
            sample_batch().request_digest()
        );
    }

    #[test]
    fn digest_changes_with_content() {
        let base = sample_batch().request_digest();
        let different_meta = {
            let mut b = sample_batch();
            b.metadata = b"other".to_vec();
            b.request_digest()
        };
        let different_op = sample_batch()
            .cancel_job(Id::from(13u128), None)
            .request_digest();
        assert_ne!(base, different_meta);
        assert_ne!(base, different_op);
    }

    #[test]
    fn digest_distinguishes_field_boundaries() {
        // "ab" + "c" must not collide with "a" + "bc" thanks to length prefixes.
        let a = CommitBatch::new(Id::from(1u128), 0)
            .expect_stream_version("ab", 1)
            .expect_stream_version("c", 1)
            .request_digest();
        let b = CommitBatch::new(Id::from(1u128), 0)
            .expect_stream_version("a", 1)
            .expect_stream_version("bc", 1)
            .request_digest();
        assert_ne!(a, b);
    }
}
