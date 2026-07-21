use crate::config::EffectMode;
use crate::id::Id;

/// Specification for a durable job.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct JobSpec {
    pub job_id: Id,
    pub queue: String,
    pub partition_key: String,
    pub payload: Vec<u8>,
    pub not_before_ms: i64,
    pub max_attempts: u32,
    pub effect_mode: EffectMode,
    pub idempotency_key: Option<String>,
}

impl JobSpec {
    fn base(
        job_id: Id,
        queue: impl Into<String>,
        partition_key: impl Into<String>,
        payload: Vec<u8>,
        effect_mode: EffectMode,
        idempotency_key: Option<String>,
    ) -> Self {
        Self {
            job_id,
            queue: queue.into(),
            partition_key: partition_key.into(),
            payload,
            not_before_ms: 0,
            max_attempts: 3,
            effect_mode,
            idempotency_key,
        }
    }

    /// A job whose external effect requires reconciliation if a lease expires
    /// without acknowledgement (the safe default).
    pub fn reconcilable(
        job_id: Id,
        queue: impl Into<String>,
        partition_key: impl Into<String>,
        payload: Vec<u8>,
    ) -> Self {
        Self::base(
            job_id,
            queue,
            partition_key,
            payload,
            EffectMode::RequiresReconciliation,
            None,
        )
    }

    /// A job whose external effect is made idempotent by the given key; expired
    /// leases may be retried safely.
    pub fn idempotent(
        job_id: Id,
        queue: impl Into<String>,
        partition_key: impl Into<String>,
        payload: Vec<u8>,
        key: impl Into<String>,
    ) -> Self {
        Self::base(
            job_id,
            queue,
            partition_key,
            payload,
            EffectMode::ExplicitlyIdempotent,
            Some(key.into()),
        )
    }

    /// A job whose effect is idempotent by construction (no external key needed).
    pub fn intrinsically_idempotent(
        job_id: Id,
        queue: impl Into<String>,
        partition_key: impl Into<String>,
        payload: Vec<u8>,
    ) -> Self {
        Self::base(
            job_id,
            queue,
            partition_key,
            payload,
            EffectMode::ExplicitlyIdempotent,
            None,
        )
    }

    /// Set the earliest time at which this job should be claimed.
    pub fn with_not_before_ms(mut self, not_before_ms: i64) -> Self {
        self.not_before_ms = not_before_ms;
        self
    }

    /// Set the maximum number of attempts before marking the job dead.
    pub fn with_max_attempts(mut self, max_attempts: u32) -> Self {
        self.max_attempts = max_attempts;
        self
    }
}

/// Durable state of a job.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum JobState {
    /// The job is waiting to be claimed.
    Pending,
    /// The job has been claimed and has an active lease.
    Leased,
    /// The job failed and is waiting for its retry time.
    RetryWait,
    /// The job's lease expired without acknowledgement for a reconcilable effect.
    Uncertain,
    /// The job completed successfully.
    Succeeded,
    /// The job exhausted attempts or was explicitly marked dead.
    Dead,
    /// The job was cancelled.
    Cancelled,
}

impl JobState {
    /// Stable integer encoding used in the `jobs` table.
    #[allow(dead_code)] // used once job writes are implemented
    pub(crate) fn encode(self) -> i64 {
        match self {
            JobState::Pending => 0,
            JobState::Leased => 1,
            JobState::RetryWait => 2,
            JobState::Uncertain => 3,
            JobState::Succeeded => 4,
            JobState::Dead => 5,
            JobState::Cancelled => 6,
        }
    }

    /// Decode the stable integer encoding used in the `jobs` table.
    #[allow(dead_code)] // used once job reads are implemented
    pub(crate) fn decode(value: i64) -> Option<Self> {
        match value {
            0 => Some(JobState::Pending),
            1 => Some(JobState::Leased),
            2 => Some(JobState::RetryWait),
            3 => Some(JobState::Uncertain),
            4 => Some(JobState::Succeeded),
            5 => Some(JobState::Dead),
            6 => Some(JobState::Cancelled),
            _ => None,
        }
    }

    /// Whether the job can never change state again.
    pub fn is_terminal(self) -> bool {
        matches!(
            self,
            JobState::Succeeded | JobState::Dead | JobState::Cancelled
        )
    }

    /// The exact allowed state machine. `Leased -> Leased` is permitted only for
    /// lease extension.
    pub fn can_transition_to(self, to: JobState) -> bool {
        use JobState::*;
        matches!(
            (self, to),
            (Pending, Leased)
                | (RetryWait, Leased)
                | (Leased, Succeeded)
                | (Leased, RetryWait)
                | (Leased, Dead)
                | (Leased, Cancelled)
                | (Leased, Uncertain)
                | (Leased, Leased)
                | (Uncertain, Pending)
                | (Uncertain, Succeeded)
                | (Uncertain, Dead)
        )
    }
}

/// A snapshot of a job record as of a point in time.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct JobInfo {
    pub job_id: Id,
    pub spec: JobSpec,
    pub state: JobState,
    pub attempt: u32,
    pub lease_expires_at_ms: Option<i64>,
    pub worker_id: Option<String>,
    pub retry_after_ms: Option<i64>,
    pub terminal_at_ms: Option<i64>,
    pub result_digest: Option<Vec<u8>>,
    pub error_summary: Option<String>,
}

/// A job claimed by a worker, carrying the lease token needed to ack, fail, or
/// extend the lease.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ClaimedJob {
    pub job_id: Id,
    pub queue: String,
    pub partition_key: String,
    pub payload: Vec<u8>,
    pub worker_id: String,
    pub lease_token: Id,
    pub attempt: u32,
    pub lease_expires_at_ms: i64,
    pub idempotency_key: Option<String>,
}

/// A request to claim ready jobs from one queue.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ClaimRequest {
    pub queue: String,
    pub worker_id: String,
    pub now_ms: i64,
    pub lease_ms: i64,
    pub limit: usize,
}

/// Receipt for a claim transaction that performed only expired-lease maintenance.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct MaintenanceReceipt {
    /// The transaction that recorded the maintenance transitions.
    pub transaction_id: Id,
}

/// Successful outcome of a `claim_jobs` call.
///
/// Uncertainty is not an outcome: an indeterminate claim is returned as
/// [`ClaimError::Indeterminate`](crate::ClaimError::Indeterminate).
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ClaimOutcome {
    /// No ready jobs and no maintenance needed; no durable transaction was created.
    Noop,
    /// Only expired-lease maintenance was committed; no leases were granted.
    MaintenanceCommitted(MaintenanceReceipt),
    /// One or more leases were durably committed.
    Committed(CommittedClaims),
}

/// A durably committed set of claimed jobs.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CommittedClaims {
    pub(crate) transaction_id: Id,
    pub(crate) jobs: Vec<ClaimedJob>,
}

impl CommittedClaims {
    /// The transaction that committed these claims.
    pub fn transaction_id(&self) -> Id {
        self.transaction_id
    }

    /// The claimed jobs.
    pub fn jobs(&self) -> &[ClaimedJob] {
        &self.jobs
    }

    /// Consume the receipt, returning the claimed jobs.
    pub fn into_jobs(self) -> Vec<ClaimedJob> {
        self.jobs
    }

    pub fn len(&self) -> usize {
        self.jobs.len()
    }

    pub fn is_empty(&self) -> bool {
        self.jobs.is_empty()
    }
}

impl IntoIterator for CommittedClaims {
    type Item = ClaimedJob;
    type IntoIter = std::vec::IntoIter<ClaimedJob>;

    fn into_iter(self) -> Self::IntoIter {
        self.jobs.into_iter()
    }
}

impl<'a> IntoIterator for &'a CommittedClaims {
    type Item = &'a ClaimedJob;
    type IntoIter = std::slice::Iter<'a, ClaimedJob>;

    fn into_iter(self) -> Self::IntoIter {
        self.jobs.iter()
    }
}

/// A claim whose durable outcome is unknown.
///
/// Deliberately opaque: it exposes only the transaction ID and the proposed job IDs
/// for verification. It carries no payloads and no lease tokens, so it cannot be
/// mistaken for a granted claim. Recover with
/// [`ControlPlaneStore::recover_claim`](crate::ControlPlaneStore::recover_claim).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct IndeterminateClaim {
    pub(crate) transaction_id: Id,
    pub(crate) proposed_jobs: Vec<Id>,
    pub(crate) storage_error: String,
}

impl IndeterminateClaim {
    /// The transaction whose durability is unknown.
    pub fn transaction_id(&self) -> Id {
        self.transaction_id
    }

    /// The job IDs that were proposed for leasing, for verification only.
    pub fn proposed_jobs_for_verification(&self) -> &[Id] {
        &self.proposed_jobs
    }

    /// The underlying storage failure reported by the COMMIT step.
    pub fn storage_error(&self) -> &str {
        &self.storage_error
    }
}

/// Result of recovering an indeterminate claim.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ClaimRecovery {
    /// The claim committed; the original lease tokens are reconstructed from
    /// `claim_receipts`.
    Committed(CommittedClaims),
    /// The claim did not commit; the jobs were never leased by this transaction.
    Absent,
    /// The store cannot yet determine the outcome; retry recovery later.
    StillIndeterminate,
}

/// Resolution for an uncertain job.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Resolution {
    /// Retry the job from a clean state.
    Retry,
    /// The external effect succeeded; mark the job complete.
    MarkSucceeded,
    /// The external effect failed or is unrecoverable; mark the job dead.
    MarkDead,
}

/// Acknowledge a completed job under its current lease token.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct JobAck {
    pub job_id: Id,
    pub lease_token: Id,
    pub result_digest: Option<Vec<u8>>,
}

/// Record a job failure under its current lease token.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct JobFailure {
    pub job_id: Id,
    pub lease_token: Id,
    pub error_summary: String,
    /// `None` uses the default retry delay resolved at commit time.
    pub retry_after_ms: Option<i64>,
}

/// Cancel a job. A leased job requires its current lease token.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct JobCancellation {
    pub job_id: Id,
    pub lease_token: Option<Id>,
}

/// Resolve an uncertain job outcome.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct JobResolution {
    pub job_id: Id,
    pub resolution: Resolution,
}

/// Request to extend an active lease.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct LeaseExtension {
    pub job_id: Id,
    pub lease_token: Id,
    pub new_expiry_ms: i64,
}

/// Receipt for a durably committed lease extension.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct LeaseExtensionReceipt {
    pub job_id: Id,
    pub attempt: u32,
    pub lease_expires_at_ms: i64,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn state_encoding_is_stable() {
        let states = [
            (JobState::Pending, 0),
            (JobState::Leased, 1),
            (JobState::RetryWait, 2),
            (JobState::Uncertain, 3),
            (JobState::Succeeded, 4),
            (JobState::Dead, 5),
            (JobState::Cancelled, 6),
        ];
        for (state, code) in states {
            assert_eq!(state.encode(), code);
            assert_eq!(JobState::decode(code), Some(state));
        }
        assert_eq!(JobState::decode(7), None);
    }

    #[test]
    fn transitions_match_spec() {
        use JobState::*;
        assert!(Pending.can_transition_to(Leased));
        assert!(RetryWait.can_transition_to(Leased));
        assert!(Leased.can_transition_to(Leased));
        assert!(Uncertain.can_transition_to(Pending));
        assert!(!Pending.can_transition_to(Succeeded));
        assert!(!Succeeded.can_transition_to(Leased));
        assert!(!Uncertain.can_transition_to(RetryWait));
        assert!(!Dead.can_transition_to(Pending));
    }
}
