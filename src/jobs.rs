use crate::config::EffectMode;
use crate::id::Id;
use crate::Error;

/// Specification for a durable job.
#[derive(Debug, Clone, PartialEq, Eq)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub struct JobSpec {
    pub job_id: Id,
    pub queue: String,
    pub partition: String,
    pub payload: Vec<u8>,
    pub not_before_ms: i64,
    pub max_attempts: u32,
    pub effect_mode: EffectMode,
    pub idempotency_key: Option<String>,
}

impl JobSpec {
    /// Convenience constructor for tests and examples.
    pub fn new(
        job_id: Id,
        queue: impl Into<String>,
        partition: impl Into<String>,
        payload: Vec<u8>,
    ) -> Self {
        Self::with_job_id(job_id, queue, partition, payload)
    }

    fn with_job_id(
        job_id: Id,
        queue: impl Into<String>,
        partition: impl Into<String>,
        payload: Vec<u8>,
    ) -> Self {
        Self {
            job_id,
            queue: queue.into(),
            partition: partition.into(),
            payload,
            not_before_ms: 0,
            max_attempts: 3,
            effect_mode: EffectMode::default(),
            idempotency_key: None,
        }
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

    /// Set the effect mode of the job.
    pub fn with_effect_mode(mut self, effect_mode: EffectMode) -> Self {
        self.effect_mode = effect_mode;
        self
    }

    /// Set an idempotency key for the job's external effect.
    pub fn with_idempotency_key(mut self, key: impl Into<String>) -> Self {
        self.idempotency_key = Some(key.into());
        self
    }
}

/// Current state of a job as derived from the durable record.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub enum JobState {
    /// The job is waiting to be claimed.
    Pending,
    /// The job has been claimed and has an active lease.
    Leased,
    /// The job failed and is waiting for its retry time.
    RetryWait,
    /// The job completed successfully.
    Succeeded,
    /// The job exhausted attempts or was explicitly marked dead.
    Dead,
    /// The job was cancelled.
    Cancelled,
    /// The job's lease expired without acknowledgement for a non-idempotent effect.
    Uncertain,
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

/// A snapshot of a job record as of a point in time.
#[derive(Debug, Clone, PartialEq, Eq)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
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

/// A job claimed by a worker.
#[derive(Debug, Clone, PartialEq, Eq)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub struct ClaimedJob {
    pub job_id: Id,
    pub queue: String,
    pub partition: String,
    pub payload: Vec<u8>,
    pub worker_id: String,
    pub lease_token: Id,
    pub attempt: u32,
    pub lease_expires_at_ms: i64,
    pub idempotency_key: Option<String>,
}

/// Resolution for an uncertain job.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub enum Resolution {
    /// Retry the job from a clean state.
    Retry,
    /// The external effect succeeded; mark the job complete.
    MarkSucceeded,
    /// The external effect failed or is unrecoverable; mark the job dead.
    MarkDead,
}

/// Internal durable record for a job.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct JobStateRecord {
    pub spec: JobSpec,
    pub state: JobInternalState,
    pub lease_token: Option<Id>,
    pub worker_id: Option<String>,
    pub attempt: u32,
    pub lease_expires_at_ms: i64,
    pub retry_after_ms: i64,
    pub result_digest: Option<Vec<u8>>,
    pub error_summary: Option<String>,
    pub terminal_at_ms: Option<i64>,
}

impl JobStateRecord {
    pub fn new(spec: JobSpec) -> Self {
        let retry_after_ms = spec.not_before_ms;
        Self {
            spec,
            state: JobInternalState::Pending,
            lease_token: None,
            worker_id: None,
            attempt: 0,
            lease_expires_at_ms: 0,
            retry_after_ms,
            result_digest: None,
            error_summary: None,
            terminal_at_ms: None,
        }
    }

    /// Derive the public `JobState` at a point in time.
    pub fn state_at(&self, now_ms: i64) -> JobState {
        match self.state {
            JobInternalState::Pending => JobState::Pending,
            JobInternalState::Leased => {
                if now_ms < self.lease_expires_at_ms {
                    JobState::Leased
                } else if self.spec.effect_mode == EffectMode::UncertainOnLeaseExpiry {
                    JobState::Uncertain
                } else {
                    // Idempotent expired lease is ready, but still reported as Leased until claimed.
                    JobState::Leased
                }
            }
            JobInternalState::RetryWait => {
                if now_ms < self.retry_after_ms {
                    JobState::RetryWait
                } else {
                    JobState::Pending
                }
            }
            JobInternalState::Succeeded => JobState::Succeeded,
            JobInternalState::Dead => JobState::Dead,
            JobInternalState::Cancelled => JobState::Cancelled,
        }
    }

    pub fn is_terminal(&self) -> bool {
        matches!(
            self.state,
            JobInternalState::Succeeded | JobInternalState::Dead | JobInternalState::Cancelled
        )
    }

    pub fn is_uncertain_at(&self, now_ms: i64) -> bool {
        matches!(self.state, JobInternalState::Leased)
            && now_ms >= self.lease_expires_at_ms
            && self.spec.effect_mode == EffectMode::UncertainOnLeaseExpiry
    }

    pub fn is_ready_at(&self, now_ms: i64) -> bool {
        if self.is_terminal() || self.attempt >= self.spec.max_attempts {
            return false;
        }
        match self.state {
            JobInternalState::Pending => now_ms >= self.not_before(),
            JobInternalState::RetryWait => now_ms >= self.retry_after_ms,
            JobInternalState::Leased => {
                now_ms >= self.lease_expires_at_ms
                    && self.spec.effect_mode == EffectMode::Idempotent
            }
            _ => false,
        }
    }

    /// True when an idempotent job's lease has expired on its final attempt and it
    /// must be marked dead before later jobs in the same partition can be claimed.
    pub fn is_expired_at_attempt_limit(&self, now_ms: i64) -> bool {
        matches!(self.state, JobInternalState::Leased)
            && now_ms >= self.lease_expires_at_ms
            && self.spec.effect_mode == EffectMode::Idempotent
            && self.attempt >= self.spec.max_attempts
    }

    /// Lease the job to a worker. Fails if the job is not ready or the lease fields
    /// violate the immutable invariants (non-zero token, monotonic attempt, expiry after
    /// claim time, attempt within the configured ceiling).
    pub fn lease(
        &mut self,
        now_ms: i64,
        token: Id,
        worker_id: impl Into<String>,
        attempt: u32,
        lease_expires_at_ms: i64,
    ) -> Result<(), Error> {
        if token == Id::ZERO {
            return Err(Error::Validation(format!(
                "job {} lease token cannot be zero",
                self.spec.job_id
            )));
        }
        if self.is_terminal() || !self.is_ready_at(now_ms) {
            return Err(Error::Validation(format!(
                "job {} is not ready for lease",
                self.spec.job_id
            )));
        }
        let expected_attempt = self.attempt.checked_add(1).ok_or_else(|| {
            Error::Validation(format!("job {} attempt overflow", self.spec.job_id))
        })?;
        if attempt != expected_attempt {
            return Err(Error::Validation(format!(
                "job {} lease attempt {} does not match expected {}",
                self.spec.job_id, attempt, expected_attempt
            )));
        }
        if attempt > self.spec.max_attempts {
            return Err(Error::Validation(format!(
                "job {} lease attempt {} exceeds max_attempts {}",
                self.spec.job_id, attempt, self.spec.max_attempts
            )));
        }
        if lease_expires_at_ms <= now_ms {
            return Err(Error::Validation(format!(
                "job {} lease expires at or before claim time",
                self.spec.job_id
            )));
        }
        self.state = JobInternalState::Leased;
        self.lease_token = Some(token);
        self.worker_id = Some(worker_id.into());
        self.attempt = attempt;
        self.lease_expires_at_ms = lease_expires_at_ms;
        Ok(())
    }

    /// Acknowledge the job with the given lease token. Fails if the token is stale, expired,
    /// or the zero sentinel.
    pub fn acknowledge(
        &mut self,
        now_ms: i64,
        token: Id,
        result_digest: Option<Vec<u8>>,
    ) -> Result<(), Error> {
        if token == Id::ZERO {
            return Err(Error::InvalidLease {
                job_id: self.spec.job_id,
            });
        }
        if self.lease_token != Some(token) || now_ms >= self.lease_expires_at_ms {
            return Err(Error::InvalidLease {
                job_id: self.spec.job_id,
            });
        }
        self.state = JobInternalState::Succeeded;
        self.result_digest = result_digest;
        self.terminal_at_ms = Some(now_ms);
        self.lease_token = None;
        self.worker_id = None;
        self.lease_expires_at_ms = 0;
        self.retry_after_ms = 0;
        Ok(())
    }

    /// Mark the job as failed under the given lease token.
    ///
    /// A failure at the attempt ceiling may be reported even after the lease has expired,
    /// because the only possible outcome is terminal. This lets the store durably remove
    /// an idempotent job whose last worker crashed before acknowledging it.
    pub fn fail(
        &mut self,
        now_ms: i64,
        token: Id,
        error_summary: impl Into<String>,
        retry_after_ms: Option<i64>,
    ) -> Result<(), Error> {
        let terminal = self.attempt >= self.spec.max_attempts;
        if token == Id::ZERO {
            return Err(Error::InvalidLease {
                job_id: self.spec.job_id,
            });
        }
        if self.lease_token != Some(token) {
            return Err(Error::InvalidLease {
                job_id: self.spec.job_id,
            });
        }
        let expired = now_ms >= self.lease_expires_at_ms;
        // Only an idempotent job that has exhausted its attempts may be marked dead after
        // its lease expires without explicit resolution. Expired non-idempotent jobs must
        // transition to `Uncertain` and be resolved by the operator.
        if expired && !(terminal && self.spec.effect_mode == EffectMode::Idempotent) {
            return Err(Error::InvalidLease {
                job_id: self.spec.job_id,
            });
        }
        self.error_summary = Some(error_summary.into());
        self.lease_token = None;
        self.worker_id = None;
        self.lease_expires_at_ms = 0;
        if terminal {
            self.state = JobInternalState::Dead;
            self.terminal_at_ms = Some(now_ms);
            self.retry_after_ms = 0;
        } else {
            self.state = JobInternalState::RetryWait;
            self.retry_after_ms = match retry_after_ms {
                Some(v) => v,
                None => now_ms
                    .checked_add(1000)
                    .ok_or_else(|| Error::Validation("retry after time overflow".into()))?,
            };
        }
        Ok(())
    }

    /// Cancel the job. If a lease token is supplied, it must match an unexpired lease.
    pub fn cancel(&mut self, now_ms: i64, lease_token: Option<Id>) -> Result<(), Error> {
        if self.is_terminal() {
            return Err(Error::Validation(format!(
                "job {} is already terminal",
                self.spec.job_id
            )));
        }
        if let Some(token) = lease_token {
            if token == Id::ZERO
                || self.lease_token != Some(token)
                || now_ms >= self.lease_expires_at_ms
            {
                return Err(Error::InvalidLease {
                    job_id: self.spec.job_id,
                });
            }
        } else if matches!(self.state, JobInternalState::Leased) {
            return Err(Error::InvalidLease {
                job_id: self.spec.job_id,
            });
        }
        self.state = JobInternalState::Cancelled;
        self.terminal_at_ms = Some(now_ms);
        self.lease_token = None;
        self.worker_id = None;
        self.lease_expires_at_ms = 0;
        self.retry_after_ms = 0;
        Ok(())
    }

    /// Resolve an uncertain job outcome.
    pub fn resolve(&mut self, now_ms: i64, resolution: Resolution) -> Result<(), Error> {
        if !self.is_uncertain_at(now_ms) {
            return Err(Error::Validation(format!(
                "job {} is not uncertain and cannot be resolved",
                self.spec.job_id
            )));
        }
        self.lease_token = None;
        self.worker_id = None;
        self.lease_expires_at_ms = 0;
        match resolution {
            Resolution::Retry => {
                // A retry authorizes one more attempt. Decrement the consumed attempt
                // count (the failed lease did not count) and schedule the retry for the
                // standard retry delay.  This preserves the `RetryWait` public state while
                // preventing the attempt ceiling from permanently blocking the partition.
                self.attempt = self
                    .attempt
                    .checked_sub(1)
                    .ok_or_else(|| Error::Validation("retry attempt underflow".into()))?;
                self.state = JobInternalState::RetryWait;
                self.retry_after_ms = now_ms
                    .checked_add(1000)
                    .ok_or_else(|| Error::Validation("resolve retry after time overflow".into()))?;
            }
            Resolution::MarkSucceeded => {
                self.state = JobInternalState::Succeeded;
                self.terminal_at_ms = Some(now_ms);
                self.retry_after_ms = 0;
            }
            Resolution::MarkDead => {
                self.state = JobInternalState::Dead;
                self.terminal_at_ms = Some(now_ms);
                self.retry_after_ms = 0;
            }
        }
        Ok(())
    }

    /// Internal maintenance transition that marks an idempotent job dead after its final
    /// lease expired. It validates the stored lease token and attempt to prevent malformed
    /// on-disk histories from being accepted during replay.
    pub fn expire(&mut self, expired_at_ms: i64, token: Id, attempt: u32) -> Result<(), Error> {
        if self.spec.effect_mode != EffectMode::Idempotent {
            return Err(Error::Validation(format!(
                "job {} cannot expire a non-idempotent effect mode",
                self.spec.job_id
            )));
        }
        if token == Id::ZERO {
            return Err(Error::Validation(format!(
                "job {} expire token cannot be zero",
                self.spec.job_id
            )));
        }
        if !matches!(self.state, JobInternalState::Leased) {
            return Err(Error::Validation(format!(
                "job {} is not leased and cannot expire",
                self.spec.job_id
            )));
        }
        if self.lease_token != Some(token) {
            return Err(Error::InvalidLease {
                job_id: self.spec.job_id,
            });
        }
        if self.attempt != attempt {
            return Err(Error::Validation(format!(
                "job {} expire attempt {} does not match lease attempt {}",
                self.spec.job_id, attempt, self.attempt
            )));
        }
        if self.attempt < self.spec.max_attempts {
            return Err(Error::Validation(format!(
                "job {} expire attempt {} is below max_attempts {}",
                self.spec.job_id, attempt, self.spec.max_attempts
            )));
        }
        if expired_at_ms < self.lease_expires_at_ms {
            return Err(Error::Validation(format!(
                "job {} expired before lease expiry",
                self.spec.job_id
            )));
        }
        self.state = JobInternalState::Dead;
        self.terminal_at_ms = Some(expired_at_ms);
        self.lease_token = None;
        self.worker_id = None;
        self.lease_expires_at_ms = 0;
        self.retry_after_ms = 0;
        self.error_summary = None;
        self.result_digest = None;
        Ok(())
    }

    fn not_before(&self) -> i64 {
        self.spec.not_before_ms
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum JobInternalState {
    Pending,
    Leased,
    RetryWait,
    Succeeded,
    Dead,
    Cancelled,
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::id::Id;

    fn spec(effect_mode: EffectMode) -> JobSpec {
        JobSpec::new(Id::new().unwrap(), "q", "p", b"work".to_vec()).with_effect_mode(effect_mode)
    }

    #[test]
    fn pending_is_ready_after_not_before() {
        let mut job = JobStateRecord::new(spec(EffectMode::Idempotent));
        job.spec.not_before_ms = 100;
        assert_eq!(job.state_at(99), JobState::Pending);
        assert!(!job.is_ready_at(99));
        assert_eq!(job.state_at(100), JobState::Pending);
        assert!(job.is_ready_at(100));
    }

    #[test]
    fn leased_is_ready_when_idempotent_lease_expires() {
        let mut job = JobStateRecord::new(spec(EffectMode::Idempotent));
        job.state = JobInternalState::Leased;
        job.lease_expires_at_ms = 100;
        assert_eq!(job.state_at(100), JobState::Leased);
        assert!(!job.is_ready_at(99));
        assert!(job.is_ready_at(100));
    }

    #[test]
    fn leased_is_uncertain_when_non_idempotent_lease_expires() {
        let mut job = JobStateRecord::new(spec(EffectMode::UncertainOnLeaseExpiry));
        job.state = JobInternalState::Leased;
        job.lease_expires_at_ms = 100;
        assert_eq!(job.state_at(99), JobState::Leased);
        assert!(!job.is_ready_at(99));
        assert!(!job.is_ready_at(100));
        assert!(job.is_uncertain_at(100));
        assert_eq!(job.state_at(100), JobState::Uncertain);
    }

    #[test]
    fn retry_wait_becomes_pending_after_retry_after() {
        let mut job = JobStateRecord::new(spec(EffectMode::Idempotent));
        job.state = JobInternalState::RetryWait;
        job.retry_after_ms = 200;
        assert_eq!(job.state_at(199), JobState::RetryWait);
        assert!(!job.is_ready_at(199));
        assert_eq!(job.state_at(200), JobState::Pending);
        assert!(job.is_ready_at(200));
    }

    #[test]
    fn terminal_states_are_not_ready() {
        for state in [
            JobInternalState::Succeeded,
            JobInternalState::Dead,
            JobInternalState::Cancelled,
        ] {
            let mut job = JobStateRecord::new(spec(EffectMode::Idempotent));
            job.state = state;
            job.lease_expires_at_ms = 0;
            assert!(job.is_terminal());
            assert!(!job.is_ready_at(i64::MAX));
            assert!(!job.is_uncertain_at(i64::MAX));
        }
    }
}
