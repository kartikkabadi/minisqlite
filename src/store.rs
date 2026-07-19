use std::collections::hash_map::Entry;
use std::collections::{BTreeMap, HashMap};
use std::path::{Path, PathBuf};
use std::sync::RwLock;

use crate::codec::encode_records;
use crate::codec::frame::{Frame, FrameHeader, FRAME_HEADER_SIZE, FRAME_TRAILER_SIZE};
use crate::codec::record::{self, Record, Resolution as RecordResolution};
use crate::config::{Durability, Limits};
use crate::error::Error;
use crate::event::{Event, PersistedEvent, StreamVersion};
use crate::id::Id;
use crate::jobs::{
    ClaimRequest, ClaimedJob, JobInternalState, JobSpec, JobState, JobStateRecord, Resolution,
};
use crate::projection::{ProjectionEntry, ProjectionState};
use crate::storage::file::DataFile;
use crate::storage::lock::Lock;
use crate::storage::recovery;
use crate::transaction::{CommitBatch, CommitReceipt, Op};

/// Builder for opening a `Store`.
#[derive(Debug, Clone)]
pub struct StoreBuilder {
    path: PathBuf,
    durability: Durability,
    limits: Limits,
    lock_path: PathBuf,
}

impl StoreBuilder {
    /// Create a builder for a store at `path`.
    pub fn new(path: impl AsRef<Path>) -> Self {
        let path = path.as_ref().to_path_buf();
        let lock_path = path.with_extension("lock");
        Self {
            path,
            durability: Durability::default(),
            limits: Limits::default(),
            lock_path,
        }
    }

    /// Select durability mode.
    pub fn durability(mut self, durability: Durability) -> Self {
        self.durability = durability;
        self
    }

    /// Override default size limits.
    pub fn limits(mut self, limits: Limits) -> Self {
        self.limits = limits;
        self
    }

    /// Set a custom lock-file path.
    pub fn lock_path(mut self, lock_path: impl AsRef<Path>) -> Self {
        self.lock_path = lock_path.as_ref().to_path_buf();
        self
    }

    /// Open or create the store and recover committed state.
    pub fn open(self) -> Result<Store, Error> {
        Store::open(self.path, self.durability, self.limits, self.lock_path)
    }
}

/// The public handle to an open MiniSQLite store.
pub struct Store {
    inner: RwLock<StoreInner>,
}

#[derive(Debug)]
struct StoreInner {
    path: PathBuf,
    #[allow(dead_code)]
    lock: Lock,
    data_file: DataFile,
    limits: Limits,
    poisoned: bool,
    transaction_seq: u64,
    high_water_sequence: u64,
    events: Vec<PersistedEvent>,
    event_ids: HashMap<Id, u64>,
    transaction_batches: HashMap<Id, CommitBatch>,
    transaction_receipts: HashMap<Id, CommitReceipt>,
    stream_versions: HashMap<String, u64>,
    stream_sequences: HashMap<String, Vec<u64>>,
    projections: HashMap<String, ProjectionState>,
    jobs: HashMap<Id, JobStateRecord>,
    queue_partitions: HashMap<(String, String), Vec<Id>>,
    recovered_tail: bool,
}

impl Store {
    fn open(
        path: PathBuf,
        durability: Durability,
        limits: Limits,
        lock_path: PathBuf,
    ) -> Result<Self, Error> {
        limits.validate()?;
        let lock = Lock::acquire(&lock_path)?;
        let mut data_file = DataFile::open_or_create(&path, durability)?;
        let scan = recovery::scan(&mut data_file)?;

        let mut inner = StoreInner {
            path,
            lock,
            data_file,
            limits,
            poisoned: false,
            transaction_seq: 0,
            high_water_sequence: 0,
            events: Vec::new(),
            event_ids: HashMap::new(),
            transaction_batches: HashMap::new(),
            transaction_receipts: HashMap::new(),
            stream_versions: HashMap::new(),
            stream_sequences: HashMap::new(),
            projections: HashMap::new(),
            jobs: HashMap::new(),
            queue_partitions: HashMap::new(),
            recovered_tail: scan.tail_truncated,
        };

        let mut frame_offset = crate::codec::frame::FILE_HEADER_SIZE as u64;
        for frame in &scan.frames {
            inner.replay_frame(frame, frame_offset)?;
            frame_offset += frame.header.total_frame_length;
        }

        if scan.tail_truncated {
            inner.data_file.truncate(scan.last_valid_offset)?;
        }

        Ok(Self {
            inner: RwLock::new(inner),
        })
    }

    /// Atomically commit a batch of events, projection mutations, and job operations.
    pub fn commit(&self, batch: CommitBatch) -> Result<CommitReceipt, Error> {
        let mut guard = self.inner.write().unwrap();
        guard.commit(batch)
    }

    /// Claim ready jobs from a queue.
    pub fn claim_jobs(&self, request: ClaimRequest) -> Result<Vec<ClaimedJob>, Error> {
        let mut guard = self.inner.write().unwrap();
        guard.claim_jobs(request)
    }

    /// Acknowledge a job with a current lease token.
    pub fn ack_job(
        &self,
        job_id: Id,
        lease_token: Id,
        result_digest: Option<Vec<u8>>,
        now_ms: i64,
    ) -> Result<CommitReceipt, Error> {
        let batch =
            CommitBatch::new(Id::new(), now_ms).acknowledge_job(job_id, lease_token, result_digest);
        self.commit(batch)
    }

    /// Record a job failure.
    pub fn fail_job(
        &self,
        job_id: Id,
        lease_token: Id,
        error_summary: impl Into<String>,
        retry_after_ms: Option<i64>,
        now_ms: i64,
    ) -> Result<CommitReceipt, Error> {
        let batch = CommitBatch::new(Id::new(), now_ms).fail_job(
            job_id,
            lease_token,
            error_summary,
            retry_after_ms,
        );
        self.commit(batch)
    }

    /// Cancel a job.
    pub fn cancel_job(
        &self,
        job_id: Id,
        lease_token: Option<Id>,
        now_ms: i64,
    ) -> Result<CommitReceipt, Error> {
        let batch = CommitBatch::new(Id::new(), now_ms).cancel_job(job_id, lease_token);
        self.commit(batch)
    }

    /// Resolve an uncertain job outcome.
    pub fn resolve_uncertain_job(
        &self,
        job_id: Id,
        resolution: Resolution,
        now_ms: i64,
    ) -> Result<CommitReceipt, Error> {
        let batch = CommitBatch::new(Id::new(), now_ms).resolve_uncertain_job(job_id, resolution);
        self.commit(batch)
    }

    /// Read events after a global sequence, ordered by sequence.
    pub fn events_after(&self, sequence_exclusive: u64, limit: usize) -> Vec<PersistedEvent> {
        let guard = self.inner.read().unwrap();
        guard
            .events
            .iter()
            .filter(|e| e.global_sequence > sequence_exclusive)
            .take(limit)
            .cloned()
            .collect()
    }

    /// Read events for one stream after a stream version.
    pub fn stream_events(
        &self,
        stream_id: impl AsRef<str>,
        version_exclusive: u64,
        limit: usize,
    ) -> Vec<PersistedEvent> {
        let guard = self.inner.read().unwrap();
        let stream_id = stream_id.as_ref();
        guard
            .events
            .iter()
            .filter(|e| e.event.stream_id == stream_id && e.stream_version > version_exclusive)
            .take(limit)
            .cloned()
            .collect()
    }

    /// Get a single event by ID.
    pub fn get_event(&self, event_id: Id) -> Result<PersistedEvent, Error> {
        let guard = self.inner.read().unwrap();
        let seq = guard
            .event_ids
            .get(&event_id)
            .copied()
            .ok_or(Error::EventNotFound(event_id))?;
        let idx = (seq as usize).saturating_sub(1);
        guard
            .events
            .get(idx)
            .cloned()
            .ok_or(Error::EventNotFound(event_id))
    }

    /// Get the receipt for a committed transaction.
    pub fn get_transaction(&self, transaction_id: Id) -> Result<CommitReceipt, Error> {
        let guard = self.inner.read().unwrap();
        guard
            .transaction_receipts
            .get(&transaction_id)
            .cloned()
            .ok_or(Error::TransactionNotFound(transaction_id))
    }

    /// Return the highest committed global event sequence.
    pub fn high_water_sequence(&self) -> u64 {
        let guard = self.inner.read().unwrap();
        guard.high_water_sequence
    }

    /// Return the current version of a stream, if it exists.
    pub fn stream_version(&self, stream_id: impl AsRef<str>) -> Option<u64> {
        let guard = self.inner.read().unwrap();
        guard.stream_versions.get(stream_id.as_ref()).copied()
    }

    /// Get a single projection value by exact key.
    pub fn get_projection(
        &self,
        projection: impl AsRef<str>,
        key: &[u8],
    ) -> Result<Option<Vec<u8>>, Error> {
        let guard = self.inner.read().unwrap();
        let state = guard
            .projections
            .get(projection.as_ref())
            .ok_or_else(|| Error::ProjectionNotFound(projection.as_ref().to_string()))?;
        Ok(state.data.get(key).cloned())
    }

    /// Return the names of all projections.
    pub fn projection_names(&self) -> Vec<String> {
        let guard = self.inner.read().unwrap();
        guard.projections.keys().cloned().collect()
    }

    /// Return the current version of a projection.
    pub fn projection_version(&self, projection: impl AsRef<str>) -> Result<u64, Error> {
        let guard = self.inner.read().unwrap();
        let state = guard
            .projections
            .get(projection.as_ref())
            .ok_or_else(|| Error::ProjectionNotFound(projection.as_ref().to_string()))?;
        Ok(state.version)
    }

    /// Scan a projection for keys with the given prefix.
    pub fn scan_projection_prefix(
        &self,
        projection: impl AsRef<str>,
        prefix: &[u8],
    ) -> Result<Vec<ProjectionEntry>, Error> {
        let guard = self.inner.read().unwrap();
        let state = guard
            .projections
            .get(projection.as_ref())
            .ok_or_else(|| Error::ProjectionNotFound(projection.as_ref().to_string()))?;
        if prefix.is_empty() {
            return Ok(state
                .data
                .iter()
                .map(|(k, v)| ProjectionEntry::new(k.clone(), v.clone()))
                .collect());
        }
        match prefix_upper_bound(prefix) {
            Some(upper) => Ok(state
                .data
                .range(prefix.to_vec()..upper)
                .map(|(k, v)| ProjectionEntry::new(k.clone(), v.clone()))
                .collect()),
            None => Ok(state
                .data
                .range(prefix.to_vec()..)
                .map(|(k, v)| ProjectionEntry::new(k.clone(), v.clone()))
                .collect()),
        }
    }

    /// Scan a projection over a key range.
    pub fn scan_projection_range(
        &self,
        projection: impl AsRef<str>,
        start: &[u8],
        end: &[u8],
    ) -> Result<Vec<ProjectionEntry>, Error> {
        let guard = self.inner.read().unwrap();
        let state = guard
            .projections
            .get(projection.as_ref())
            .ok_or_else(|| Error::ProjectionNotFound(projection.as_ref().to_string()))?;
        Ok(state
            .data
            .range(start.to_vec()..end.to_vec())
            .map(|(k, v)| ProjectionEntry::new(k.clone(), v.clone()))
            .collect())
    }

    /// Return job records filtered by optional queue and state as of `now_ms`.
    pub fn jobs(
        &self,
        now_ms: i64,
        queue: Option<String>,
        state: Option<JobState>,
    ) -> Vec<(Id, JobSpec, JobState)> {
        let guard = self.inner.read().unwrap();
        guard
            .jobs
            .values()
            .filter(|j| queue.as_ref().map(|q| &j.spec.queue == q).unwrap_or(true))
            .filter(|j| state.map(|s| j.state_at(now_ms) == s).unwrap_or(true))
            .map(|j| (j.spec.job_id, j.spec.clone(), j.state_at(now_ms)))
            .collect()
    }

    /// Return the job state for a single job at `now_ms`.
    pub fn job_state(&self, job_id: Id, now_ms: i64) -> Result<JobState, Error> {
        let guard = self.inner.read().unwrap();
        guard
            .jobs
            .get(&job_id)
            .map(|j| j.state_at(now_ms))
            .ok_or(Error::JobNotFound(job_id))
    }

    /// Return current store diagnostics.
    pub fn stats(&self) -> StoreStats {
        let guard = self.inner.read().unwrap();
        let now_ms = current_time_ms();
        let mut job_counts: HashMap<JobState, usize> = HashMap::new();
        for j in guard.jobs.values() {
            *job_counts.entry(j.state_at(now_ms)).or_insert(0) += 1;
        }
        let (format_version_major, format_version_minor) = guard.data_file.format_version();
        StoreStats {
            file_size: guard.data_file.file_len(),
            transaction_count: guard.transaction_seq,
            event_count: guard.events.len() as u64,
            stream_count: guard.stream_versions.len() as u64,
            projection_count: guard.projections.len() as u64,
            job_count: guard.jobs.len() as u64,
            job_counts,
            last_transaction_sequence: guard.transaction_seq,
            last_event_sequence: guard.high_water_sequence,
            recovered_tail: guard.recovered_tail,
            poisoned: guard.poisoned,
            format_version_major,
            format_version_minor,
        }
    }

    /// Re-read and verify the entire file. Returns `Ok(())` if every frame is intact.
    pub fn verify(&self) -> Result<(), Error> {
        let mut guard = self.inner.write().unwrap();
        let _scan = recovery::scan(&mut guard.data_file)?;
        Ok(())
    }

    /// Copy the primary file to `destination` with safe temporary-file semantics.
    ///
    /// The copy is written to a sibling temporary file, fsynced, and atomically renamed onto
    /// `destination`. On any failure the temporary file is removed. The destination is then
    /// reopened and scanned to prove it is a consistent copy.
    pub fn backup(&self, destination: impl AsRef<Path>) -> Result<(), Error> {
        let mut guard = self.inner.write().unwrap();
        guard.data_file.sync()?;
        let src_path = guard.path.clone();
        let dest = destination.as_ref().to_path_buf();
        let tmp = dest.with_extension("mini.tmp");

        let result: Result<(), Error> = (|| {
            std::fs::copy(&src_path, &tmp)?;
            #[cfg(unix)]
            {
                use std::os::unix::fs::PermissionsExt;
                std::fs::set_permissions(&tmp, std::fs::Permissions::from_mode(0o600))?;
            }
            let mut tmp_file = DataFile::open_or_create(&tmp, Durability::Memory)?;
            tmp_file.sync()?;
            drop(tmp_file);
            std::fs::rename(&tmp, &dest)?;
            let mut backup_file = DataFile::open_or_create(&dest, Durability::Memory)?;
            let _scan = recovery::scan(&mut backup_file)?;
            Ok(())
        })();

        if result.is_err() {
            let _ = std::fs::remove_file(&tmp);
        }
        result
    }

    /// Return the file path.
    pub fn path(&self) -> PathBuf {
        let guard = self.inner.read().unwrap();
        guard.path.clone()
    }

    /// Returns true if the store is poisoned and must be reopened.
    pub fn is_poisoned(&self) -> bool {
        let guard = self.inner.read().unwrap();
        guard.poisoned
    }

    /// Close the store, flushing any pending writes and releasing the file lock.
    pub fn close(self) -> Result<(), Error> {
        let mut guard = self.inner.write().unwrap();
        guard.data_file.sync()?;
        Ok(())
    }
}

impl Drop for Store {
    fn drop(&mut self) {
        let _ = self.inner.write().map(|mut g| g.data_file.sync());
    }
}

impl StoreInner {
    fn replay_frame(&mut self, frame: &Frame, frame_offset: u64) -> Result<(), Error> {
        if frame.header.transaction_sequence != self.transaction_seq + 1 {
            return Err(Error::Corruption {
                message: "transaction sequence regression or gap".into(),
                offset: 0,
            });
        }
        if self
            .transaction_batches
            .contains_key(&frame.header.transaction_id)
        {
            return Err(Error::Corruption {
                message: "duplicate transaction id in committed history".into(),
                offset: 0,
            });
        }

        let records = record::decode_records(&frame.payload)?;

        // Reconstruct the original commit batch and re-validate every operation against
        // the state rebuilt so far. This catches corrupted frames that pass checksums.
        // We do not re-run `validate_batch` here because configured `Limits` can change
        // between runs; committed frames are bounded by the hard frame-size limit and are
        // decoded safely without allocating from untrusted lengths.
        let batch = CommitBatch::from_records(
            frame.header.transaction_id,
            frame.header.commit_timestamp_ms,
            records.clone(),
        )?;
        self.validate_projection_ops(&batch)?;
        self.validate_job_ops(&batch)?;
        let expected = self.ops_to_records(&batch)?;
        if expected != records {
            return Err(Error::Corruption {
                message: "frame records do not match re-validated commit".into(),
                offset: 0,
            });
        }

        self.apply_records(&records, frame.header.transaction_id, frame_offset)?;
        self.transaction_seq = frame.header.transaction_sequence;

        // Reconstruct receipt.
        let receipt = build_receipt(
            &records,
            frame.header.transaction_id,
            frame.header.transaction_sequence,
            frame_offset,
        );
        self.transaction_batches
            .insert(frame.header.transaction_id, batch);
        self.transaction_receipts
            .insert(frame.header.transaction_id, receipt);
        Ok(())
    }

    fn apply_records(
        &mut self,
        records: &[Record],
        transaction_id: Id,
        frame_offset: u64,
    ) -> Result<(), Error> {
        for record in records {
            match record {
                Record::Event(e) => {
                    if self.event_ids.contains_key(&e.event_id) {
                        return Err(Error::Corruption {
                            message: "duplicate event id in frame".into(),
                            offset: 0,
                        });
                    }
                    self.high_water_sequence = self.high_water_sequence.max(e.global_sequence);
                    self.events.push(PersistedEvent {
                        transaction_id,
                        global_sequence: e.global_sequence,
                        stream_version: e.stream_version,
                        event: Event {
                            event_id: e.event_id,
                            stream_id: e.stream_id.clone(),
                            event_type: e.event_type.clone(),
                            schema_version: e.schema_version,
                            occurred_at_ms: e.occurred_at_ms,
                            causation_id: e.causation_id,
                            correlation_id: e.correlation_id,
                            payload: e.payload.clone(),
                            metadata: e.metadata.clone(),
                        },
                        frame_offset,
                    });
                    self.event_ids.insert(e.event_id, e.global_sequence);
                    let version = self.stream_versions.entry(e.stream_id.clone()).or_insert(0);
                    *version = (*version).max(e.stream_version);
                    self.stream_sequences
                        .entry(e.stream_id.clone())
                        .or_default()
                        .push(e.global_sequence);
                }
                Record::ProjectionPut {
                    projection,
                    version,
                    key,
                    value,
                } => {
                    let state = self.projections.entry(projection.clone()).or_default();
                    state.data.insert(key.clone(), value.clone());
                    state.version = *version;
                }
                Record::ProjectionDelete {
                    projection,
                    version,
                    key,
                } => {
                    if let Some(state) = self.projections.get_mut(projection) {
                        state.data.remove(key);
                        state.version = *version;
                    }
                }
                Record::ProjectionClear {
                    projection,
                    new_version,
                } => {
                    let state = self.projections.entry(projection.clone()).or_default();
                    state.data.clear();
                    state.version = *new_version;
                }
                Record::ProjectionReplace {
                    projection,
                    new_version,
                    entries,
                } => {
                    let mut data = BTreeMap::new();
                    for (k, v) in entries {
                        data.insert(k.clone(), v.clone());
                    }
                    self.projections.insert(
                        projection.clone(),
                        ProjectionState {
                            version: *new_version,
                            data,
                        },
                    );
                }
                Record::JobEnqueue {
                    job_id,
                    queue,
                    partition,
                    payload,
                    not_before_ms,
                    max_attempts,
                    effect_mode,
                    idempotency_key,
                } => {
                    let spec = JobSpec {
                        job_id: *job_id,
                        queue: queue.clone(),
                        partition: partition.clone(),
                        payload: payload.clone(),
                        not_before_ms: *not_before_ms,
                        max_attempts: *max_attempts,
                        effect_mode: *effect_mode,
                        idempotency_key: idempotency_key.clone(),
                    };
                    if let Entry::Vacant(e) = self.jobs.entry(*job_id) {
                        self.queue_partitions
                            .entry((queue.clone(), partition.clone()))
                            .or_default()
                            .push(*job_id);
                        e.insert(JobStateRecord::new(spec));
                    }
                }
                Record::JobLease {
                    job_id,
                    lease_token,
                    worker_id,
                    attempt,
                    lease_expires_at_ms,
                    ..
                } => {
                    if let Some(job) = self.jobs.get_mut(job_id) {
                        job.state = JobInternalState::Leased;
                        job.lease_token = Some(*lease_token);
                        job.worker_id = Some(worker_id.clone());
                        job.attempt = *attempt;
                        job.lease_expires_at_ms = *lease_expires_at_ms;
                    }
                }
                Record::JobAck {
                    job_id,
                    result_digest,
                    acknowledged_at_ms,
                    ..
                } => {
                    if let Some(job) = self.jobs.get_mut(job_id) {
                        job.state = JobInternalState::Succeeded;
                        job.result_digest = result_digest.clone();
                        job.terminal_at_ms = Some(*acknowledged_at_ms);
                        job.lease_token = None;
                    }
                }
                Record::JobFail {
                    job_id,
                    error_summary,
                    retry_after_ms,
                    terminal,
                    failed_at_ms,
                    ..
                } => {
                    if let Some(job) = self.jobs.get_mut(job_id) {
                        if *terminal {
                            job.state = JobInternalState::Dead;
                            job.terminal_at_ms = Some(*failed_at_ms);
                        } else {
                            job.state = JobInternalState::RetryWait;
                            job.retry_after_ms = *retry_after_ms;
                        }
                        job.error_summary = Some(error_summary.clone());
                        job.lease_token = None;
                    }
                }
                Record::JobCancel {
                    job_id,
                    cancelled_at_ms,
                    ..
                } => {
                    if let Some(job) = self.jobs.get_mut(job_id) {
                        job.state = JobInternalState::Cancelled;
                        job.terminal_at_ms = Some(*cancelled_at_ms);
                        job.lease_token = None;
                    }
                }
                Record::JobResolve {
                    job_id,
                    resolution,
                    resolved_at_ms,
                } => {
                    if let Some(job) = self.jobs.get_mut(job_id) {
                        match resolution {
                            RecordResolution::Retry => {
                                job.state = JobInternalState::RetryWait;
                                job.retry_after_ms = *resolved_at_ms;
                                job.lease_token = None;
                            }
                            RecordResolution::MarkSucceeded => {
                                job.state = JobInternalState::Succeeded;
                                job.terminal_at_ms = Some(*resolved_at_ms);
                                job.lease_token = None;
                            }
                            RecordResolution::MarkDead => {
                                job.state = JobInternalState::Dead;
                                job.terminal_at_ms = Some(*resolved_at_ms);
                                job.lease_token = None;
                            }
                        }
                    }
                }
            }
        }
        Ok(())
    }

    fn commit(&mut self, batch: CommitBatch) -> Result<CommitReceipt, Error> {
        if self.poisoned {
            return Err(Error::StorePoisoned {
                transaction_id: batch.transaction_id,
            });
        }

        // Idempotency is checked first so a resubmitted transaction returns its original
        // receipt without re-running validation that would fail on already-committed event IDs.
        if let Some(prev) = self.transaction_batches.get(&batch.transaction_id) {
            if batch.logical_eq(prev) {
                return Ok(self
                    .transaction_receipts
                    .get(&batch.transaction_id)
                    .cloned()
                    .unwrap());
            }
            return Err(Error::DuplicateIdWithDifferentContent {
                kind: "transaction",
                id: batch.transaction_id,
            });
        }

        self.validate_batch(&batch)?;
        self.validate_projection_ops(&batch)?;
        self.validate_job_ops(&batch)?;

        let records = self.ops_to_records(&batch)?;
        let payload_bytes = encode_records(&records);

        if payload_bytes.len()
            > self
                .limits
                .max_frame_size
                .saturating_sub(FRAME_HEADER_SIZE + FRAME_TRAILER_SIZE)
        {
            return Err(Error::PayloadTooLarge {
                kind: "transaction frame",
                size: payload_bytes.len(),
                limit: self.limits.max_frame_size,
            });
        }

        let frame_header = FrameHeader {
            version: 1,
            total_frame_length: 0,
            transaction_sequence: self.transaction_seq + 1,
            transaction_id: batch.transaction_id,
            commit_timestamp_ms: batch.now_ms,
            record_count: records.len() as u32,
            payload_length: payload_bytes.len() as u32,
        };
        let mut frame = Frame::new(frame_header, payload_bytes);
        frame.header.record_count = records.len() as u32;
        let frame_offset = self.data_file.file_len();
        let frame_bytes = frame.encode();

        let original_file_len = self.data_file.file_len();

        if let Err(e) = self
            .data_file
            .append_frame(&frame_bytes, frame.header.payload_length as u64)
        {
            // Attempt to roll back the partial append. If the rollback cannot be
            // confirmed (truncate or its sync fails), the outcome is uncertain:
            // some or all of the frame may be on disk.
            let rollback_ok = self
                .data_file
                .truncate(original_file_len)
                .map(|()| self.data_file.file_len() == original_file_len)
                .unwrap_or(false);
            if rollback_ok {
                return Err(Error::Io(e.to_string()));
            }
            self.poisoned = true;
            return Err(Error::CommitOutcomeUncertain {
                transaction_id: batch.transaction_id,
                original_file_len,
                source: e.to_string(),
            });
        }

        // Failpoint: before memory apply.
        #[cfg(feature = "failpoint")]
        {
            if std::env::var_os("MINISQLITE_FAILPOINT").as_deref()
                == Some(std::ffi::OsStr::new("before-memory-apply"))
            {
                std::process::abort();
            }
        }

        // Apply the staged delta to memory. From here on, the operation is infallible.
        let receipt = self.apply_commit(&batch, frame_offset, records)?;

        // Failpoint: after memory apply.
        #[cfg(feature = "failpoint")]
        {
            if std::env::var_os("MINISQLITE_FAILPOINT").as_deref()
                == Some(std::ffi::OsStr::new("after-memory-apply"))
            {
                std::process::abort();
            }
        }

        Ok(receipt)
    }

    fn apply_commit(
        &mut self,
        batch: &CommitBatch,
        frame_offset: u64,
        records: Vec<Record>,
    ) -> Result<CommitReceipt, Error> {
        self.transaction_batches
            .insert(batch.transaction_id, batch.clone());
        self.apply_records(&records, batch.transaction_id, frame_offset)?;
        self.transaction_seq += 1;

        let receipt = build_receipt(
            &records,
            batch.transaction_id,
            self.transaction_seq,
            frame_offset,
        );
        self.transaction_receipts
            .insert(batch.transaction_id, receipt.clone());
        Ok(receipt)
    }

    fn validate_batch(&self, batch: &CommitBatch) -> Result<(), Error> {
        if batch.ops.len() > self.limits.max_records_per_transaction {
            return Err(Error::Validation(format!(
                "too many records: {} > {}",
                batch.ops.len(),
                self.limits.max_records_per_transaction
            )));
        }
        for op in &batch.ops {
            match op {
                Op::AppendEvent(e) => {
                    self.limits
                        .validate_event(e.payload.len(), e.metadata.len())?;
                    self.limits.validate_string("stream_id", &e.stream_id)?;
                    self.limits.validate_string("event_type", &e.event_type)?;
                }
                Op::ProjectionPut { key, value, .. } => {
                    self.limits.validate_projection_key(key.len())?;
                    self.limits.validate_projection_value(value.len())?;
                }
                Op::ProjectionDelete { key, .. } => {
                    self.limits.validate_projection_key(key.len())?;
                }
                Op::ProjectionReplace { entries, .. } => {
                    if entries.len() > self.limits.max_replace_entries {
                        return Err(Error::PayloadTooLarge {
                            kind: "projection replace entries",
                            size: entries.len(),
                            limit: self.limits.max_replace_entries,
                        });
                    }
                    for ProjectionEntry { key, value } in entries {
                        self.limits.validate_projection_key(key.len())?;
                        self.limits.validate_projection_value(value.len())?;
                    }
                }
                Op::EnqueueJob(job) => {
                    self.limits.validate_job_payload(job.payload.len())?;
                    self.limits.validate_string("queue", &job.queue)?;
                    self.limits.validate_string("partition", &job.partition)?;
                    if let Some(ref key) = job.idempotency_key {
                        self.limits.validate_string("idempotency_key", key)?;
                    }
                }
                Op::FailJob { error_summary, .. } => {
                    self.limits.validate_summary(error_summary)?;
                }
                Op::LeaseJob { worker_id, .. } => {
                    self.limits.validate_string("worker_id", worker_id)?;
                }
                Op::AckJob {
                    result_digest: Some(digest),
                    ..
                } => {
                    self.limits.validate_metadata(digest.len())?;
                }
                _ => {}
            }
        }
        Ok(())
    }

    fn validate_projection_ops(&self, batch: &CommitBatch) -> Result<(), Error> {
        let mut simulated: HashMap<String, ProjectionState> = HashMap::new();
        for op in &batch.ops {
            let (projection, version) = match op {
                Op::ProjectionPut {
                    projection,
                    version,
                    ..
                } => (projection, version),
                Op::ProjectionDelete {
                    projection,
                    version,
                    ..
                } => (projection, version),
                Op::ProjectionClear {
                    projection,
                    new_version,
                } => (projection, new_version),
                Op::ProjectionReplace {
                    projection,
                    new_version,
                    ..
                } => (projection, new_version),
                _ => continue,
            };

            let current = self
                .projections
                .get(projection)
                .cloned()
                .unwrap_or_else(ProjectionState::new);
            let sim = simulated.entry(projection.clone()).or_insert(current);
            if *version == sim.version && !Self::projection_op_changes_state(op, sim) {
                continue;
            }
            if *version != sim.version + 1 {
                return Err(Error::ProjectionVersionMismatch {
                    projection: projection.clone(),
                    current: sim.version,
                    supplied: *version,
                });
            }
            Self::apply_projection_op_to_state(op, sim);
            sim.version = *version;
        }
        Ok(())
    }

    fn projection_op_changes_state(op: &Op, state: &ProjectionState) -> bool {
        match op {
            Op::ProjectionPut { key, value, .. } => state.data.get(key) != Some(value),
            Op::ProjectionDelete { key, .. } => state.data.contains_key(key),
            Op::ProjectionClear { .. } => !state.data.is_empty(),
            Op::ProjectionReplace { entries, .. } => {
                if entries.len() != state.data.len() {
                    return true;
                }
                entries
                    .iter()
                    .any(|ProjectionEntry { key, value }| state.data.get(key) != Some(value))
            }
            _ => false,
        }
    }

    fn apply_projection_op_to_state(op: &Op, state: &mut ProjectionState) {
        match op {
            Op::ProjectionPut { key, value, .. } => {
                state.data.insert(key.clone(), value.clone());
            }
            Op::ProjectionDelete { key, .. } => {
                state.data.remove(key);
            }
            Op::ProjectionClear { .. } => {
                state.data.clear();
            }
            Op::ProjectionReplace { entries, .. } => {
                state.data.clear();
                for ProjectionEntry { key, value } in entries {
                    state.data.insert(key.clone(), value.clone());
                }
            }
            _ => {}
        }
    }

    fn validate_job_ops(&self, batch: &CommitBatch) -> Result<(), Error> {
        let mut simulated: HashMap<Id, JobStateRecord> = HashMap::new();
        for op in &batch.ops {
            match op {
                Op::EnqueueJob(spec) => {
                    if let Some(existing) = self.jobs.get(&spec.job_id) {
                        if existing.spec != *spec {
                            return Err(Error::DuplicateJobId(spec.job_id));
                        }
                        continue;
                    }
                    if let Some(first) = simulated.get(&spec.job_id) {
                        if first.spec != *spec {
                            return Err(Error::DuplicateJobId(spec.job_id));
                        }
                        continue;
                    }
                    simulated.insert(spec.job_id, JobStateRecord::new(spec.clone()));
                }
                Op::LeaseJob {
                    job_id,
                    lease_token,
                    worker_id,
                    attempt,
                    lease_expires_at_ms,
                } => {
                    let mut job = self.get_job_or_simulated(&mut simulated, *job_id)?;
                    if job.is_terminal() || !job.is_ready_at(batch.now_ms) {
                        return Err(Error::Validation(format!(
                            "job {job_id} is not ready for lease"
                        )));
                    }
                    job.state = JobInternalState::Leased;
                    job.lease_token = Some(*lease_token);
                    job.worker_id = Some(worker_id.clone());
                    job.attempt = *attempt;
                    job.lease_expires_at_ms = *lease_expires_at_ms;
                    simulated.insert(*job_id, job);
                }
                Op::AckJob {
                    job_id,
                    lease_token,
                    ..
                }
                | Op::FailJob {
                    job_id,
                    lease_token,
                    ..
                } => {
                    let mut job = self.get_job_or_simulated(&mut simulated, *job_id)?;
                    if job.lease_token != Some(*lease_token)
                        || batch.now_ms >= job.lease_expires_at_ms
                    {
                        return Err(Error::InvalidLease { job_id: *job_id });
                    }
                    if let Op::FailJob {
                        error_summary,
                        retry_after_ms,
                        ..
                    } = op
                    {
                        let terminal = job.attempt >= job.spec.max_attempts;
                        if terminal {
                            job.state = JobInternalState::Dead;
                            job.terminal_at_ms = Some(batch.now_ms);
                        } else {
                            job.state = JobInternalState::RetryWait;
                            job.retry_after_ms = retry_after_ms.unwrap_or(batch.now_ms + 1000);
                        }
                        job.error_summary = Some(error_summary.clone());
                    } else {
                        job.state = JobInternalState::Succeeded;
                        job.terminal_at_ms = Some(batch.now_ms);
                    }
                    job.lease_token = None;
                    simulated.insert(*job_id, job);
                }
                Op::CancelJob {
                    job_id,
                    lease_token,
                } => {
                    let mut job = self.get_job_or_simulated(&mut simulated, *job_id)?;
                    if job.is_terminal() {
                        return Err(Error::Validation(format!(
                            "job {job_id} is already terminal"
                        )));
                    }
                    if let Some(token) = lease_token {
                        if job.lease_token != Some(*token)
                            || batch.now_ms >= job.lease_expires_at_ms
                        {
                            return Err(Error::InvalidLease { job_id: *job_id });
                        }
                    } else if matches!(job.state, JobInternalState::Leased) {
                        return Err(Error::InvalidLease { job_id: *job_id });
                    }
                    job.state = JobInternalState::Cancelled;
                    job.terminal_at_ms = Some(batch.now_ms);
                    job.lease_token = None;
                    simulated.insert(*job_id, job);
                }
                Op::ResolveJob { job_id, .. } => {
                    let job = self.get_job_or_simulated(&mut simulated, *job_id)?;
                    if !job.is_uncertain_at(batch.now_ms) {
                        return Err(Error::Validation(format!(
                            "job {job_id} is not uncertain and cannot be resolved"
                        )));
                    }
                }
                _ => {}
            }
        }
        Ok(())
    }

    fn get_job_or_simulated(
        &self,
        simulated: &mut HashMap<Id, JobStateRecord>,
        job_id: Id,
    ) -> Result<JobStateRecord, Error> {
        if let Some(job) = simulated.get(&job_id) {
            return Ok(job.clone());
        }
        self.jobs
            .get(&job_id)
            .cloned()
            .ok_or(Error::JobNotFound(job_id))
    }

    fn ops_to_records(&self, batch: &CommitBatch) -> Result<Vec<Record>, Error> {
        let mut records = Vec::with_capacity(batch.ops.len());
        let mut next_global_seq = self.high_water_sequence + 1;
        let mut per_stream_next_version: HashMap<String, u64> = HashMap::new();

        for (stream_id, expected) in &batch.expected_stream_versions {
            let current = self.stream_versions.get(stream_id).copied().unwrap_or(0);
            if current != *expected {
                return Err(Error::Conflict {
                    stream_id: stream_id.clone(),
                    expected: *expected,
                    actual: current,
                });
            }
            per_stream_next_version.insert(stream_id.clone(), current + 1);
        }

        // Also ensure no duplicate event IDs within the batch.
        let mut seen_event_ids: HashMap<Id, &Event> = HashMap::new();

        // Simulated job state so ops within the same batch see earlier job transitions
        // (e.g., a lease immediately followed by a fail in one atomic commit).
        let mut job_sim: HashMap<Id, JobStateRecord> = HashMap::new();

        for op in &batch.ops {
            match op {
                Op::AppendEvent(e) => {
                    if let Some(prev) = seen_event_ids.get(&e.event_id) {
                        if *prev != e {
                            return Err(Error::DuplicateEventId(e.event_id));
                        }
                        continue;
                    }
                    if self.event_ids.contains_key(&e.event_id) {
                        return Err(Error::DuplicateEventId(e.event_id));
                    }
                    seen_event_ids.insert(e.event_id, e);

                    let stream_version = per_stream_next_version
                        .entry(e.stream_id.clone())
                        .or_insert_with(|| {
                            self.stream_versions.get(&e.stream_id).copied().unwrap_or(0) + 1
                        });
                    let sv = *stream_version;
                    *stream_version += 1;
                    let gs = next_global_seq;
                    next_global_seq += 1;
                    records.push(Record::Event(record::EventRecord {
                        global_sequence: gs,
                        stream_version: sv,
                        event_id: e.event_id,
                        stream_id: e.stream_id.clone(),
                        event_type: e.event_type.clone(),
                        schema_version: e.schema_version,
                        occurred_at_ms: e.occurred_at_ms,
                        causation_id: e.causation_id,
                        correlation_id: e.correlation_id,
                        payload: e.payload.clone(),
                        metadata: e.metadata.clone(),
                    }));
                }
                Op::ProjectionPut {
                    projection,
                    version,
                    key,
                    value,
                } => {
                    records.push(Record::ProjectionPut {
                        projection: projection.clone(),
                        version: *version,
                        key: key.clone(),
                        value: value.clone(),
                    });
                }
                Op::ProjectionDelete {
                    projection,
                    version,
                    key,
                } => {
                    records.push(Record::ProjectionDelete {
                        projection: projection.clone(),
                        version: *version,
                        key: key.clone(),
                    });
                }
                Op::ProjectionClear {
                    projection,
                    new_version,
                } => {
                    records.push(Record::ProjectionClear {
                        projection: projection.clone(),
                        new_version: *new_version,
                    });
                }
                Op::ProjectionReplace {
                    projection,
                    new_version,
                    entries,
                } => {
                    let mapped: Vec<(Vec<u8>, Vec<u8>)> = entries
                        .iter()
                        .map(|e| (e.key.clone(), e.value.clone()))
                        .collect();
                    records.push(Record::ProjectionReplace {
                        projection: projection.clone(),
                        new_version: *new_version,
                        entries: mapped,
                    });
                }
                Op::EnqueueJob(job) => {
                    records.push(Record::JobEnqueue {
                        job_id: job.job_id,
                        queue: job.queue.clone(),
                        partition: job.partition.clone(),
                        payload: job.payload.clone(),
                        not_before_ms: job.not_before_ms,
                        max_attempts: job.max_attempts,
                        effect_mode: job.effect_mode,
                        idempotency_key: job.idempotency_key.clone(),
                    });
                    job_sim.insert(job.job_id, JobStateRecord::new(job.clone()));
                }
                Op::AckJob {
                    job_id,
                    lease_token,
                    result_digest,
                } => {
                    let mut job = job_sim
                        .get(job_id)
                        .or_else(|| self.jobs.get(job_id))
                        .cloned()
                        .ok_or(Error::JobNotFound(*job_id))?;
                    job.state = JobInternalState::Succeeded;
                    job.lease_token = None;
                    job.result_digest = result_digest.clone();
                    job.terminal_at_ms = Some(batch.now_ms);
                    job_sim.insert(*job_id, job);
                    records.push(Record::JobAck {
                        job_id: *job_id,
                        lease_token: *lease_token,
                        result_digest: result_digest.clone(),
                        acknowledged_at_ms: batch.now_ms,
                    });
                }
                Op::FailJob {
                    job_id,
                    lease_token,
                    error_summary,
                    retry_after_ms,
                } => {
                    let mut job = job_sim
                        .get(job_id)
                        .or_else(|| self.jobs.get(job_id))
                        .cloned()
                        .ok_or(Error::JobNotFound(*job_id))?;
                    let terminal = job.attempt >= job.spec.max_attempts;
                    let retry_after = if terminal {
                        0
                    } else {
                        retry_after_ms.unwrap_or(batch.now_ms + 1000)
                    };
                    if terminal {
                        job.state = JobInternalState::Dead;
                        job.terminal_at_ms = Some(batch.now_ms);
                    } else {
                        job.state = JobInternalState::RetryWait;
                        job.retry_after_ms = retry_after;
                    }
                    job.lease_token = None;
                    job.error_summary = Some(error_summary.clone());
                    job_sim.insert(*job_id, job);
                    records.push(Record::JobFail {
                        job_id: *job_id,
                        lease_token: *lease_token,
                        error_summary: error_summary.clone(),
                        retry_after_ms: retry_after,
                        terminal,
                        failed_at_ms: batch.now_ms,
                    });
                }
                Op::CancelJob {
                    job_id,
                    lease_token,
                } => {
                    let mut job = job_sim
                        .get(job_id)
                        .or_else(|| self.jobs.get(job_id))
                        .cloned()
                        .ok_or(Error::JobNotFound(*job_id))?;
                    job.state = JobInternalState::Cancelled;
                    job.lease_token = None;
                    job.terminal_at_ms = Some(batch.now_ms);
                    job_sim.insert(*job_id, job);
                    records.push(Record::JobCancel {
                        job_id: *job_id,
                        lease_token: *lease_token,
                        cancelled_at_ms: batch.now_ms,
                    });
                }
                Op::ResolveJob { job_id, resolution } => {
                    let mut job = job_sim
                        .get(job_id)
                        .or_else(|| self.jobs.get(job_id))
                        .cloned()
                        .ok_or(Error::JobNotFound(*job_id))?;
                    match resolution {
                        Resolution::Retry => {
                            job.state = JobInternalState::RetryWait;
                            job.retry_after_ms = batch.now_ms;
                        }
                        Resolution::MarkSucceeded => {
                            job.state = JobInternalState::Succeeded;
                            job.terminal_at_ms = Some(batch.now_ms);
                        }
                        Resolution::MarkDead => {
                            job.state = JobInternalState::Dead;
                            job.terminal_at_ms = Some(batch.now_ms);
                        }
                    }
                    job.lease_token = None;
                    job_sim.insert(*job_id, job);
                    records.push(Record::JobResolve {
                        job_id: *job_id,
                        resolution: Self::resolution_to_record(*resolution),
                        resolved_at_ms: batch.now_ms,
                    });
                }
                Op::LeaseJob {
                    job_id,
                    lease_token,
                    worker_id,
                    attempt,
                    lease_expires_at_ms,
                } => {
                    let mut job = job_sim
                        .get(job_id)
                        .or_else(|| self.jobs.get(job_id))
                        .cloned()
                        .ok_or(Error::JobNotFound(*job_id))?;
                    job.state = JobInternalState::Leased;
                    job.lease_token = Some(*lease_token);
                    job.worker_id = Some(worker_id.clone());
                    job.attempt = *attempt;
                    job.lease_expires_at_ms = *lease_expires_at_ms;
                    job_sim.insert(*job_id, job);
                    records.push(Record::JobLease {
                        job_id: *job_id,
                        lease_token: *lease_token,
                        worker_id: worker_id.clone(),
                        attempt: *attempt,
                        lease_expires_at_ms: *lease_expires_at_ms,
                        claimed_at_ms: batch.now_ms,
                    });
                }
            }
        }
        Ok(records)
    }

    fn resolution_to_record(r: Resolution) -> RecordResolution {
        match r {
            Resolution::Retry => RecordResolution::Retry,
            Resolution::MarkSucceeded => RecordResolution::MarkSucceeded,
            Resolution::MarkDead => RecordResolution::MarkDead,
        }
    }

    fn claim_jobs(&mut self, request: ClaimRequest) -> Result<Vec<ClaimedJob>, Error> {
        if self.poisoned {
            return Err(Error::StorePoisoned {
                transaction_id: Id::ZERO,
            });
        }

        let mut candidates: Vec<(Id, Id, u32, i64, JobSpec)> = Vec::new();
        let mut partitions: Vec<(String, String)> = self
            .queue_partitions
            .keys()
            .filter(|(q, _)| q == &request.queue)
            .cloned()
            .collect();
        partitions.sort_by(|a, b| a.1.cmp(&b.1));

        for (queue, partition) in partitions {
            if candidates.len() >= request.limit {
                break;
            }
            let ids = self
                .queue_partitions
                .get(&(queue.clone(), partition.clone()))
                .cloned()
                .unwrap_or_default();
            for job_id in ids {
                let job = match self.jobs.get(&job_id) {
                    Some(j) => j,
                    None => continue,
                };
                if job.is_terminal() {
                    continue;
                }
                if !job.is_ready_at(request.now_ms) {
                    // Earlier active nonterminal job blocks this partition.
                    break;
                }

                let lease_token = Id::new();
                let attempt = job.attempt + 1;
                let lease_expires_at_ms = request.now_ms + request.lease_ms;
                candidates.push((
                    job_id,
                    lease_token,
                    attempt,
                    lease_expires_at_ms,
                    job.spec.clone(),
                ));
                if candidates.len() >= request.limit {
                    break;
                }
            }
        }

        if candidates.is_empty() {
            return Ok(Vec::new());
        }

        let mut batch = CommitBatch::new(Id::new(), request.now_ms);
        for (job_id, lease_token, attempt, lease_expires_at_ms, _) in &candidates {
            batch = batch.lease_job(
                *job_id,
                *lease_token,
                request.worker_id.clone(),
                *attempt,
                *lease_expires_at_ms,
            );
        }

        self.commit(batch)?;

        let mut claimed = Vec::with_capacity(candidates.len());
        for (job_id, lease_token, attempt, lease_expires_at_ms, spec) in candidates {
            claimed.push(ClaimedJob {
                job_id,
                queue: spec.queue,
                partition: spec.partition,
                payload: spec.payload,
                worker_id: request.worker_id.clone(),
                lease_token,
                attempt,
                lease_expires_at_ms,
                idempotency_key: spec.idempotency_key,
            });
        }
        Ok(claimed)
    }
}

/// Return the smallest byte string strictly greater than every string that starts with `prefix`.
/// Returns `None` when `prefix` is empty or consists entirely of `0xff` bytes, meaning the scan
/// is unbounded above.
fn prefix_upper_bound(prefix: &[u8]) -> Option<Vec<u8>> {
    if prefix.is_empty() {
        return None;
    }
    let mut upper = prefix.to_vec();
    for i in (0..upper.len()).rev() {
        if upper[i] < 0xff {
            upper[i] += 1;
            upper.truncate(i + 1);
            return Some(upper);
        }
    }
    None
}

fn build_receipt(
    records: &[Record],
    transaction_id: Id,
    transaction_sequence: u64,
    frame_offset: u64,
) -> CommitReceipt {
    let mut first_event_sequence: Option<u64> = None;
    let mut last_event_sequence: Option<u64> = None;
    let mut stream_versions: Vec<StreamVersion> = Vec::new();
    let mut job_ids: Vec<Id> = Vec::new();
    let mut final_stream_version: HashMap<String, u64> = HashMap::new();

    for record in records {
        match record {
            Record::Event(e) => {
                first_event_sequence.get_or_insert(e.global_sequence);
                last_event_sequence = Some(e.global_sequence);
                final_stream_version.insert(e.stream_id.clone(), e.stream_version);
            }
            Record::JobEnqueue { job_id, .. } => {
                job_ids.push(*job_id);
            }
            _ => {}
        }
    }

    for (stream_id, version) in final_stream_version {
        stream_versions.push(StreamVersion { stream_id, version });
    }

    CommitReceipt {
        transaction_id,
        transaction_sequence,
        first_event_sequence,
        last_event_sequence,
        stream_versions,
        job_ids,
        frame_offset,
    }
}

fn current_time_ms() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_millis() as i64)
        .unwrap_or(0)
}

/// Summary statistics for an open store.
#[derive(Debug, Clone, PartialEq, Eq)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub struct StoreStats {
    pub file_size: u64,
    pub transaction_count: u64,
    pub event_count: u64,
    pub stream_count: u64,
    pub projection_count: u64,
    pub job_count: u64,
    pub job_counts: HashMap<JobState, usize>,
    pub last_transaction_sequence: u64,
    pub last_event_sequence: u64,
    pub recovered_tail: bool,
    pub poisoned: bool,
    pub format_version_major: u16,
    pub format_version_minor: u16,
}
