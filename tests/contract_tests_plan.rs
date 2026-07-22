//! Contract-test plan skeleton for the SQLite control-plane kernel.
//!
//! Companion to `docs/CONTRACT_TESTS.md`. Each `#[ignore]`-ed stub names one
//! contract scenario and sketches the intended public-API usage
//! (`ControlPlaneStore`, `CommitBatch`, `ClaimOutcome`, ...). The semantic
//! types do not exist yet, so the entire module is compiled out with
//! `cfg(any())`; remove that gate once the Phase 1 API lands.
#![cfg(any())]

use minisqlite::{
    ClaimError, ClaimOutcome, ClaimRecovery, ClaimRequest, CommitBatch, CommitError,
    ControlPlaneStore, Event, ExpectedStreamVersion, JobSpec, LeaseError, Operation,
    ProjectionMutation, ProjectionPatch, TransactionRecovery,
};

/// Fresh file-backed store in a temp dir. Never `:memory:` — WAL, locking,
/// sync, and backup behavior differ.
fn fresh_store() -> (ControlPlaneStore, tempfile::TempDir) {
    unimplemented!()
}

/// Drop all connections and reopen the same file (simulated restart).
fn reopen(store: ControlPlaneStore) -> ControlPlaneStore {
    unimplemented!()
}

// ---------------------------------------------------------------------------
// A1. Transaction identity and idempotency
// ---------------------------------------------------------------------------

#[test]
#[ignore = "spec A1.1 — awaiting Phase 1 semantic API"]
fn duplicate_transaction_id_identical_content_returns_original_receipt() {
    let (store, _dir) = fresh_store();
    let batch: CommitBatch = unimplemented!("deterministic batch with txn id T");
    let first = store.commit(batch.clone()).unwrap();
    let second = store.commit(batch).unwrap();
    assert_eq!(first.transaction_sequence, second.transaction_sequence);
    // Store state unchanged: no new events, projections, or jobs.
}

#[test]
#[ignore = "spec A1.2 — awaiting Phase 1 semantic API"]
fn duplicate_transaction_id_different_content_is_typed_error() {
    let (store, _dir) = fresh_store();
    store.commit(unimplemented!("batch with txn id T")).unwrap();
    let err = store
        .commit(unimplemented!("txn id T, mutated metadata"))
        .unwrap_err();
    assert!(matches!(err, CommitError::DuplicateIdWithDifferentContent));
}

// ---------------------------------------------------------------------------
// A2. Event streams
// ---------------------------------------------------------------------------

#[test]
#[ignore = "spec A2.1"]
fn expected_stream_version_success() {
    // Append at ExpectedStreamVersion(0) for a new stream; version advances
    // by the number of appended events; stream read returns them in order.
}

#[test]
#[ignore = "spec A2.2"]
fn expected_stream_version_conflict() {
    let (store, _dir) = fresh_store();
    // Two batches expecting the same stream version: second returns
    // CommitError::Conflict identifying stream, expected, and actual
    // versions; no operation of the losing batch is applied.
    let err: CommitError = unimplemented!();
    assert!(matches!(err, CommitError::Conflict(_)));
}

#[test]
#[ignore = "spec A2.3"]
fn event_id_uniqueness_enforced() {
    // Same event_id in a second transaction (and within one batch) is a
    // typed error with no partial commit.
}

// ---------------------------------------------------------------------------
// A3. Projections
// ---------------------------------------------------------------------------

#[test]
#[ignore = "spec A3.1/A3.3"]
fn projection_patch_advances_one_version() {
    // One ProjectionPatch { expected_version: v, new_version: v + 1 } with
    // several mutations advances exactly one version, atomically.
    // new_version != expected_version + 1 => CommitError::Validation.
}

#[test]
#[ignore = "spec A3.2"]
fn projection_patch_version_conflict() {
    // Stale expected_version => CommitError::Conflict naming the projection
    // and both versions; no mutations applied.
}

// ---------------------------------------------------------------------------
// A4/A5. Jobs, leases, acknowledgement
// ---------------------------------------------------------------------------

#[test]
#[ignore = "spec A4.1"]
fn duplicate_job_id_rejected() {
    // EnqueueJob with an existing job_id (across and within batches) is a
    // typed validation error; the original job is unchanged.
}

#[test]
#[ignore = "spec A5.1"]
fn stale_or_wrong_lease_token_rejected() {
    // AckJob / FailJob / ExtendLease with a wrong or stale token returns a
    // typed lease error; job state unchanged.
}

#[test]
#[ignore = "spec A5.2"]
fn max_attempts_exhaustion_leads_to_dead() {
    // Fail a max_attempts = N job N times; it is Dead, terminal-timestamped,
    // lease-free, and never claimable again. max_attempts = 0 is rejected.
}

#[test]
#[ignore = "spec A5.3"]
fn reconciliation_required_lease_expiry_becomes_uncertain() {
    // Claim a JobSpec::reconcilable job; expire the lease; the job is
    // Uncertain (not silently retried, not claimable) and visible in the
    // uncertain-jobs inspection API.
}

#[test]
#[ignore = "spec A5.4"]
fn explicitly_idempotent_lease_expiry_requeues() {
    // Claim a JobSpec::idempotent(.., key) job; expire the lease; the job is
    // claimable again with attempt incremented (or Dead at final attempt).
}

#[test]
#[ignore = "spec A5.5"]
fn stale_acknowledgement_rejected() {
    // Ack with token T1 after expiry + re-claim under T2 => typed stale
    // error; the T2 lease is unaffected. Ack after terminal state rejected.
}

#[test]
#[ignore = "spec A5.6"]
fn cancellation_transitions() {
    // Pending -> Cancelled and Leased -> Cancelled succeed; cancelling a
    // terminal job is a typed invalid-transition error; old tokens dead.
}

#[test]
#[ignore = "spec A5.7"]
fn uncertain_resolution_paths() {
    // Uncertain -> Succeeded / Dead / Pending each reach the exact target
    // state and survive reopen; resolving non-uncertain jobs is rejected.
}

#[test]
#[ignore = "spec A5.8"]
fn lease_extension_rules() {
    // extend_lease(job, token, new_expiry, now): success requires matching
    // token, currently-leased job, new_expiry > current, within grace
    // boundary; durable across reopen; attempt count unchanged. Each
    // violation returns a typed LeaseError.
}

// ---------------------------------------------------------------------------
// A6. Claim-outcome safety
// ---------------------------------------------------------------------------

#[test]
#[ignore = "spec A6.1"]
fn indeterminate_claim_exposes_no_executable_work() {
    let (store, _dir) = fresh_store();
    let err: ClaimError = unimplemented!("failpoint-forced indeterminate claim");
    let ClaimError::Indeterminate(indeterminate) = err else {
        panic!("expected indeterminate");
    };
    // IndeterminateClaim carries ONLY a transaction_id: no jobs, payloads,
    // lease tokens, iterators, len(), or is_empty(). Pair with a
    // compile-fail (trybuild) test asserting non-iterability.
    let _txn = indeterminate.transaction_id;
}

#[test]
#[ignore = "spec A6.2"]
fn claim_recovery_committed_returns_original_tokens() {
    // Indeterminate claim whose transaction committed: after reopen,
    // recover_claim(txn) == ClaimRecovery::Committed(claims) with the
    // ORIGINAL lease tokens; the effect runs exactly once under them.
}

#[test]
#[ignore = "spec A6.3"]
fn claim_recovery_absent_leaves_jobs_claimable() {
    // Transaction never committed: recover_claim == ClaimRecovery::Absent;
    // jobs remain claimable; no lease tokens exist.
}

#[test]
#[ignore = "spec A6.4"]
fn claim_recovery_still_indeterminate_when_store_unavailable() {
    // While the database is unreadable, recovery reports
    // ClaimRecovery::StillIndeterminate rather than guessing Absent.
}

#[test]
#[ignore = "spec A6.5"]
fn transaction_recovery_trichotomy() {
    // recover_transaction: Committed(receipt) with digest match / Absent /
    // StillIndeterminate.
    let _ = TransactionRecovery::Absent;
}

// ---------------------------------------------------------------------------
// A7. Scheduling fairness and claim cost
// ---------------------------------------------------------------------------

#[test]
#[ignore = "spec A7.1"]
fn round_robin_partition_fairness() {
    // Partitions {a, b, c}: claims with limit 1 lease in strict rotation;
    // continuous replenishment of `a` cannot starve `b` or `c`.
}

#[test]
#[ignore = "spec A7.2"]
fn claim_cost_independent_of_historical_partitions() {
    // With H all-terminal historical partitions and A active ones, claims
    // never select historical partitions; a partition leaves active
    // scheduling at its last terminal transition and re-enters on new work.
    // Latency scaling with A (not A + H) is asserted at benchmark tier.
}

#[test]
#[ignore = "spec A7.3"]
fn out_of_order_completion_does_not_rewind_cursor() {
    // Lease A -> B -> C, ack C then A: the cursor still reflects C as last
    // leased; next claim serves the partition after C, before and after
    // reopen. Only JobLease moves the cursor.
}

#[test]
#[ignore = "spec A7.4"]
fn maintenance_only_claim_is_maintenance_committed() {
    let (store, _dir) = fresh_store();
    // Expired final-attempt head + ready second job + budget fitting only
    // the expiry record:
    let outcome: ClaimOutcome = unimplemented!();
    assert!(matches!(outcome, ClaimOutcome::MaintenanceCommitted(_)));
    // Never Committed with empty jobs, never Noop; a drain loop polling
    // again on MaintenanceCommitted then obtains the second job.
}

// ---------------------------------------------------------------------------
// B1. Backup and restore
// ---------------------------------------------------------------------------

#[test]
#[ignore = "spec B1.1"]
fn backup_during_writes_succeeds() {
    // Online backup while a writer continuously commits: backup completes,
    // writer unaffected beyond budget, backup passes integrity checks.
}

#[test]
#[ignore = "spec B1.2"]
fn restore_is_semantically_equivalent() {
    // Restored backup passes semantic verification and represents a
    // consistent point-in-time: no partial transactions.
}

// ---------------------------------------------------------------------------
// B2. Process-crash / reopen scenarios (child-process harness)
// ---------------------------------------------------------------------------

#[test]
#[ignore = "spec B2.1"]
fn crash_before_claim_commit_leaves_job_pending() {
    // Kill before commit: no lease, no claim receipt, job Pending, no
    // effect ran.
}

#[test]
#[ignore = "spec B2.2"]
fn crash_after_commit_before_response_recovers_exactly_once() {
    // THE critical test: commit succeeds, process dies before confirmation;
    // no effect runs on the indeterminate result; reopen + recover_claim
    // yields Committed with the original token; effect runs exactly once.
}

#[test]
#[ignore = "spec B2.3"]
fn crash_with_wal_reopens_without_replay() {
    // Reopen after a mid-write kill: committed transactions all present,
    // uncommitted all absent, integrity check passes, open cost not
    // proportional to history.
}

#[test]
#[ignore = "spec B2.4"]
fn worker_death_follows_effect_mode_on_expiry() {
    // Dead worker's lease expires per effect mode; its stale token is
    // rejected everywhere.
}
