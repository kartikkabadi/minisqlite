# Contract-Test Specification — SQLite Control-Plane Kernel

Status: Design specification (Phase 1 deliverable)

This document specifies the backend-independent contract tests required by
Phase 1 ("Define the Semantic Kernel") and the validation tests required by
Phase 7 ("Validation and Test Program") of the control-plane kernel rewrite.
The tests define the observable behavior of the public API
(`ControlPlaneStore`, `CommitBatch`, `ClaimOutcome`, and related types)
independently of any storage implementation. A skeleton of the intended test
suite lives in `tests/contract_tests_plan.rs` (all stubs are `#[ignore]`-ed
and gated out of compilation until the semantic types land).

## Goals and ground rules

- Tests exercise only the public semantic API. No SQLite types, SQL text,
  file offsets, or physical-layout concepts may appear in any test.
- Every test runs against a fresh **file-backed** store (not `:memory:`),
  because WAL, locking, sync, and backup behavior differ.
- Every test asserts **exact typed outcomes** (specific `CommitError`,
  `ClaimError`, `ClaimOutcome`, `TransactionRecovery`, `ClaimRecovery`
  variants), never mere counts or booleans.
- Tests must pass unchanged against any future backend (redb, journal)
  should one ever be added; they are the definition of the product contract.

## Test fixture conventions

- `fresh_store() -> (ControlPlaneStore, TempDir)` — opens a new file-backed
  store in a temp directory.
- `reopen(store) -> ControlPlaneStore` — drops all connections and reopens
  the same file, simulating process restart.
- `batch(...)` helpers build `CommitBatch` values with deterministic IDs,
  timestamps, and canonical operation ordering so digests are reproducible.
- Failpoint hooks (Layer 5) are enabled via a test-only injection API and
  must not exist in the public release surface.

---

## Part A — Phase 1 backend-independent contract tests

### A1. Transaction identity and idempotency

**A1.1 Duplicate transaction ID, identical content**
Commit a `CommitBatch`; commit a byte-identical batch with the same
`transaction_id`. Expect: second commit succeeds and returns the **original
receipt** (same transaction sequence, same `committed_at_ms`). No new
events, projection changes, or jobs are produced; store state is unchanged.

**A1.2 Duplicate transaction ID, different content**
Commit a batch; commit a batch with the same `transaction_id` but any
difference covered by the canonical request digest (timestamp, correlation
ID, metadata, expected stream versions, or ordered operations). Expect:
`CommitError::DuplicateIdWithDifferentContent`. No partial effects.

**A1.3 Digest coverage**
For each digest-covered field, mutate exactly that field and assert the
duplicate-content error, proving the digest covers the full canonical form.

### A2. Event streams

**A2.1 Expected stream-version success**
Append events with `ExpectedStreamVersion` matching the current version
(including version 0 for a new stream). Expect: commit succeeds; stream
version advances by the number of appended events; events are readable in
order via the stream read API.

**A2.2 Expected stream-version conflict**
Append with a stale expected version. Expect: `CommitError::Conflict` with a
typed conflict identifying the stream, expected version, and actual version.
No operation in the batch is applied (atomicity).

**A2.3 Event ID uniqueness**
Commit an event with `event_id` E. Commit a different transaction containing
another event with the same `event_id` E. Expect: typed validation/conflict
error; no partial commit. Also reject duplicate event IDs *within* a single
batch.

### A3. Projections

**A3.1 Patch versioning success**
Apply a `ProjectionPatch` with `expected_version = current`,
`new_version = expected_version + 1`, containing multiple mutations
(put/delete/clear/replace). Expect: one version advance total; all mutations
visible atomically after commit.

**A3.2 Patch version conflict**
Apply a patch with a stale `expected_version`. Expect: typed
`CommitError::Conflict` naming the projection and both versions; no
mutations applied.

**A3.3 Invalid version arithmetic**
`new_version != expected_version + 1` is a `Validation` error.

**A3.4 Duplicate keys within one patch**
Contradictory duplicate mutations for the same key in one patch are
rejected with a typed validation error (the documented deterministic rule),
unless/until last-write-wins is explicitly adopted.

### A4. Jobs — enqueue and identity

**A4.1 Duplicate job IDs**
Enqueue job J; enqueue another job with the same `job_id` in a later
transaction. Expect: typed validation error; the original job is unchanged.
Also reject duplicate job IDs within a single batch.

**A4.2 Effect-mode construction**
`JobSpec::reconcilable(...)`, `JobSpec::idempotent(..., idempotency_key)`,
and `JobSpec::intrinsically_idempotent(...)` construct the documented
effect modes; `idempotent` without an idempotency key does not compile or
returns a typed validation error (per final API shape).

### A5. Leases and acknowledgement

**A5.1 Lease-token validation**
Claim job J obtaining lease token T. Attempt `AckJob`/`FailJob`/
`ExtendLease` with a wrong or stale token. Expect: typed lease/validation
error; job state unchanged. 100% of stale-token attempts must be rejected.

**A5.2 Maximum attempts**
Enqueue with `max_attempts = N`. Fail the job N times (claim → fail →
retry-wait → claim ...). Expect: after the Nth failure the job is `Dead`
with a terminal timestamp and no active lease; it is never claimable again.
`max_attempts = 0` is rejected at validation.

**A5.3 Reconciliation-required lease expiry**
Claim a `RequiresReconciliation` job; let the lease expire (advance the
clock). Expect: the job transitions to `Uncertain` — it is **not** silently
retried and is not claimable; it appears in the uncertain-jobs inspection
API awaiting explicit resolution.

**A5.4 Explicitly idempotent lease expiry**
Claim an `ExplicitlyIdempotent` job; let the lease expire. Expect: the job
returns to `Pending` (or `RetryWait` per backoff policy) with attempt count
incremented, and is claimable again — unless it was at its final attempt,
in which case A5.2 semantics apply.

**A5.5 Stale acknowledgement**
Claim J (token T1); let the lease expire; job is re-claimed (token T2).
Ack with T1. Expect: typed stale-lease error; the T2 lease is unaffected.
Also: ack with T1 after the job reached any terminal state is rejected.

**A5.6 Cancellation**
Cancel a `Pending` job: it becomes `Cancelled` (terminal, timestamped, no
lease) and is never claimable. Cancel a `Leased` job: per contract, the
transition `Leased -> Cancelled` succeeds; a subsequent ack/fail with the
old token returns a typed error. Cancelling a terminal job is a typed
invalid-transition error.

**A5.7 Uncertain resolution**
Drive a job to `Uncertain` (via A5.3). Resolve it each way in separate
tests: `Uncertain -> Succeeded`, `Uncertain -> Dead`, `Uncertain ->
Pending` (retry). Expect: exact target state; resolution is durable across
reopen; resolving a non-uncertain job is a typed invalid-transition error.

**A5.8 Lease extension**
Claim J with expiry E. Extend with the correct token to E' > E before the
grace boundary. Expect: `LeaseExtensionReceipt`; expiry is durably E' after
reopen; attempt count unchanged. Rejected cases (each a typed `LeaseError`):
wrong token, new expiry ≤ current expiry, job not leased, terminal job,
extension after the configurable grace boundary.

### A6. Claim-outcome safety (the P0 contract)

**A6.1 Indeterminate claim non-executability**
When a claim ends indeterminate, the caller receives
`ClaimError::Indeterminate(IndeterminateClaim)` containing **only** a
`transaction_id`. API-shape test (compile-fail or trait-absence assertion):
`IndeterminateClaim` exposes no jobs, payloads, lease tokens, iterators,
`len()`, or `is_empty()`. It must be impossible at the type level to obtain
executable work from an indeterminate claim.

**A6.2 Claim recovery — committed**
Force an indeterminate claim whose underlying transaction actually
committed. Reopen the store; call `recover_claim(transaction_id)`. Expect:
`ClaimRecovery::Committed(CommittedClaims)` containing the **original lease
tokens** and job identities; the effect can then run exactly once under the
recovered lease.

**A6.3 Claim recovery — absent**
Force an indeterminate claim whose transaction never committed. Reopen;
recover. Expect: `ClaimRecovery::Absent`; the jobs remain claimable; no
lease tokens exist for the failed transaction.

**A6.4 Claim recovery — still indeterminate**
While the store/database remains unavailable (e.g. reopen fails or the
file is inaccessible), recovery returns `ClaimRecovery::StillIndeterminate`
rather than guessing. The caller must not be told "absent" until the store
is readable.

**A6.5 Transaction recovery (commit path analog)**
Same trichotomy for `recover_transaction`: `Committed(receipt)` with digest
match, `Absent`, and `StillIndeterminate`. An indeterminate commit result
carries only the transaction ID.

### A7. Scheduling fairness and claim cost

**A7.1 Round-robin partition fairness**
Enqueue one job in each of partitions {a, b, c} on one queue. Claim with
limit 1 repeatedly. Expect: leases granted in strict rotation (a, b, c, a,
...), the cursor advancing only on lease. With limit ≥ partitions, one head
job per partition is leased. Continuous replenishment of partition `a` must
not starve `b` or `c` (test: re-enqueue into `a` after each ack; assert `b`
and `c` are still served in rotation).

**A7.2 Active-partition-only claim cost**
Create H historical partitions whose jobs are all terminal and A active
partitions. Expect functionally: a claim never selects a historical
partition, and a partition leaves active scheduling when its last
nonterminal job reaches a terminal state, re-entering when new work
arrives. Expect operationally (benchmark-tier assertion, Phase 6): claim
latency scales with A, not with A + H (e.g. 100 active / 100k historical is
not materially slower than 100 active / 100 historical).

**A7.3 Out-of-order completion does not rewind the cursor**
Lease partitions in order A → B → C, then complete out of order (ack C,
then ack A). Expect: the round-robin cursor still reflects C as the last
*leased* partition; the next claim serves the partition after C. Repeat the
assertion after reopen. Ack, fail, cancel, resolve, and expiry must never
move the cursor.

**A7.4 Maintenance-only claims produce `MaintenanceCommitted`**
Arrange an expired final-attempt head job plus a ready second job, with a
transaction budget that fits only the expiry maintenance record. Expect:
`ClaimOutcome::MaintenanceCommitted(receipt)` — never
`Committed(CommittedClaims { jobs: [] })` and never `Noop`. A standard
drain loop that polls again on `MaintenanceCommitted` then obtains the
second job. `Noop` is returned only when there is truly nothing to lease
and no maintenance was performed.

---

## Part B — Phase 7 validation tests

### B1. Backup and restore

**B1.1 Backup during writes**
Start the online backup while a writer continuously commits events,
projection patches, and job operations. Expect: backup completes; the
writer observes no error and no outage beyond the documented budget; the
backup file passes integrity verification.

**B1.2 Restore equivalence**
Restore the backup into a new location; open it. Expect: semantic
equivalence with a consistent point-in-time of the source — transaction
receipts, stream versions, events, projection entries/versions, job states,
queue cursors, and active partitions all pass the semantic verification
suite; every transaction present is complete (no partial batches).

**B1.3 Destination handling**
Backup to an existing path without an explicit overwrite flag is a typed
error and leaves the existing file untouched.

### B2. Process-crash / reopen scenarios

Each drill uses a child process killed at a controlled point (failpoints:
before transaction, after BEGIN, after writes before COMMIT, during COMMIT,
after COMMIT before return, after receipt).

**B2.1 Crash before claim commit**
Kill before the claim transaction commits. Reopen. Expect: no lease exists,
no claim receipt exists, the job is still `Pending`/claimable, and no
external effect ran.

**B2.2 Crash after claim commit, before response (the critical test)**
SQLite commit succeeds; the process/connection dies before the caller
receives confirmation. Expect: caller (or supervisor) treats the claim as
indeterminate; **no effect runs**; after reopen, `recover_claim` returns
`Committed` with the original lease token; the effect then runs exactly
once under the recovered lease.

**B2.3 Crash with WAL present**
Kill mid-write leaving WAL activity. Reopen. Expect: open succeeds without
event replay proportional to history; committed transactions are all
present; uncommitted ones are all absent; integrity check passes.

**B2.4 Worker death / heartbeat stop**
Kill a worker holding a lease; stop heartbeats. Expect: on expiry the job
follows its effect mode (A5.3/A5.4); another worker cannot act with the
dead worker's token (A5.5).

**B2.5 Reopen cost**
Reopening a store with a large event history is not proportional to event
count and does not load historical payloads into process memory (asserted
at benchmark tier; functionally: reads work immediately after open).

### B3. Concurrency races (barrier-synchronized)

Assert exact typed outcomes for: two commits expecting the same stream
version (exactly one `Conflict`); two workers claiming the same queue (no
job double-leased); ack vs. lease expiry; heartbeat extension vs. expiry;
cancellation vs. ack; uncertain resolution vs. retry; backup vs. writes.

---

## Traceability matrix

| Scenario | Spec section | Test stub |
|---|---|---|
| Duplicate txn ID, identical content | A1.1 | `duplicate_transaction_id_identical_content_returns_original_receipt` |
| Duplicate txn ID, different content | A1.2 | `duplicate_transaction_id_different_content_is_typed_error` |
| Expected stream-version success | A2.1 | `expected_stream_version_success` |
| Expected stream-version conflict | A2.2 | `expected_stream_version_conflict` |
| Event ID uniqueness | A2.3 | `event_id_uniqueness_enforced` |
| Projection patch versioning | A3.1/A3.3 | `projection_patch_advances_one_version` |
| Projection patch conflict | A3.2 | `projection_patch_version_conflict` |
| Duplicate job IDs | A4.1 | `duplicate_job_id_rejected` |
| Lease-token validation | A5.1 | `stale_or_wrong_lease_token_rejected` |
| Maximum attempts | A5.2 | `max_attempts_exhaustion_leads_to_dead` |
| Reconciliation-required expiry | A5.3 | `reconciliation_required_lease_expiry_becomes_uncertain` |
| Explicitly idempotent expiry | A5.4 | `explicitly_idempotent_lease_expiry_requeues` |
| Stale acknowledgement | A5.5 | `stale_acknowledgement_rejected` |
| Cancellation | A5.6 | `cancellation_transitions` |
| Uncertain resolution | A5.7 | `uncertain_resolution_paths` |
| Lease extension | A5.8 | `lease_extension_rules` |
| Indeterminate claim non-executability | A6.1 | `indeterminate_claim_exposes_no_executable_work` |
| Claim recovery: committed | A6.2 | `claim_recovery_committed_returns_original_tokens` |
| Claim recovery: absent | A6.3 | `claim_recovery_absent_leaves_jobs_claimable` |
| Claim recovery: still indeterminate | A6.4 | `claim_recovery_still_indeterminate_when_store_unavailable` |
| Round-robin fairness | A7.1 | `round_robin_partition_fairness` |
| Active-partition claim cost | A7.2 | `claim_cost_independent_of_historical_partitions` |
| Out-of-order completion / cursor | A7.3 | `out_of_order_completion_does_not_rewind_cursor` |
| Maintenance-only claims | A7.4 | `maintenance_only_claim_is_maintenance_committed` |
| Backup during writes | B1.1 | `backup_during_writes_succeeds` |
| Restore equivalence | B1.2 | `restore_is_semantically_equivalent` |
| Crash/reopen scenarios | B2.* | `crash_*` stubs |

## Release-blocking assertions (from the plan)

The release cannot proceed unless: zero test path executes work from an
indeterminate claim; every job transition is model-tested; claim recovery
works for committed and absent outcomes; stream conflicts are
deterministic; duplicate transaction IDs are content-checked; live backup
restores successfully; active scheduling cost is independent of historical
inactive partitions.
