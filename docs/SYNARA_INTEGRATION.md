# Synara Vertical-Slice Integration Design

Status: Design specification (Phase 5 of the control-plane kernel rewrite plan)

This document specifies how Synara dogfoods the SQLite-backed control-plane
kernel through **one provider-turn vertical slice**. It does not describe a
full Synara persistence migration. Scope is deliberately limited: prove that
the semantic model (atomic events + projections + durable jobs + honest
uncertainty) reduces Synara orchestration complexity before expanding.

Companion skeleton: [`examples/synara_control_plane.rs`](../examples/synara_control_plane.rs).

---

## 1. The provider-turn vertical slice

One complete workflow, end to end:

```text
1. thread created                  -> event appended to thread:<id>
2. turn requested                  -> event appended
3. thread projection -> "queued"   -> projection patch in same transaction
4. provider job enqueued           -> job in same atomic transaction (outbox)
5. job claimed by worker           -> lease token issued, claim receipt persisted
6. provider effect executed        -> external call with idempotency key where supported
7. completion event appended       -> thread.turn-completed
8. thread projection -> "idle"     -> projection patch in same transaction
9. job acknowledged                -> job terminal (Succeeded), same transaction as 7-8
```

Steps 2–4 are **one `CommitBatch`**. Steps 7–9 are **one `CommitBatch`**.
This is the transactional-outbox guarantee: application state can never
say "queued" without durable work existing, and can never say "idle"
without the completion event and acknowledgement being recorded together.

### Kernel API mapping

| Slice step | Kernel operation |
| --- | --- |
| 1 | `CommitBatch { AppendEvent(thread.created), ApplyProjectionPatch(threads) }` |
| 2–4 | `CommitBatch { AppendEvent(thread.turn-requested), ApplyProjectionPatch(threads: queued), EnqueueJob(provider-command) }` |
| 5 | `claim_jobs(queue: "provider-command", ...)` -> `ClaimOutcome::Committed(CommittedClaims)` |
| 6 | Worker-side external call (outside the kernel), heartbeat via `extend_lease` |
| 7–9 | `CommitBatch { AppendEvent(thread.turn-completed), ApplyProjectionPatch(threads: idle), AckJob }` |

---

## 2. Domain mapping

### Streams

| Stream | Contents |
| --- | --- |
| `thread:<thread_id>` | Thread lifecycle and turn events |
| `loop:<loop_id>` | Loop scheduling/execution events |
| `provider-run:<run_id>` | Per-provider-invocation events (request, response, failure) |

Expected stream versions guard against concurrent turn requests on the same
thread: the turn-request `CommitBatch` carries
`ExpectedStreamVersion { stream: "thread:<id>", version: N }`, so a racing
second request fails with a typed `Conflict` rather than double-enqueueing.

### Event types

```text
thread.created
thread.turn-requested
thread.turn-started
thread.turn-completed
thread.turn-failed
thread.turn-uncertain
loop.scheduled
loop.started
loop.completed
```

### Projections

| Projection | Key | Value (serialized) |
| --- | --- | --- |
| `threads` | `thread_id` | `{ status: idle \| queued \| running \| uncertain, current_turn, updated_at }` |
| `provider_runs` | `run_id` | `{ thread_id, state, attempt, provider, started_at, finished_at }` |
| `loops` | `loop_id` | `{ state, next_run_at, last_result }` |
| `queue_status` | `queue name` | `{ ready, leased, retry_wait, uncertain, dead }` counters for operator UI |

Each logical thread-state change is **one `ProjectionPatch`** advancing the
projection version by exactly one, regardless of how many keys it mutates.

### Queues and partition keys

| Queue | Partition key | Purpose |
| --- | --- | --- |
| `provider-command` | `thread:<thread_id>` | Provider turn execution; per-thread FIFO ordering |
| `loop-run` | `loop:<loop_id>` | Scheduled loop iterations |
| `maintenance` | fixed (`"global"`) | Compaction hints, retention, reconciliation sweeps |

Partitioning by thread ID gives the required invariant: turns within one
thread execute in order, while independent threads proceed in parallel via
round-robin claiming over **active** partitions only (completed threads drop
out of the scheduler entirely).

### Effect modes

Provider calls default to `JobSpec::reconcilable(..)`
(`EffectMode::RequiresReconciliation`). A provider that supports request
idempotency keys uses `JobSpec::idempotent(.., idempotency_key)` where the
key is derived from `(thread_id, turn_id, attempt)` — never reused across
attempts unless the provider guarantees replay-safe semantics.

---

## 3. Worker protocol

```text
1. claim_jobs("provider-command", worker_id, limit, now)
2. match outcome:
     Committed(claims)      -> proceed
     MaintenanceCommitted   -> poll again immediately (durable progress was made;
                               this is NOT an empty queue)
     Noop                   -> back off
     Err(Indeterminate{tx}) -> DO NOT execute anything; reopen/reset connection,
                               recover_claim(tx), then act on Committed/Absent
3. start heartbeat: extend_lease(job_id, lease_token, new_expiry, now) on interval
4. append thread.turn-started + threads projection -> "running" (one CommitBatch)
5. perform provider effect (idempotency key attached where supported)
6. stop heartbeat
7. one CommitBatch: thread.turn-completed + threads -> "idle" + AckJob(lease_token)
8. on ambiguous provider result (timeout, connection dropped mid-response):
     do NOT retry automatically; record thread.turn-uncertain and let the job
     become Uncertain for explicit operator/automatic reconciliation
```

An `IndeterminateClaim` carries only a `transaction_id` — no jobs, payloads,
or lease tokens — so executing unconfirmed work is impossible at the type
level.

---

## 4. Failure drills

Each drill is executed deliberately as part of the integration exit gate.

| # | Drill | Expected behavior |
| --- | --- | --- |
| 1 | Crash before claim commit | No external work begins; job remains ready; next worker claims it normally. |
| 2 | Claim commits but worker never receives the response (indeterminate claim) | Worker receives `ClaimError::Indeterminate { transaction_id }` with no executable data; no effect runs; after reopen, `recover_claim` returns `Committed(CommittedClaims)` with the original lease tokens (from `claim_receipts`), or `Absent`. Effect runs exactly once under the recovered lease. |
| 3 | Provider timeout / ambiguous response mid-request | No silent retry. Reconciliation-required job transitions to `Uncertain` on lease expiry; `thread.turn-uncertain` recorded; operator resolves via `ResolveUncertainJob`. |
| 4 | Provider succeeds, process crashes before acknowledgement | Idempotent jobs: reconciled via provider idempotency key, resolved to `Succeeded`. Non-idempotent jobs: become `Uncertain`; never silently re-executed. |
| 5 | Worker runs longer than the original lease | Heartbeat extends the lease (`extend_lease`, token-checked, durable); no second worker can claim the job. |
| 6 | Heartbeat stops (worker death) | Lease expires; expiry behavior follows effect mode — reconciliation-required jobs go `Uncertain`, explicitly idempotent jobs return to `Pending`/`RetryWait` for retry. |
| 7 | Database process restart | Open time does not scale with event history (no replay); jobs, projections, and uncertain work are immediately queryable via SQLite indexes. |
| 8 | Operator reconciliation | Uncertain jobs are visible via `uncertain_jobs_page`/operator view; operator checks provider-side state and calls `ResolveUncertainJob` to `Pending` (retry), `Succeeded`, or `Dead`. Resolution is recorded as a normal transaction. |

Release-blocking assertion across the whole matrix: **zero duplicate
non-idempotent provider effects, and zero effects executed from an
indeterminate claim.**

---

## 5. Shadow mode and rollback

### Shadow mode

For a limited validation period the kernel path runs alongside the existing
Synara orchestration path:

- Writes go through the new transaction model (events + projections + jobs).
- Reads continue to be served by the existing Synara path.
- Derived state (thread status, run state) is compared asynchronously; any
  mismatch is logged and reported — mismatches are bugs, not noise.
- **Exactly one effect executor exists.** Shadow mode must never create two
  provider-call paths; the shadow side records intent only. Double execution
  is a hard failure of the shadow design, not a tolerable artifact.

### Rollback switch

The integration lives behind a Synara feature flag
(e.g. `SYNARA_CONTROL_PLANE_KERNEL=on|off|shadow`):

- `off` — previous orchestration path only (default until exit criteria pass).
- `shadow` — old path authoritative; kernel written and compared.
- `on` — kernel authoritative for the provider-turn slice; old path disabled
  for this slice but retained in the codebase until the exit gate passes.

Flipping to `off` must be safe at any moment: the old path never depends on
kernel state, and the kernel store can be discarded or retained for
diagnosis without affecting the old path.

---

## 6. Exit criteria (Phase 5)

The integration is complete only when all of the following hold:

1. One provider-turn workflow runs end to end through the kernel.
2. No duplicate provider effect occurs anywhere in the failure-drill matrix.
3. Uncertain outcomes are visible to the operator (uncertainty view) and at
   least one uncertain effect has been reconciled through the operator path.
4. The old orchestration path can be disabled for the vertical slice via the
   rollback switch, and re-enabled safely.
5. Synara code complexity for the migrated slice is measured before and
   after (LOC, failure states, recovery paths) and is materially smaller or
   clearer.
6. The team can explain every failure state in the drill matrix without
   reading SQLite internals.

Non-goals for this phase: migrating any other Synara persistence, loop
scheduling beyond stream/queue naming, multi-process writers, and any public
backend abstraction.
