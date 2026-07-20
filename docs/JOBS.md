# Durable Jobs

## Job lifecycle

```text
Pending -> Leased -> Succeeded
   |         |
   |         +-> RetryWait -> Pending
   |         |
   |         +-> Dead
   |         |
   |         +-> Cancelled
   |
   +-> (lease expires)
        Idempotent effect -> can be reclaimed as Leased
        Non-idempotent effect -> Uncertain -> resolved explicitly
        Idempotent effect at max_attempts -> Dead (via internal `JobExpire` maintenance)
```

## Enqueuing

```rust
let job = JobSpec::new(id, "provider", partition, payload)
    .with_max_attempts(3)
    .with_effect_mode(EffectMode::Idempotent);
store.commit(CommitBatch::new(tx, now_ms).enqueue_job(job))?;
```

`not_before_ms` schedules future work.
`idempotency_key` is an application-level key for the external effect.

The default `EffectMode` is `UncertainOnLeaseExpiry`: an expired lease is never silently
reclaimed. Reclaim-on-expiry must be opted into explicitly with `EffectMode::Idempotent`.

## Claiming

```rust
let request = ClaimRequest {
    queue: "provider".into(),
    worker_id: "worker-1".into(),
    now_ms,
    lease_ms: 60_000,
    limit: 1,
};
let outcome = store.claim_jobs(request)?;
for job in outcome.claims() {
    println!("claimed {} partition={} token={}", job.job_id, job.partition, job.lease_token);
}
```

* `claim_jobs` returns `ClaimOutcome::Noop` when the queue is empty or no job is ready; no durable transaction is written and no transaction ID is allocated. Otherwise it returns `ClaimOutcome::Committed { transaction_id, claims }` on a durable commit, or `ClaimOutcome::Uncertain { transaction_id, claims }` when the frame was written but the in-memory apply could not be confirmed (the caller can reopen and use the returned lease tokens if the frame is present). Use `outcome.claims()` and `outcome.transaction_id()` to inspect the result.
* Partitions within a queue are served round-robin: a durable per-queue cursor records the last partition served, and each request starts from the next partition in lexicographic rotation. Per-partition head indexes skip terminal jobs. Both structures are rebuilt from the journal on replay, so fairness is stable across reopen.
* Within a partition, jobs are claimed in insertion order.
* Only one ready job per partition is claimed per request, up to `limit` total.
* A new lease token is generated for every claim.
* Round-robin rotation means repeated `limit=1` callers cycle across partitions instead of starving later partitions when earlier ones are continuously replenished.
* Expired final-attempt jobs are maintained with a fixed-size `JobExpire` record that is independent of `max_summary_len` and `max_frame_size`.
* `claim_jobs` builds one atomic `CommitBatch` containing all maintenance and candidate lease ops; if the configured `max_records_per_transaction` or `max_frame_size` does not fit everything, it commits a safe bounded prefix and makes progress without leaving a partial durable state.

## Completion

* `ack_job(job_id, lease_token, result_digest, now_ms)` — success.
* `fail_job(job_id, lease_token, error_summary, retry_after_ms, now_ms)` — retry or dead after `max_attempts`.
* `cancel_job(job_id, lease_token, now_ms)` — explicit cancellation.

`Store::jobs(now_ms, queue, state)` returns a `JobInfo` snapshot for each job, including `attempt`, `lease_expires_at_ms`, `worker_id`, `retry_after_ms`, `terminal_at_ms`, and `lease_token` (when the job is currently leased) so callers can render queues without extra lookups.

A stale lease token is rejected.

## Uncertain outcomes

When `EffectMode::UncertainOnLeaseExpiry` (the default) is used and a lease expires:

* The job becomes `Uncertain`.
* It is not silently retried.
* The operator or application resolves it explicitly:

```rust
store.resolve_uncertain_job(job_id, Resolution::Retry, now_ms)?;
// or Resolution::MarkSucceeded, Resolution::MarkDead
```

This matches the reality that some external effects cannot be safely repeated without human or system confirmation.

## Timestamps

Job `not_before_ms` and lease-expiry timestamps use the caller-supplied wall-clock `now_ms`.
Large clock jumps can make a job claimable earlier or later than intended. The engine does not
implement a distributed clock; it trusts the `now_ms` value it receives.
