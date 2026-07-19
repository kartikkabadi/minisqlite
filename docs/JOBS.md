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

## Claiming

```rust
let request = ClaimRequest {
    queue: "provider".into(),
    worker_id: "worker-1".into(),
    now_ms,
    lease_ms: 60_000,
    limit: 1,
};
let jobs = store.claim_jobs(request)?;
```

* Claims are ordered by `(queue, partition)` and then by insertion order.
* Only one ready job per partition is claimed per request, up to `limit` total.
* A new lease token is generated for every claim.

## Completion

* `ack_job(job_id, lease_token, result_digest, now_ms)` — success.
* `fail_job(job_id, lease_token, error_summary, retry_after_ms, now_ms)` — retry or dead after `max_attempts`.
* `cancel_job(job_id, lease_token, now_ms)` — explicit cancellation.

A stale lease token is rejected.

## Uncertain outcomes

When `EffectMode::UncertainOnLeaseExpiry` is used and a lease expires:

* The job becomes `Uncertain`.
* It is not silently retried.
* The operator or application resolves it explicitly:

```rust
store.resolve_uncertain_job(job_id, Resolution::Retry, now_ms)?;
// or Resolution::MarkSucceeded, Resolution::MarkDead
```

This matches the reality that some external effects cannot be safely repeated without human or system confirmation.
