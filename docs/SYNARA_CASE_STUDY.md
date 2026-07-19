# Synara-Shaped Case Study

This document explains how `examples/synara_control_plane.rs` proves the engine against a real local-first AI control-plane workload.

## Flow A: Create a thread

A single `CommitBatch` appends `thread.created` and inserts the projected thread record.
On success the application receives the global event sequence and stream version.

## Flow B: Request a provider turn

The application requires the expected stream version, appends `thread.turn-requested`, updates the projected state to `queued`, and enqueues one provider job partitioned by thread ID.

Because all three happen in one batch, it is impossible for the event to exist without the projection or the job.

## Flow C: Claim and complete provider work

A worker claims the next provider job and receives a lease token.
It performs the work, then commits `thread.turn-completed`, sets the projection to `idle`, and acknowledges the job atomically.

A stale lease token cannot acknowledge the job, preventing accidental duplicate completion.

## Flow D: Crash after external effect

* **Idempotent effect:** after lease expiry the job can be safely reclaimed and retried.
* **Non-idempotent effect:** the job becomes `Uncertain` and is not silently retried. The application or operator explicitly resolves it.

## Flow E: Durable loop scheduling

A loop iteration is an event; the next iteration is a future job with `not_before_ms`.
After a process restart, the future job is still present and becomes claimable when `not_before_ms` passes.

## Flow F: Projection replacement

The application reads the event history for a stream and rebuilds the `threads` projection from those events.
The rebuilt projection is atomically replaced, without a SQL migration.

## Why this matters

The same patterns appear in local-first AI control planes: events that drive state, projections that answer queries, and jobs that coordinate long-running side effects with honest uncertainty.
