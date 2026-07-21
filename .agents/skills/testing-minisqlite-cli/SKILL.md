---
name: testing-minisqlite-cli
description: How to build, seed, and end-to-end test the minisqlite control-plane CLI, including creating Uncertain jobs and verifying exit codes.
---

# Testing the minisqlite CLI

## Build & basics
- `cargo build` produces `target/debug/minisqlite`. `--help` documents all commands.
- Exit-code classes: 0 success, 1 operational, 2 usage, 3 verify findings, 4 not found.
- The CLI never creates DBs (except `jobs resolve` which opens read-write on an existing file). You must seed a store via the library first.

## Seeding a store
Write a small `examples/seed.rs` (temporary, delete afterwards) using the public API and run `cargo run --example seed -- /tmp/cpk.db`:
- Events: `CommitBatch::new(id, ts).append_event(Event::with_json_payload(...))`.
- Projections: `apply_projection_patch(ProjectionPatch::new("p", 0).put(key, value))`.
- Pending job: `.enqueue_job(JobSpec::reconcilable(id, "q", "pk", payload))`.
- **Uncertain job**: enqueue reconcilable → `claim_jobs` at t (lease_ms=10_000) → `claim_jobs` again at t+20s; maintenance moves it to Uncertain (returns `ClaimOutcome::MaintenanceCommitted`). Pattern in `tests/jobs_transitions.rs` (`make_uncertain`).
- Job with `error_summary`: claim then `.fail_job(id, lease_token, "boom", None)` → RetryWait.

## Key assertions
- `doctor` prints `schema version: 2` and `verify: ok` on a healthy store.
- Default `diagnostic-export` contains only `error_summary_len` (length metadata), never the raw `error_summary` text; `--include-payloads` includes `"error_summary":"boom"` and header `payloads_included:true`.
- `jobs resolve <id> retry` moves Uncertain → Pending; `jobs uncertain` becomes empty.
- `backup <dest>` refuses to overwrite without `--overwrite` (exit 1).
- Missing db/stream/job/projection key → exit 4; unknown command/flag/state → exit 2. A typo'd `--db` path must NOT create a file.

## Notes
- Terminal-only project: do not record; capture transcripts (`cmd; echo exit=$?`) instead.
- Exercising exit 3 requires a corrupted store (verify findings) — usually skip.
