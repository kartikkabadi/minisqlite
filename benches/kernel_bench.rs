//! Benchmark skeleton for the control-plane kernel.
//!
//! See `docs/BENCHMARKS.md` for the full specification: required environment
//! fields, workload definitions (T1-T8, scale, O1-O9), performance budgets,
//! and regression policy.
//!
//! These are design-stage stubs: the SQLite-backed `ControlPlaneStore` does
//! not exist yet, so every stub is `#[ignore]`-ed and contains no working
//! benchmark logic. Once the kernel lands, this file moves to a
//! `criterion`-based harness (`harness = false` in Cargo.toml) with these
//! stubs becoming real benchmark groups.

// ---------------------------------------------------------------------------
// Transaction workloads (docs/BENCHMARKS.md §2.1)
// ---------------------------------------------------------------------------

/// T1: CommitBatch appending one event to one stream.
#[test]
#[ignore = "benchmark stub: kernel not implemented yet"]
fn bench_t1_commit_event_only() {
    unimplemented!("strict + relaxed, file-backed DB, report p50/p95/p99");
}

/// T2: One event + one projection patch (single Put).
#[test]
#[ignore = "benchmark stub: kernel not implemented yet"]
fn bench_t2_commit_event_plus_projection() {
    unimplemented!();
}

/// T3: Event + projection patch + job enqueue (canonical control-plane txn).
/// Budget: strict p95 < 25 ms, p99 < 75 ms; relaxed p95 < 5 ms.
#[test]
#[ignore = "benchmark stub: kernel not implemented yet"]
fn bench_t3_commit_event_projection_job() {
    unimplemented!();
}

/// T4: Claim one job from one active partition.
#[test]
#[ignore = "benchmark stub: kernel not implemented yet"]
fn bench_t4_claim_one_job() {
    unimplemented!();
}

/// T5: Claim 100 jobs across 100 active partitions (round-robin path).
/// Budget: p95 < 15 ms with 100 active partitions.
#[test]
#[ignore = "benchmark stub: kernel not implemented yet"]
fn bench_t5_claim_100_jobs_100_partitions() {
    unimplemented!();
}

/// T6: Completion event + projection update + job ack in one batch.
#[test]
#[ignore = "benchmark stub: kernel not implemented yet"]
fn bench_t6_completion_projection_ack() {
    unimplemented!();
}

/// T7: Lease extension on a currently leased job.
#[test]
#[ignore = "benchmark stub: kernel not implemented yet"]
fn bench_t7_lease_extension() {
    unimplemented!();
}

/// T8: Uncertain resolution (Uncertain -> Succeeded).
#[test]
#[ignore = "benchmark stub: kernel not implemented yet"]
fn bench_t8_uncertain_resolution() {
    unimplemented!();
}

// ---------------------------------------------------------------------------
// Scale workloads (docs/BENCHMARKS.md §2.2)
// ---------------------------------------------------------------------------

/// Re-run transaction workloads at 10k / 100k / 1m events.
#[test]
#[ignore = "benchmark stub: kernel not implemented yet"]
fn bench_scale_event_history() {
    unimplemented!("populations: 10_000, 100_000, 1_000_000 events");
}

/// Re-run job workloads at 10k / 100k / 1m terminal jobs.
#[test]
#[ignore = "benchmark stub: kernel not implemented yet"]
fn bench_scale_terminal_jobs() {
    unimplemented!("populations: 10_000, 100_000, 1_000_000 terminal jobs");
}

/// Claim latency vs. partition mix: 100/100, 100/100k, 1k/1m
/// (active/historical). Claim latency must not grow with historical
/// terminal partitions.
#[test]
#[ignore = "benchmark stub: kernel not implemented yet"]
fn bench_scale_active_vs_historical_partitions() {
    unimplemented!();
}

// ---------------------------------------------------------------------------
// Operational workloads (docs/BENCHMARKS.md §2.3)
// ---------------------------------------------------------------------------

/// O1: Store open time at each event-history scale. No replay permitted.
/// Budget: open 1m-event DB < 250 ms; RSS increase on open < 128 MiB.
#[test]
#[ignore = "benchmark stub: kernel not implemented yet"]
fn bench_o1_open_time() {
    unimplemented!("fresh process per measurement; record peak RSS");
}

/// O2: Read 100 events from one stream in a 1m-event DB.
/// Budget: < 10 ms warm.
#[test]
#[ignore = "benchmark stub: kernel not implemented yet"]
fn bench_o2_stream_read() {
    unimplemented!();
}

/// O3: Global event pagination in 256-event pages; must be linear.
#[test]
#[ignore = "benchmark stub: kernel not implemented yet"]
fn bench_o3_global_event_pagination() {
    unimplemented!();
}

/// O4: Projection point read. Budget: p95 < 5 ms.
#[test]
#[ignore = "benchmark stub: kernel not implemented yet"]
fn bench_o4_projection_point_read() {
    unimplemented!();
}

/// O5: Paginated byte-prefix scan over projection entries.
#[test]
#[ignore = "benchmark stub: kernel not implemented yet"]
fn bench_o5_projection_prefix_scan() {
    unimplemented!();
}

/// O6: Job pagination; must not re-sort total history per page.
#[test]
#[ignore = "benchmark stub: kernel not implemented yet"]
fn bench_o6_job_pagination() {
    unimplemented!();
}

/// O7: SQLite online backup of ~1 GiB DB while commits continue.
/// Budget: no writer outage exceeding 100 ms.
#[test]
#[ignore = "benchmark stub: kernel not implemented yet"]
fn bench_o7_live_backup_writer_stall() {
    unimplemented!();
}

/// O8: Integrity verification (integrity_check + FK check + semantic checks).
#[test]
#[ignore = "benchmark stub: kernel not implemented yet"]
fn bench_o8_integrity_verification() {
    unimplemented!();
}

/// O9: Paged diagnostic export of full history; total time and peak RSS.
#[test]
#[ignore = "benchmark stub: kernel not implemented yet"]
fn bench_o9_diagnostic_export() {
    unimplemented!();
}
