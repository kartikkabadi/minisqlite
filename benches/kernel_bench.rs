//! Manual benchmark harness for the control-plane kernel (`harness = false`).
//!
//! Implements the workloads from `docs/BENCHMARKS.md`: transaction workloads
//! T1-T8, scale workloads (event history, terminal jobs, active vs. historical
//! partitions), and operational workloads O1-O9. Every report prints the full
//! environment metadata required by §1 and per-workload p50/p95/p99
//! distributions.
//!
//! Run with `cargo bench`. By default a reduced-scale profile runs so the
//! suite completes in minutes; set `KERNEL_BENCH_FULL=1` for the full §2.2
//! populations (1m events, 1m terminal jobs, ~1 GiB backup source).

use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::{Duration, Instant};

use minisqlite::{
    ClaimOutcome, ClaimRequest, ClaimedJob, CommitBatch, ControlPlaneStore, Durability, Event, Id,
    JobSpec, ProjectionPatch, Resolution, StoreBuilder,
};

const WARMUP: usize = 10;
const TXN_ITERS: usize = 100;
const CLAIM_BATCH_ITERS: usize = 30;
const POPULATE_BATCH: usize = 1000;
const PAGE: usize = 256;

fn main() {
    // Cargo passes `--bench` when invoked via `cargo bench`; under
    // `cargo test --all-targets` the binary runs without it, so only verify
    // that the harness links and exit (mirrors criterion's behavior).
    if !std::env::args().any(|a| a == "--bench") {
        println!("kernel_bench: pass --bench (cargo bench) to run the suite");
        return;
    }

    let profile = Profile::from_env();
    let root = bench_root();
    print_environment(&root, &profile);

    transaction_workloads(&root);
    scale_event_history(&root, &profile);
    scale_terminal_jobs(&root, &profile);
    scale_partitions(&root, &profile);
    operational_projections(&root);
    live_backup(&root, &profile);

    let _ = std::fs::remove_dir_all(&root);
    println!("\nkernel_bench: done");
}

// ---------------------------------------------------------------------------
// Profile and environment report
// ---------------------------------------------------------------------------

struct Profile {
    full: bool,
    event_populations: Vec<u64>,
    terminal_job_populations: Vec<u64>,
    /// (active partitions, historical terminal partitions)
    partition_mixes: Vec<(u64, u64)>,
    backup_target_bytes: u64,
}

impl Profile {
    fn from_env() -> Self {
        let full = std::env::var("KERNEL_BENCH_FULL").is_ok_and(|v| v == "1");
        if full {
            Self {
                full,
                event_populations: vec![10_000, 100_000, 1_000_000],
                terminal_job_populations: vec![10_000, 100_000, 1_000_000],
                partition_mixes: vec![(100, 100), (100, 100_000), (1000, 1_000_000)],
                backup_target_bytes: 1 << 30,
            }
        } else {
            Self {
                full,
                event_populations: vec![10_000, 100_000],
                terminal_job_populations: vec![10_000, 100_000],
                partition_mixes: vec![(100, 100), (100, 10_000), (1000, 100_000)],
                backup_target_bytes: 64 << 20,
            }
        }
    }
}

fn bench_root() -> PathBuf {
    let root = Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("target")
        .join("kernel-bench");
    let _ = std::fs::remove_dir_all(&root);
    std::fs::create_dir_all(&root).expect("create bench root");
    root
}

fn read_first_match(path: &str, key: &str) -> Option<String> {
    let text = std::fs::read_to_string(path).ok()?;
    text.lines()
        .find(|l| l.starts_with(key))
        .map(|l| l.split(':').nth(1).unwrap_or("").trim().to_string())
}

fn os_pretty_name() -> String {
    std::fs::read_to_string("/etc/os-release")
        .ok()
        .and_then(|t| {
            t.lines().find(|l| l.starts_with("PRETTY_NAME=")).map(|l| {
                l.trim_start_matches("PRETTY_NAME=")
                    .trim_matches('"')
                    .to_string()
            })
        })
        .unwrap_or_else(|| "unknown".into())
}

fn kernel_release() -> String {
    std::fs::read_to_string("/proc/sys/kernel/osrelease")
        .map(|s| s.trim().to_string())
        .unwrap_or_else(|_| "unknown".into())
}

/// Filesystem type and device backing `path`, from the longest mount-point
/// prefix in /proc/mounts.
fn filesystem_for(path: &Path) -> (String, String, String) {
    let mounts = match std::fs::read_to_string("/proc/mounts") {
        Ok(m) => m,
        Err(_) => return ("unknown".into(), "unknown".into(), "unknown".into()),
    };
    let target = path.to_string_lossy();
    let mut best: Option<(&str, &str, &str, &str)> = None;
    for line in mounts.lines() {
        let f: Vec<&str> = line.split_whitespace().collect();
        if f.len() < 4 {
            continue;
        }
        if target.starts_with(f[1]) && best.is_none_or(|b| f[1].len() > b.1.len()) {
            best = Some((f[0], f[1], f[2], f[3]));
        }
    }
    match best {
        Some((dev, _, fstype, opts)) => (fstype.into(), opts.into(), dev.into()),
        None => ("unknown".into(), "unknown".into(), "unknown".into()),
    }
}

fn device_class(device: &str) -> String {
    let name = device.trim_start_matches("/dev/");
    let base: String = name
        .trim_end_matches(|c: char| c.is_ascii_digit())
        .trim_end_matches('p')
        .to_string();
    for candidate in [name, base.as_str()] {
        let path = format!("/sys/block/{candidate}/queue/rotational");
        if let Ok(v) = std::fs::read_to_string(&path) {
            return if v.trim() == "0" {
                format!("non-rotational (SSD-class), device {device}")
            } else {
                format!("rotational (HDD-class), device {device}")
            };
        }
    }
    format!("unknown class, device {device}")
}

fn peak_rss_mib() -> String {
    read_first_match("/proc/self/status", "VmHWM")
        .map(|kb| {
            let kib: f64 = kb.trim_end_matches(" kB").trim().parse().unwrap_or(0.0);
            format!("{:.1} MiB", kib / 1024.0)
        })
        .unwrap_or_else(|| "unavailable".into())
}

fn print_environment(root: &Path, profile: &Profile) {
    let (fstype, opts, device) = filesystem_for(root);
    println!("== kernel_bench environment report (docs/BENCHMARKS.md §1) ==");
    println!(
        "CPU:             {} ({} logical cores)",
        read_first_match("/proc/cpuinfo", "model name").unwrap_or_else(|| "unknown".into()),
        std::thread::available_parallelism().map_or(0, std::num::NonZero::get)
    );
    println!(
        "RAM:             {}",
        read_first_match("/proc/meminfo", "MemTotal").unwrap_or_else(|| "unknown".into())
    );
    println!(
        "OS:              {}, kernel {}",
        os_pretty_name(),
        kernel_release()
    );
    println!("SQLite version:  {}", rusqlite::version());
    println!("Filesystem:      {fstype}, {opts}");
    println!("Storage device:  {}", device_class(&device));
    println!("Page size:       4096 (SQLite default; store does not override)");
    println!("WAL state:       WAL on (set at open), fresh DB per fixture");
    println!("Cache state:     warm (in-process, no cache drop between iterations)");
    println!("Bench DB root:   {}", root.display());
    println!(
        "Profile:         {} (KERNEL_BENCH_FULL={})",
        if profile.full {
            "full §2.2 populations"
        } else {
            "reduced populations"
        },
        u8::from(profile.full)
    );
    println!("Warm-up iters:   {WARMUP} (excluded from measurements)");
    println!("Durability:      per-workload, printed on each line (strict=FULL, relaxed=NORMAL)");
}

// ---------------------------------------------------------------------------
// Measurement helpers
// ---------------------------------------------------------------------------

/// Deterministic ID generator: sequential u128 values in a private range.
struct IdGen(u128);

impl IdGen {
    fn new(range: u128) -> Self {
        Self(range << 64)
    }

    fn next(&mut self) -> Id {
        self.0 += 1;
        Id::from(self.0)
    }
}

/// Deterministic logical clock (milliseconds).
struct Clock(i64);

impl Clock {
    fn new() -> Self {
        Self(1_000)
    }

    fn tick(&mut self) -> i64 {
        self.0 += 1;
        self.0
    }
}

fn fmt_dur(d: Duration) -> String {
    let us = d.as_secs_f64() * 1e6;
    if us >= 10_000.0 {
        format!("{:.2}ms", us / 1000.0)
    } else {
        format!("{us:.0}us")
    }
}

fn percentile(sorted: &[Duration], p: f64) -> Duration {
    let idx = ((sorted.len() - 1) as f64 * p).round() as usize;
    sorted[idx]
}

fn report(name: &str, samples: &mut [Duration]) {
    samples.sort_unstable();
    println!(
        "{name:<44} n={:<7} p50={:<9} p95={:<9} p99={:<9} min={:<9} max={}",
        samples.len(),
        fmt_dur(percentile(samples, 0.50)),
        fmt_dur(percentile(samples, 0.95)),
        fmt_dur(percentile(samples, 0.99)),
        fmt_dur(samples[0]),
        fmt_dur(samples[samples.len() - 1]),
    );
}

/// Run `warmup` unmeasured then `iters` measured iterations and report.
fn run_workload(name: &str, warmup: usize, iters: usize, mut f: impl FnMut()) {
    for _ in 0..warmup {
        f();
    }
    let mut samples = Vec::with_capacity(iters);
    for _ in 0..iters {
        let t = Instant::now();
        f();
        samples.push(t.elapsed());
    }
    report(name, &mut samples);
}

fn report_single(name: &str, d: Duration, extra: &str) {
    println!("{name:<44} n=1       time={:<9} {extra}", fmt_dur(d));
}

fn open_store(path: &Path, durability: Durability) -> ControlPlaneStore {
    StoreBuilder::new(path)
        .durability(durability)
        .open()
        .expect("open store")
}

fn durability_label(d: Durability) -> &'static str {
    match d {
        Durability::Strict => "strict",
        Durability::Relaxed => "relaxed",
    }
}

fn event(ids: &mut IdGen, clock: &mut Clock, stream: &str) -> Event {
    Event::with_json_payload(ids.next(), stream, "bench", clock.tick(), b"{\"n\":1}")
}

fn claimed_jobs(outcome: ClaimOutcome) -> Vec<ClaimedJob> {
    match outcome {
        ClaimOutcome::Committed(claims) => claims.into_jobs(),
        _ => Vec::new(),
    }
}

fn claim(store: &ControlPlaneStore, queue: &str, now_ms: i64, limit: usize) -> Vec<ClaimedJob> {
    let request = ClaimRequest {
        queue: queue.into(),
        worker_id: "bench-worker".into(),
        now_ms,
        lease_ms: 3_600_000,
        limit,
    };
    claimed_jobs(store.claim_jobs(&request).expect("claim jobs"))
}

fn ack_all(store: &ControlPlaneStore, ids: &mut IdGen, clock: &mut Clock, jobs: &[ClaimedJob]) {
    for chunk in jobs.chunks(1000) {
        let mut batch = CommitBatch::new(ids.next(), clock.tick());
        for job in chunk {
            batch = batch.acknowledge_job(job.job_id, job.lease_token, None);
        }
        store.commit(&batch).expect("ack jobs");
    }
}

// ---------------------------------------------------------------------------
// Fixture population
// ---------------------------------------------------------------------------

/// Append `count` small events round-robin over `streams` streams, in relaxed
/// batches of up to `POPULATE_BATCH` events per commit.
fn populate_events(
    store: &ControlPlaneStore,
    ids: &mut IdGen,
    clock: &mut Clock,
    count: u64,
    streams: u64,
) {
    let mut written = 0u64;
    while written < count {
        let n = POPULATE_BATCH.min((count - written) as usize);
        let mut batch = CommitBatch::new(ids.next(), clock.tick());
        for i in 0..n {
            let stream = format!("s{}", (written + i as u64) % streams);
            batch = batch.append_event(event(ids, clock, &stream));
        }
        store.commit(&batch).expect("populate events");
        written += n as u64;
    }
}

/// Enqueue `count` jobs spread over `partitions` partitions named
/// `{prefix}{i}`, in relaxed batches.
fn populate_jobs(
    store: &ControlPlaneStore,
    ids: &mut IdGen,
    clock: &mut Clock,
    queue: &str,
    prefix: &str,
    count: u64,
    partitions: u64,
) {
    let mut written = 0u64;
    while written < count {
        let n = POPULATE_BATCH.min((count - written) as usize);
        let mut batch = CommitBatch::new(ids.next(), clock.tick());
        for i in 0..n {
            let partition = format!("{prefix}{}", (written + i as u64) % partitions);
            batch = batch.enqueue_job(JobSpec::reconcilable(
                ids.next(),
                queue,
                partition,
                b"{}".to_vec(),
            ));
        }
        store.commit(&batch).expect("populate jobs");
        written += n as u64;
    }
}

/// Drive every pending job in `queue` to the terminal Succeeded state via
/// claim + ack rounds (one job per partition per round).
fn terminalize_queue(store: &ControlPlaneStore, ids: &mut IdGen, clock: &mut Clock, queue: &str) {
    loop {
        let jobs = claim(store, queue, clock.tick(), 1000);
        if jobs.is_empty() {
            break;
        }
        ack_all(store, ids, clock, &jobs);
    }
}

// ---------------------------------------------------------------------------
// §2.1 Transaction workloads (T1-T8)
// ---------------------------------------------------------------------------

fn transaction_workloads(root: &Path) {
    println!("\n== §2.1 transaction workloads (fresh file-backed DB per workload) ==");
    for durability in [Durability::Strict, Durability::Relaxed] {
        let label = durability_label(durability);
        commit_workloads(root, durability, label);
        claim_workloads(root, durability, label);
        lifecycle_workloads(root, durability, label);
    }
}

fn commit_workloads(root: &Path, durability: Durability, label: &str) {
    let dir = root.join(format!("txn-{label}"));
    std::fs::create_dir_all(&dir).expect("mkdir");
    let mut ids = IdGen::new(1);
    let mut clock = Clock::new();

    let store = open_store(&dir.join("t1.db"), durability);
    run_workload(
        &format!("T1 commit event only [{label}]"),
        WARMUP,
        TXN_ITERS,
        || {
            let batch = CommitBatch::new(ids.next(), clock.tick())
                .append_event(event(&mut ids, &mut clock, "s"));
            store.commit(&batch).expect("T1 commit");
        },
    );

    let store = open_store(&dir.join("t2.db"), durability);
    let mut version = 0u64;
    run_workload(
        &format!("T2 event + projection [{label}]"),
        WARMUP,
        TXN_ITERS,
        || {
            let batch = CommitBatch::new(ids.next(), clock.tick())
                .append_event(event(&mut ids, &mut clock, "s"))
                .apply_projection_patch(
                    ProjectionPatch::new("proj", version).put(b"key", b"value"),
                );
            store.commit(&batch).expect("T2 commit");
            version += 1;
        },
    );

    let store = open_store(&dir.join("t3.db"), durability);
    let mut version = 0u64;
    let mut n = 0u64;
    run_workload(
        &format!("T3 event + projection + job [{label}]"),
        WARMUP,
        TXN_ITERS,
        || {
            let job = JobSpec::reconcilable(ids.next(), "q", format!("p{n}"), b"{}".to_vec());
            let batch = CommitBatch::new(ids.next(), clock.tick())
                .append_event(event(&mut ids, &mut clock, "s"))
                .apply_projection_patch(ProjectionPatch::new("proj", version).put(b"key", b"value"))
                .enqueue_job(job);
            store.commit(&batch).expect("T3 commit");
            version += 1;
            n += 1;
        },
    );
}

fn claim_workloads(root: &Path, durability: Durability, label: &str) {
    let dir = root.join(format!("claim-{label}"));
    std::fs::create_dir_all(&dir).expect("mkdir");
    let mut ids = IdGen::new(2);
    let mut clock = Clock::new();

    // T4: one pending job per partition; every claim leases the next partition.
    let path = dir.join("t4.db");
    {
        let setup = open_store(&path, Durability::Relaxed);
        let total = (WARMUP + TXN_ITERS) as u64;
        populate_jobs(&setup, &mut ids, &mut clock, "q", "p", total, total);
    }
    let store = open_store(&path, durability);
    run_workload(
        &format!("T4 claim one job [{label}]"),
        WARMUP,
        TXN_ITERS,
        || {
            let jobs = claim(&store, "q", clock.tick(), 1);
            assert_eq!(jobs.len(), 1, "T4 expected one claimed job");
        },
    );

    // T5: 100 partitions with enough depth for every iteration; jobs are acked
    // (untimed) between iterations so each partition head is pending again.
    let path = dir.join("t5.db");
    {
        let setup = open_store(&path, Durability::Relaxed);
        let total = (WARMUP + CLAIM_BATCH_ITERS) as u64 * 100;
        populate_jobs(&setup, &mut ids, &mut clock, "q", "p", total, 100);
    }
    let store = open_store(&path, durability);
    let mut leased: Vec<ClaimedJob> = Vec::new();
    run_workload(
        &format!("T5 claim 100 jobs / 100 partitions [{label}]"),
        WARMUP,
        CLAIM_BATCH_ITERS,
        || {
            ack_all(&store, &mut ids, &mut clock, &leased);
            leased = claim(&store, "q", clock.tick(), 100);
            assert_eq!(leased.len(), 100, "T5 expected 100 claimed jobs");
        },
    );
}

fn lifecycle_workloads(root: &Path, durability: Durability, label: &str) {
    let dir = root.join(format!("life-{label}"));
    std::fs::create_dir_all(&dir).expect("mkdir");
    let mut ids = IdGen::new(3);
    let mut clock = Clock::new();

    // T6: completion event + projection update + ack in one batch, against
    // jobs claimed outside the timed section.
    let path = dir.join("t6.db");
    let total = (WARMUP + TXN_ITERS) as u64;
    {
        let setup = open_store(&path, Durability::Relaxed);
        populate_jobs(&setup, &mut ids, &mut clock, "q", "p", total, total);
    }
    let store = open_store(&path, durability);
    let mut pending: Vec<ClaimedJob> = Vec::new();
    let mut version = 0u64;
    run_workload(
        &format!("T6 completion + projection + ack [{label}]"),
        WARMUP,
        TXN_ITERS,
        || {
            if pending.is_empty() {
                pending = claim(&store, "q", clock.tick(), 100);
                pending.reverse();
            }
            let job = pending.pop().expect("T6 claimed job");
            let batch = CommitBatch::new(ids.next(), clock.tick())
                .append_event(event(&mut ids, &mut clock, "s"))
                .apply_projection_patch(ProjectionPatch::new("proj", version).put(b"key", b"done"))
                .acknowledge_job(job.job_id, job.lease_token, None);
            store.commit(&batch).expect("T6 commit");
            version += 1;
        },
    );

    // T7: repeated lease extension on one leased job.
    let path = dir.join("t7.db");
    {
        let setup = open_store(&path, Durability::Relaxed);
        populate_jobs(&setup, &mut ids, &mut clock, "q", "p", 1, 1);
    }
    let store = open_store(&path, durability);
    let job = claim(&store, "q", clock.tick(), 1).pop().expect("T7 claim");
    let mut expiry = job.lease_expires_at_ms;
    run_workload(
        &format!("T7 lease extension [{label}]"),
        WARMUP,
        TXN_ITERS,
        || {
            expiry += 1_000;
            store
                .extend_lease(job.job_id, job.lease_token, expiry, clock.tick())
                .expect("T7 extend lease");
        },
    );

    // T8: resolve Uncertain -> Succeeded. Jobs are made uncertain by claiming
    // with a short lease and letting maintenance repair the expiry.
    let path = dir.join("t8.db");
    let total = WARMUP + TXN_ITERS;
    let mut uncertain: Vec<Id> = Vec::new();
    {
        let setup = open_store(&path, Durability::Relaxed);
        populate_jobs(
            &setup,
            &mut ids,
            &mut clock,
            "q",
            "p",
            total as u64,
            total as u64,
        );
        let mut short_leased = Vec::new();
        loop {
            let request = ClaimRequest {
                queue: "q".into(),
                worker_id: "bench-worker".into(),
                now_ms: clock.tick(),
                lease_ms: 1,
                limit: 1000,
            };
            let jobs = claimed_jobs(setup.claim_jobs(&request).expect("T8 claim"));
            if jobs.is_empty() && short_leased.len() >= total {
                break;
            }
            short_leased.extend(jobs.into_iter().map(|j| j.job_id));
        }
        clock.0 += 3_600_000; // expire every short lease
        while uncertain.len() < total {
            // Maintenance repairs at most 64 expired leases per claim call.
            let request = ClaimRequest {
                queue: "q".into(),
                worker_id: "bench-worker".into(),
                now_ms: clock.tick(),
                lease_ms: 1,
                limit: 0,
            };
            setup.claim_jobs(&request).expect("T8 maintenance");
            let n = uncertain.len();
            uncertain = short_leased
                .iter()
                .copied()
                .filter(|id| {
                    setup
                        .job(*id)
                        .expect("T8 job lookup")
                        .expect("T8 job")
                        .state
                        == minisqlite::JobState::Uncertain
                })
                .collect();
            if uncertain.len() == n {
                break;
            }
        }
        uncertain.truncate(total);
    }
    let store = open_store(&path, durability);
    let mut next = 0usize;
    run_workload(
        &format!("T8 uncertain resolution [{label}]"),
        WARMUP,
        TXN_ITERS,
        || {
            let job_id = uncertain[next];
            next += 1;
            let batch = CommitBatch::new(ids.next(), clock.tick())
                .resolve_uncertain_job(job_id, Resolution::MarkSucceeded);
            store.commit(&batch).expect("T8 resolve");
        },
    );
}

// ---------------------------------------------------------------------------
// §2.2 scale workloads + §2.3 O1/O2/O3/O8/O9 (event history)
// ---------------------------------------------------------------------------

fn scale_event_history(root: &Path, profile: &Profile) {
    println!("\n== §2.2 event-history scale + O1/O2/O3/O8/O9 (grown incrementally) ==");
    let dir = root.join("scale-events");
    std::fs::create_dir_all(&dir).expect("mkdir");
    let path = dir.join("events.db");
    let mut ids = IdGen::new(4);
    let mut clock = Clock::new();
    let mut population = 0u64;

    for &target in &profile.event_populations {
        {
            let setup = open_store(&path, Durability::Relaxed);
            populate_events(&setup, &mut ids, &mut clock, target - population, 100);
        }
        population = target;

        // T1/T3-style commits at scale, both durability modes.
        for durability in [Durability::Strict, Durability::Relaxed] {
            let label = durability_label(durability);
            let store = open_store(&path, durability);
            let mut n = store.projection_version("scale").expect("scale version");
            run_workload(
                &format!("T3 @ {population} events [{label}]"),
                WARMUP,
                TXN_ITERS,
                || {
                    let job = JobSpec::reconcilable(
                        ids.next(),
                        "scaleq",
                        format!("p{n}"),
                        b"{}".to_vec(),
                    );
                    let batch = CommitBatch::new(ids.next(), clock.tick())
                        .append_event(event(&mut ids, &mut clock, "s0"))
                        .apply_projection_patch(
                            ProjectionPatch::new("scale", n).put(b"key", b"value"),
                        )
                        .enqueue_job(job);
                    store.commit(&batch).expect("scale T3 commit");
                    n += 1;
                },
            );
            population += (WARMUP + TXN_ITERS) as u64;
        }

        // O1: open time (store dropped and reopened per measurement).
        let rss_before = peak_rss_mib();
        let mut samples = Vec::new();
        for _ in 0..5 {
            let t = Instant::now();
            let store = ControlPlaneStore::open(&path).expect("O1 open");
            store.stream_version("s0").expect("O1 first read");
            samples.push(t.elapsed());
        }
        report(
            &format!("O1 open + first read @ {population} events"),
            &mut samples,
        );
        println!(
            "    peak RSS before/after O1 opens: {rss_before} / {} (VmHWM, process-wide)",
            peak_rss_mib()
        );

        let store = open_store(&path, Durability::Relaxed);

        // O2: read 100 events from one stream.
        run_workload(
            &format!("O2 stream read 100 @ {population} events"),
            WARMUP,
            TXN_ITERS,
            || {
                let events = store.stream_events("s0", 1, 100).expect("O2 stream read");
                assert_eq!(events.len(), 100, "O2 expected 100 events");
            },
        );

        // O3: global pagination over the full history in 256-event pages.
        let mut pages = Vec::new();
        let mut cursor = 0u64;
        loop {
            let t = Instant::now();
            let events = store.events_after(cursor, PAGE).expect("O3 page");
            pages.push(t.elapsed());
            match events.last() {
                Some(last) if events.len() == PAGE => cursor = last.global_sequence,
                _ => break,
            }
        }
        let (first, last) = (pages[0], pages[pages.len() - 1]);
        report(
            &format!("O3 global pagination page @ {population} events"),
            &mut pages,
        );
        println!(
            "    O3 linearity check: first page {} vs last page {}",
            fmt_dur(first),
            fmt_dur(last)
        );

        // O8: integrity verification.
        let t = Instant::now();
        let verify = store.verify().expect("O8 verify");
        report_single(
            &format!("O8 verify @ {population} events"),
            t.elapsed(),
            &format!("ok={}", verify.is_ok()),
        );

        // O9: paged diagnostic export of the full history.
        let t = Instant::now();
        let export = store.diagnostic_export().expect("O9 export");
        report_single(
            &format!("O9 diagnostic export @ {population} events"),
            t.elapsed(),
            &format!("bytes={} peak_rss={}", export.len(), peak_rss_mib()),
        );
    }
}

// ---------------------------------------------------------------------------
// §2.2 terminal-job scale + O6 (job pagination)
// ---------------------------------------------------------------------------

fn scale_terminal_jobs(root: &Path, profile: &Profile) {
    println!("\n== §2.2 terminal-job scale + O6 (grown incrementally) ==");
    let dir = root.join("scale-jobs");
    std::fs::create_dir_all(&dir).expect("mkdir");
    let path = dir.join("jobs.db");
    let mut ids = IdGen::new(5);
    let mut clock = Clock::new();
    let mut population = 0u64;

    for &target in &profile.terminal_job_populations {
        {
            let setup = open_store(&path, Durability::Relaxed);
            populate_jobs(
                &setup,
                &mut ids,
                &mut clock,
                "histq",
                "h",
                target - population,
                1000,
            );
            terminalize_queue(&setup, &mut ids, &mut clock, "histq");
        }
        population = target;

        let store = open_store(&path, Durability::Relaxed);

        // T4 against a fresh active queue sharing the DB with the terminal history.
        let total = (WARMUP + TXN_ITERS) as u64;
        let live_queue = format!("liveq{population}");
        populate_jobs(&store, &mut ids, &mut clock, &live_queue, "a", total, total);
        run_workload(
            &format!("T4 claim one @ {population} terminal jobs [relaxed]"),
            WARMUP,
            TXN_ITERS,
            || {
                let jobs = claim(&store, &live_queue, clock.tick(), 1);
                assert_eq!(jobs.len(), 1, "scale T4 expected one job");
            },
        );

        // O6: job pagination (public API paginates by state filter + limit).
        run_workload(
            &format!("O6 jobs page (limit {PAGE}) @ {population} terminal jobs"),
            WARMUP,
            TXN_ITERS,
            || {
                let jobs = store.jobs(Some("histq"), None, PAGE).expect("O6 jobs page");
                assert_eq!(jobs.len(), PAGE, "O6 expected a full page");
            },
        );
    }
}

// ---------------------------------------------------------------------------
// §2.2 active vs. historical partition scaling (the P1 claim-latency check)
// ---------------------------------------------------------------------------

fn scale_partitions(root: &Path, profile: &Profile) {
    println!("\n== §2.2 partition scaling: claim latency vs. historical partitions ==");
    let dir = root.join("scale-partitions");
    std::fs::create_dir_all(&dir).expect("mkdir");
    let mut ids = IdGen::new(6);
    let mut clock = Clock::new();

    for &(active, historical) in &profile.partition_mixes {
        let path = dir.join(format!("mix-{active}-{historical}.db"));
        let depth = (WARMUP + CLAIM_BATCH_ITERS) as u64;
        {
            let setup = open_store(&path, Durability::Relaxed);
            populate_jobs(
                &setup, &mut ids, &mut clock, "q", "hist", historical, historical,
            );
            terminalize_queue(&setup, &mut ids, &mut clock, "q");
            populate_jobs(
                &setup,
                &mut ids,
                &mut clock,
                "q",
                "act",
                active * depth,
                active,
            );
        }
        let store = open_store(&path, Durability::Relaxed);
        let limit = 100.min(active as usize);
        let mut leased: Vec<ClaimedJob> = Vec::new();
        run_workload(
            &format!("T5 claim {limit} @ {active} active / {historical} historical"),
            WARMUP,
            CLAIM_BATCH_ITERS,
            || {
                ack_all(&store, &mut ids, &mut clock, &leased);
                leased = claim(&store, "q", clock.tick(), limit);
                assert_eq!(leased.len(), limit, "partition-mix claim shortfall");
            },
        );
    }
    println!("    assertion: claim latency must not grow with historical partitions (§2.2)");
}

// ---------------------------------------------------------------------------
// §2.3 O4/O5: projection point read and prefix scan
// ---------------------------------------------------------------------------

fn operational_projections(root: &Path) {
    println!("\n== §2.3 projection reads (O4/O5, 10k-entry projection) ==");
    let dir = root.join("projections");
    std::fs::create_dir_all(&dir).expect("mkdir");
    let path = dir.join("proj.db");
    let mut ids = IdGen::new(7);
    let mut clock = Clock::new();
    let entries = 10_000u64;
    {
        let setup = open_store(&path, Durability::Relaxed);
        let mut version = 0u64;
        let mut written = 0u64;
        while written < entries {
            let n = POPULATE_BATCH.min((entries - written) as usize);
            let mut patch = ProjectionPatch::new("proj", version);
            for i in 0..n {
                let key = format!("key/{:08}", written + i as u64);
                patch = patch.put(key.into_bytes(), b"value".to_vec());
            }
            let batch = CommitBatch::new(ids.next(), clock.tick()).apply_projection_patch(patch);
            setup.commit(&batch).expect("populate projection");
            version += 1;
            written += n as u64;
        }
    }
    let store = open_store(&path, Durability::Relaxed);

    let mut n = 0u64;
    run_workload("O4 projection point read", WARMUP, TXN_ITERS, || {
        let key = format!("key/{:08}", (n * 7919) % entries);
        let value = store
            .projection_get("proj", key.as_bytes())
            .expect("O4 get");
        assert!(value.is_some(), "O4 expected a value");
        n += 1;
    });

    let mut pages = Vec::new();
    let mut after: Option<Vec<u8>> = None;
    loop {
        let t = Instant::now();
        let page = store
            .projection_scan_prefix_page("proj", b"key/", after.as_deref(), PAGE)
            .expect("O5 page");
        pages.push(t.elapsed());
        match page.last() {
            Some(last) if page.len() == PAGE => after = Some(last.key.clone()),
            _ => break,
        }
    }
    report("O5 prefix scan page (256 entries)", &mut pages);
}

// ---------------------------------------------------------------------------
// §2.3 O7: live backup writer stall
// ---------------------------------------------------------------------------

fn live_backup(root: &Path, profile: &Profile) {
    println!("\n== §2.3 O7 live backup (writer stall while commits continue) ==");
    let dir = root.join("backup");
    std::fs::create_dir_all(&dir).expect("mkdir");
    let path = dir.join("source.db");
    let mut ids = IdGen::new(8);
    let mut clock = Clock::new();

    // Grow the source DB toward the target size with 32 KiB event payloads.
    let payload = vec![b'x'; 32 << 10];
    {
        let setup = open_store(&path, Durability::Relaxed);
        loop {
            let size = std::fs::metadata(&path).map(|m| m.len()).unwrap_or(0);
            if size >= profile.backup_target_bytes {
                break;
            }
            let mut batch = CommitBatch::new(ids.next(), clock.tick());
            for _ in 0..256 {
                batch = batch.append_event(Event::with_json_payload(
                    ids.next(),
                    "bulk",
                    "bench",
                    clock.tick(),
                    &payload,
                ));
            }
            setup.commit(&batch).expect("grow backup source");
        }
    }
    let size = std::fs::metadata(&path).map(|m| m.len()).unwrap_or(0);

    let store = open_store(&path, Durability::Relaxed);
    let stop = AtomicBool::new(false);
    let dest = dir.join("backup.db");
    let (backup_time, max_stall) = std::thread::scope(|scope| {
        let store = &store;
        let stop = &stop;
        let writer = scope.spawn(move || {
            let mut ids = IdGen::new(9);
            let mut now = 10_000_000i64;
            let mut max_commit = Duration::ZERO;
            while !stop.load(Ordering::Relaxed) {
                now += 1;
                let batch = CommitBatch::new(ids.next(), now).append_event(
                    Event::with_json_payload(ids.next(), "live", "bench", now, b"{}"),
                );
                let t = Instant::now();
                store.commit(&batch).expect("O7 live commit");
                max_commit = max_commit.max(t.elapsed());
            }
            max_commit
        });
        std::thread::sleep(Duration::from_millis(50));
        let t = Instant::now();
        store.backup(&dest, true).expect("O7 backup");
        let backup_time = t.elapsed();
        stop.store(true, Ordering::Relaxed);
        (backup_time, writer.join().expect("O7 writer thread"))
    });
    report_single(
        &format!("O7 live backup of {} MiB DB", size >> 20),
        backup_time,
        &format!(
            "max writer stall (worst commit latency)={}",
            fmt_dur(max_stall)
        ),
    );
}
