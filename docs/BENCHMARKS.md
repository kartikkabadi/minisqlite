# Performance and Benchmark Specification

Status: Design specification (Phase 6 of the control-plane kernel rewrite plan)

This document defines the benchmark environment requirements, required workloads,
initial performance budgets, regression-enforcement policy, and measurement
harness for the SQLite-backed control-plane kernel. No benchmark result may be
published or used for an architectural decision unless it complies with this
specification.

Motivation: the prior journal-based implementation reported one million Strict
(fsync-backed) commits completing *faster* than unsynced writes. Results like
that are environmental artifacts, not evidence. Every number produced under this
spec must be reproducible, fully environment-documented, and distribution-aware
(median/p95/p99), or it is rejected.

---

## 1. Benchmark environment requirements

Every benchmark report MUST state all of the following fields. Reports missing
any field are invalid and must not be used in PRs, ADRs, or release notes.

| Field | Description | Example |
| --- | --- | --- |
| CPU | Model, core count, base/boost clocks | Apple M2 Pro, 10 cores |
| RAM | Total physical memory | 32 GiB |
| OS | Name, version, kernel | Ubuntu 24.04, 6.8.0 |
| SQLite version | `sqlite3_libversion()` of the linked library | 3.46.1 |
| Filesystem | Type and mount options | ext4, `rw,relatime` |
| Storage device | Physical device class; local SSD vs. virtual/network FS | NVMe SSD (local), not tmpfs/overlayfs |
| Durability mode | `Strict` (`synchronous=FULL`) or `Relaxed` (`synchronous=NORMAL`) | Strict |
| DB size | On-disk database file size at measurement time | 182 MiB |
| WAL state | WAL enabled? WAL file size; checkpoint state before run | WAL on, 4 MiB, truncated |
| Page size | SQLite page size | 4096 |
| Warm vs. cold cache | Whether OS page cache / SQLite cache was primed | cold (drop_caches) |
| Iterations | Number of measured iterations after warm-up | 10,000 |
| Median | p50 latency | 1.8 ms |
| p95 | 95th percentile latency | 4.2 ms |
| p99 | 99th percentile latency | 9.1 ms |
| Peak RSS | Peak resident set size of the process during the run | 96 MiB |

Additional rules:

- **Suspicious durability results are release-blocking to publish.** If Strict
  (fsync) throughput approaches or exceeds Relaxed/unsynced throughput, the
  result must be flagged and investigated (ephemeral filesystem, virtualized
  storage, coalesced syncs, lying cache) rather than reported. Reject
  unexplained results that imply impossible durability behavior.
- Benchmarks must run on a **real local SSD**, never tmpfs, overlayfs, or a
  network/virtual filesystem, unless the report explicitly measures that
  environment and says so.
- Reported latencies must always include the full distribution (median, p95,
  p99), never a single mean or total wall time alone.
- Warm-up iterations must be stated and excluded from the measured set.
- These are engineering budgets and internal evidence, not public marketing
  claims. Public claims must link to the full environment report.

---

## 2. Required benchmark workloads

### 2.1 Transaction workloads

Single-operation latency of the writer path, measured per commit against a
file-backed database (never `:memory:`), in both Strict and Relaxed modes:

| ID | Workload | Description |
| --- | --- | --- |
| T1 | Event only | `CommitBatch` appending one event to one stream |
| T2 | Event + projection | One event + one `ProjectionPatch` (single Put) |
| T3 | Event + projection + job | One event + one patch + one `EnqueueJob` (the canonical control-plane transaction) |
| T4 | Claim one | `claim_jobs` with limit 1, one active partition |
| T5 | Claim 100 / 100 partitions | `claim_jobs` with limit 100 across 100 active partitions (round-robin fairness path) |
| T6 | Completion + ack | Completion event + projection update + `AckJob` in one batch |
| T7 | Lease extension | `extend_lease` on a currently leased job |
| T8 | Uncertain resolution | `ResolveUncertainJob` transitioning Uncertain → Succeeded |

### 2.2 Scale workloads

Each transaction and operational workload must be re-measured at these
population sizes to detect nonlinear behavior:

**Event history:**

- 10k events
- 100k events
- 1m events

**Terminal job history:**

- 10k jobs
- 100k jobs
- 1m terminal jobs

**Partition scaling** (active = partitions with nonterminal work; historical =
partitions whose work is all terminal):

- 100 active / 100 historical
- 100 active / 100k historical
- 1k active / 1m historical

The key assertion for partition scaling: **claim latency must depend only on
active partitions**, not on historical terminal partitions. Any material growth
in T4/T5 latency across the three partition configurations at fixed active
count is a regression (this was a P1 finding against the journal
implementation, where every claim sorted all partitions ever seen).

### 2.3 Operational workloads

| ID | Workload | Description |
| --- | --- | --- |
| O1 | Open time | Time from `ControlPlaneStore::open` to first successful read, at each event-history scale (no replay permitted) |
| O2 | Stream read | Read 100 events from one stream inside a 1m-event database (must use the `(stream_id, stream_version)` index) |
| O3 | Global event pagination | Page through all events by `global_sequence` in 256-event pages (must be linear, not quadratic) |
| O4 | Projection point read | Single-key `projection_entries` lookup |
| O5 | Prefix scan | Paginated byte-prefix scan over projection entries |
| O6 | Job pagination | Page through jobs via `jobs_page` (must not re-sort total history per page) |
| O7 | Live backup | SQLite online backup of a ~1 GiB database while commits continue; measure writer stall |
| O8 | Integrity verification | `store verify` (integrity_check + foreign_key_check + semantic checks) at each scale |
| O9 | Diagnostic export | Paged `diagnostic-export` of full history; total time and peak RSS |

---

## 3. Initial performance budgets

Release targets from the implementation plan, to be calibrated once against the
designated reference machine. Budgets are enforced at the stated scale on the
reference environment; they are engineering budgets, not public claims.

| Metric | Initial target |
| --- | ---: |
| Strict atomic control-plane commit (T3) p95 | < 25 ms |
| Strict atomic control-plane commit (T3) p99 | < 75 ms |
| Relaxed commit (T3) p95 | < 5 ms |
| Claim p95 with 100 active partitions (T5) | < 15 ms |
| Stream read of 100 events from 1m-event DB (O2) | < 10 ms warm |
| Projection point read (O4) p95 | < 5 ms |
| Open 1m-event database (O1) | < 250 ms |
| Rust process RSS increase on open (1m-event DB) | < 128 MiB |
| Live backup of 1 GiB DB (O7) | No writer outage exceeding 100 ms |
| Historical inactive partitions affecting claim latency | No material growth |

Structural (non-latency) assertions that accompany the budgets:

- Opening a 1m-event store must not load 1m event payloads into Rust memory.
- Open time must not grow linearly with event payload history (no replay).
- Stream reads must use indexes; job pagination must not repeatedly sort total
  history.

---

## 4. Regression enforcement

- **Trend tracking:** every benchmark run stores its full report (environment
  fields + distributions + peak RSS) as an artifact; baselines are kept per
  reference environment.
- **Gross-regression gate (CI-failing), initially:**
  - > 2x baseline latency on any budgeted metric,
  - > 50% peak-RSS increase,
  - algorithmic behavior becoming nonlinear across the scale workloads
    (e.g., claim latency growing with historical partitions, pagination
    turning quadratic).
- **Scheduling:** benchmarks run on a scheduled CI job (nightly/weekly), not on
  every pull request. Performance-sensitive PRs must attach a manual run of the
  affected workloads per §1.
- Budget tightening (moving from the 2x gross gate toward the §3 targets) is
  done deliberately via PRs updating this document, never silently.

---

## 5. Measurement harness

- **Framework:** [`criterion`](https://crates.io/crates/criterion) is the
  default harness (`benches/kernel_bench.rs`, `harness = false`). Criterion
  provides warm-up control, iteration counts, and p50/p95/p99 estimates.
  Workloads that criterion cannot express well (open time at scale, peak RSS,
  backup writer stall, soak-style claim loops) use a manual `bencher`-style
  binary that emits the same report schema.
- **Peak RSS:** measured via `getrusage(RUSAGE_SELF).ru_maxrss` on Unix
  (documented per-platform units), sampled per workload in a fresh process so
  runs do not contaminate each other.
- **Databases:** always file-backed on a local SSD; never `:memory:` (WAL,
  locking, sync, and backup behavior differ). Fresh database per scale
  population; populations are pre-generated fixtures reused across workloads
  within a run.
- **Cold vs. warm:** cold runs drop OS caches (or use a fresh boot/VM) and are
  labeled; warm runs state the priming procedure.
- **Report format:** one machine-readable JSON blob per run containing every §1
  field plus per-workload distributions, checked into the benchmark artifact
  store (not the repo) and summarized in the PR/nightly report.

---

## 6. Out of scope

- Multi-process writer benchmarks (excluded from the product scope).
- Distributed or networked scenarios.
- Comparative marketing benchmarks against other databases.
- Fuzzing and soak testing (covered by the validation program, Phase 7), except
  that soak runs record queue latency, DB/WAL/RSS growth for trend tracking.
