# Roadmap: SQLite-backed control-plane kernel

An 8-week plan covering Phases 0–7, following the control-plane kernel
end-to-end execution plan. Tracking: Issue #10. Storage decision:
[ADR-001](./ADR-001.md). Scope boundaries: [SCOPE.md](./SCOPE.md).

Ordering principle: **semantics first**. The public semantic API and
backend-independent contract tests (Phase 1) come before any SQLite
implementation work (Phase 2).

## Phase 0 — Program reset and governance (Days 0–3)

Freeze and archive the journal rewrite (PR #9,
`archive/append-only-journal-v1`, tag `journal-v1-experimental`); establish
the ADR, roadmap, scope, and PR rules before any implementation.

**Exit criteria:** ADR-001, ROADMAP, SCOPE, and PR-RULES merged to `main`;
ADR signed off by the technical decision owner and referenced from the root
README; PR #9 archived and closed; Issue #10 open as the tracking issue;
a named decision owner recorded.

## Phase 1 — Define the semantic kernel (Week 1)

Define what the kernel means independently of any storage implementation:
the public transaction model (`CommitBatch` with transaction IDs and
idempotent resubmission), commit result contract with typed
`Indeterminate` outcomes and transaction recovery, the safe claim contract
(non-iterable `IndeterminateClaim`, claim recovery), the `ProjectionPatch`
model, the job state machine (Pending, Leased, RetryWait, Uncertain,
Succeeded, Dead, Cancelled), the effect-mode contract
(`RequiresReconciliation` default, `ExplicitlyIdempotent` with idempotency
keys), and lease extension. Build backend-independent contract tests for
all of the above.

**Exit criteria:** public API review completed; no physical storage fields
in public types; no `frame_offset`; no `Durability::Memory` (use `Strict`
and `Relaxed`); uncertain claims cannot yield executable work; contract
tests compile independently of SQLite internals; API documentation includes
executable examples using exhaustive matching.

## Phase 2 — SQLite persistence core (Weeks 2–3)

SQLite-backed implementation of the Phase 1 contracts: connection strategy
(one writer, read pool, WAL, busy timeout), checksum-verified transactional
migrations, schemas for transactions, events, streams, projections, jobs,
queue cursors, active partitions, and claim receipts; the transaction
commit algorithm with idempotent resubmission via request digests; and
indeterminate-commit handling with recovery via `recover_transaction`.

**Exit criteria:** database creates and migrates on Linux, macOS, and
Windows; atomic event + projection + enqueue transactions pass; duplicate
transaction behavior passes; stream and projection conflicts pass;
reopening requires no event replay; opening a one-million-event store does
not load all event payloads into memory; strict and relaxed durability
modes documented accurately.

## Phase 3 — Durable job execution (Weeks 3–5)

The claim algorithm (bounded expiry maintenance, round-robin partition
selection, claim receipts), lease extension and heartbeat primitives
(runtime-neutral core, optional Tokio helper), the safe external-effect
worker protocol, and paginated job inspection APIs. Expiry behavior
follows the effect-mode contract: reconciliation-required jobs become
`Uncertain` on lease expiry, never silently retried; only explicitly
idempotent jobs re-enter the retry path.

**Exit criteria:** no executable data is returned for an indeterminate
claim; recovered committed claims contain original lease tokens; absent
claims are distinguishable from persisted claims; maintenance-only progress
is explicit; early-partition replenishment cannot starve later partitions;
out-of-order acknowledgements cannot rewind the cursor; historical
completed partitions do not affect active claim complexity; long-running
jobs can extend leases; worker death results in defined expiry behavior.

## Phase 4 — Operational tooling (Week 5)

CLI command set (doctor, verify, stats, events tail/stream, projections
list/scan/get, jobs list/show/uncertain/resolve, backup,
diagnostic-export, migrations status); live online backup via SQLite's
backup API; integrity, foreign-key, migration-checksum, and semantic
verification; paginated, redacted-by-default diagnostic export; statistics.

**Exit criteria:** backup is safe during writes and restores to a database
that passes integrity checks; verification covers SQLite integrity plus
semantic invariants (stream versions, leases, active partitions, claim
receipts, projection versions); diagnostic export is clearly non-restorable
and excludes lease tokens by default.

## Phase 5 — Synara dogfood integration (Weeks 5–6)

Integrate one provider-turn vertical slice in Synara behind a feature flag:
domain mapping (streams, events, projections, queues, partition keys), one
provider worker with claim recovery and heartbeat, an operator view for
uncertain jobs, a rollback switch, shadow validation with a single effect
executor, and the full set of failure drills.

**Exit criteria:** one provider-turn workflow runs end to end; no duplicate
provider effect occurs in the failure matrix; uncertain outcomes are
visible to the operator; the old orchestration path can be disabled for the
slice; Synara code complexity is measured before and after; every failure
state is explainable without reading SQLite internals.

## Phase 6 — Performance and scalability (Week 7)

Benchmark suite with fully documented environments (CPU, RAM, OS, SQLite
version, filesystem, device, durability mode, DB size, WAL state, cache
state, iterations, median/p95/p99, peak RSS); transaction, scale, and
operational workloads; initial performance budgets; regression tracking on
scheduled CI with gross-regression gates.

**Exit criteria:** all required workloads measured against the documented
reference environment; budgets met or explicitly renegotiated; suspicious
durability results investigated rather than published.

## Phase 7 — Validation, test program, and release (Weeks 7–8)

The full test program: unit, backend contract (file-backed, not only
`:memory:`), model-based, concurrency (barrier-raced with exact typed
outcomes), indeterminate-outcome failpoints, process-crash, fuzzing,
security, and 24–72h soak tests. Then documentation, the repository/crate
naming decision, release notes, and a release candidate.

**Exit criteria (release-blocking):** zero test path executes work from an
indeterminate claim; every job transition is model-tested; claim recovery
works for committed and absent outcomes; stream conflicts are
deterministic; duplicate transaction IDs are content-checked; live backup
restores successfully; active scheduling cost is independent of historical
inactive partitions; Synara passes the full failure drill; docs match
behavior; CI green at the exact release head; the human technical owner
signs off.

## Recommended PR sequence

Small, focused PRs per [PR-RULES.md](./PR-RULES.md), following the plan's
§17 sequence:

1. docs: archive decision — ADR-001, roadmap, scope, PR rules (this PR)
2. semantics: IDs, `CommitBatch`, events, projection patches, job state
   model, safe claim outcomes, backend-independent contract tests
3. storage: SQLite connection, pragmas, and migration framework
4. storage: transactions and events (digests, duplicates, stream versions,
   recovery receipts)
5. storage: projections (metadata, patches, point reads, prefix/range
   pagination, version conflicts)
6. jobs: enqueue and transitions (ack, fail, cancel, resolve, validation)
7. jobs: safe claiming and recovery (active partitions, queue cursor,
   lease tokens, claim receipts, indeterminate recovery,
   maintenance-progress outcome)
8. jobs: lease extension and heartbeat primitives
9. ops: doctor, verify, stats, online backup, diagnostic export
10. synara: provider-turn vertical slice, feature flag, crash drills,
    operator uncertainty view
11. performance and hardening: benchmark suite, RSS measurement, soak
    harness, dependency auditing
12. release preparation: naming decision, README, migration policy,
    release notes, final end-to-end review
