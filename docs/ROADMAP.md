# Roadmap: SQLite-backed control-plane kernel

An 8-week plan covering Phases 0–7. Tracking: Issue #10. Storage decision:
[ADR-001](./ADR-001.md). Scope boundaries: [SCOPE.md](./SCOPE.md).

## Phase 0 — Governance & decision records (Week 1)

Establish the ADR, roadmap, scope, and PR rules before any implementation.

**Exit criteria:** ADR-001, ROADMAP, SCOPE, and PR-RULES merged to `main`;
PR #9 archived and closed; Issue #10 open as the tracking issue.

## Phase 1 — Storage foundation (Week 1–2)

SQLite integration: connection management, WAL configuration, schema
migrations, and a private internal storage module with a narrow API.

**Exit criteria:** database opens/migrates idempotently; crash-safety pragmas
set and tested; storage module has no public API surface.

## Phase 2 — Event log (Week 2–3)

Typed, append-only event log: event envelope (id, sequence, timestamp, type,
payload), append and ordered-read operations, monotonic sequence invariant.

**Exit criteria:** append is atomic; sequence gaps impossible under test;
property tests cover ordering and durability across simulated restarts.

## Phase 3 — Projections (Week 3–4)

Deterministic projections derived from the event log, with tracked apply
positions and full rebuild-from-scratch support.

**Exit criteria:** rebuilding any projection from the log reproduces identical
state; projection position never exceeds the log head; rebuild is tested.

## Phase 4 — Durable jobs (Week 4–5)

Durable job queue: enqueue, claim, complete/fail transitions, retry with
backoff, at-least-once semantics.

**Exit criteria:** jobs survive process restart; state transitions are
transactional with event appends; no job is lost or double-completed in tests.

## Phase 5 — Kernel API (Week 5–6)

The typed public kernel API tying events, projections, and jobs together;
explicit representation of uncertainty (in-flight, unacknowledged, rebuilding).

**Exit criteria:** a consumer can drive the full event → projection → job loop
through the public API only; API documented with examples.

## Phase 6 — Hardening (Week 6–7)

Crash-injection and property testing, fuzzing of the public surface,
performance baselines for control-plane workloads, integrity checks.

**Exit criteria:** crash-injection suite green; documented performance
baseline; `PRAGMA integrity_check` clean after all torture tests.

## Phase 7 — Release (Week 7–8)

Documentation, examples, changelog, versioning, and a tagged release.

**Exit criteria:** docs.rs builds clean; example programs run; release tagged
and published.

## Recommended PR sequence

Small, focused PRs per [PR-RULES.md](./PR-RULES.md):

1. docs: ADR-001, roadmap, scope, PR rules (this PR)
2. storage: SQLite connection + pragmas
3. storage: schema migrations
4. events: envelope types + append
5. events: ordered reads + sequence invariant tests
6. projections: position tracking + apply loop
7. projections: rebuild support + determinism tests
8. jobs: schema + enqueue/claim
9. jobs: completion, retry, backoff
10. kernel: public API + uncertainty states
11. hardening: crash-injection + property tests
12. release: docs, examples, changelog, tag
