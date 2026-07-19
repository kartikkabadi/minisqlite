# Performance Notes

These are informal, machine-specific measurements from `examples/benchmark.rs` on the
reference environment. They are intended to detect pathological behavior and decide whether
snapshots or compaction are needed, not to make competitive claims.

## Method

`examples/benchmark.rs` opens a store and commits `N` small `Event` records in individual
transactions, then reopens the store to measure replay time. It also measures a 10,000-entry
projection replacement, a prefix scan, and 10,000 job enqueue/claim/ack cycles.

```bash
cargo run --example benchmark --release
```

## Event throughput and replay

| Durability | Events | Commit time | Reopen time | File size |
|------------|--------|-------------|-------------|-----------|
| Memory     | 1,000   | 2.3 ms      | 2.2 ms      | 182 KB    |
| Memory     | 10,000  | 21.3 ms     | 22.3 ms     | 1.82 MB   |
| Memory     | 100,000 | 234.2 ms    | 228.9 ms    | 18.2 MB   |
| Memory     | 1,000,000 | 2.61 s    | 2.65 s      | 182 MB    |
| Strict     | 1,000   | 2.2 ms      | 2.0 ms      | 182 KB    |
| Strict     | 10,000  | 35.1 ms     | 19.8 ms     | 1.82 MB   |
| Strict     | 100,000 | 220.4 ms    | 208.9 ms    | 18.2 MB   |
| Strict     | 1,000,000 | 2.66 s    | 2.67 s      | 182 MB    |

## Projection and job operations

| Operation | Time |
|-----------|------|
| Replace 10,000 projection entries | 6.9 ms |
| Prefix scan (100 matches out of 10,000) | 24 µs |
| Enqueue 10,000 jobs | 20.1 ms |
| Claim and acknowledge 10,000 jobs | 34.7 ms |

## Decision on snapshots and compaction

Replay of 100,000 events completes in well under one second on the reference hardware.
At 1,000,000 events, replay grows to roughly 2.6-2.7 seconds. Projection and job operations
are sub-millisecond to low-double-digit milliseconds. The file grows linearly with the number
of events, which is expected for an append-only format.

Because startup at 100,000 events is still fast and the Synara-shaped reference workload
performs well below the million-event scale in normal use, automatic snapshots and compaction
are not implemented in the first version. They should be added after measuring a real
workload that exceeds the acceptable startup budget.
