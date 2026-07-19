# Performance Notes

These are informal, machine-specific measurements from `examples/benchmark.rs` on the
reference environment. They are intended to detect pathological behavior and decide whether
snapshots or compaction are needed, not to make competitive claims.

## Method

`examples/benchmark.rs` opens a store and commits `N` small `Event` records in individual
transactions, then reopens the store to measure replay time.

```bash
cargo run --example benchmark --release
```

## Results

| Durability | Events | Commit time | Reopen time | File size |
|------------|--------|-------------|-------------|-----------|
| Memory     | 1,000   | 2.3 ms      | 2.3 ms      | 182 KB    |
| Memory     | 10,000  | 21.7 ms     | 22.4 ms     | 1.82 MB   |
| Memory     | 100,000 | 223.6 ms    | 241.7 ms    | 18.2 MB   |
| Memory     | 1,000,000 | 2.68 s    | 2.70 s      | 182 MB    |
| Strict     | 1,000   | 2.1 ms      | 2.0 ms      | 182 KB    |
| Strict     | 10,000  | 20.6 ms     | 20.0 ms     | 1.82 MB   |
| Strict     | 100,000 | 218.2 ms    | 203.7 ms    | 18.2 MB   |
| Strict     | 1,000,000 | 2.63 s    | 2.71 s      | 182 MB    |

## Decision on snapshots and compaction

Replay of 100,000 events completes in well under one second on the reference hardware.
At 1,000,000 events, replay grows to roughly 2.6-2.7 seconds. The file grows linearly
with the number of events, which is expected for an append-only format.

Because startup at 100,000 events is still fast and the Synara-shaped reference workload
performs well below the million-event scale in normal use, automatic snapshots and compaction
are not implemented in the first version. They should be added after measuring a real
workload that exceeds the acceptable startup budget.
