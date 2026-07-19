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
| Memory     | 1,000  | 2.3 ms      | 2.3 ms      | 182 KB    |
| Memory     | 10,000 | 21.4 ms     | 22.5 ms     | 1.82 MB   |
| Memory     | 100,000| 233.9 ms    | 242.0 ms    | 18.2 MB   |
| Strict     | 1,000  | 2.1 ms      | 2.0 ms      | 182 KB    |
| Strict     | 10,000 | 20.0 ms     | 20.5 ms     | 1.82 MB   |
| Strict     | 100,000| 211.2 ms    | 232.1 ms    | 18.2 MB   |

## Decision on snapshots and compaction

Replay of 100,000 events completes in well under one second on the reference hardware.
The file grows linearly with the number of events, which is expected for an append-only
format. Because startup is still fast at this scale, automatic snapshots and compaction
are not implemented in the first version. They can be added after measuring a real
workload that exceeds the acceptable startup budget.
