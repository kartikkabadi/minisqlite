# Limitations

## Current alpha

`v0.3.0-alpha.1` is a correctness-focused rewrite. The public API and file format may change.
Do not use it for production data you cannot afford to lose.

## Time semantics

`now_ms` is caller-supplied wall-clock time. Lease expiry, `not_before_ms`, and commit
timestamps are interpreted relative to that value. Large clock jumps or divergent callers
can change job visibility or lease timing. The engine does not provide a distributed clock.

## Concurrency

* Single process owns the store.
* Writes are serialized through an in-process mutex.
* There is no multi-process writer support.
* Readers are in-process and take short locks.

## Durability

* `Strict` mode calls `fsync`, but the actual guarantee depends on the OS, file system, and storage device.
* Power-loss durability beyond that is not claimed.
* No replication or remote backup.

## Queries

* No SQL, no query planner, no indexes.
* Reads are by event sequence, stream, exact projection key, projection prefix scan, or job state.

## Storage

* One primary `.mini` file grows append-only.
* No automatic snapshots or compaction in this version.
* Large binary blobs should not be stored inline.

## Security

* No encryption at rest.
* Checksums detect accidental corruption, not malicious tampering.
* File permissions are set to `0o600` on Unix but are not a complete security boundary.

## Missing (explicit deletions)

* Distributed consensus, replication, multi-process writes.
* Vector search, workflow DSL, dashboard, background scheduler.
* Automatic snapshots/compaction.
* SQL compatibility with previous `minisqlite` versions.
