# Recovery

## Scanner behavior

Reopening a store invokes `storage::recovery::scan`:

1. Read and validate the 64-byte file header.
2. Walk frames sequentially starting at offset 64.
3. For each frame:
   * Check frame header magic.
   * Verify header checksum.
   * Read the declared payload and trailer.
   * Verify trailer magic and frame checksum.
   * Verify trailer sequence and length match the header.
4. Return a vector of validated frames.

## Incomplete final frame

If the final frame is shorter than its declared length, has a bad checksum, or has a torn trailer, the scanner stops at the last complete valid frame and records `recovered_tail = true`.
The store opens with the valid prefix.

No earlier frame is affected because the format is append-only and frames are self-contained.

## Mid-file corruption

If a frame in the middle of the file fails validation, the scanner returns `Corruption` and opening fails.
This is intentional: we do not silently skip committed state that may have been tampered with or truncated by a tool that wrote past a frame boundary.

## Replay

After scanning, `StoreInner::replay_frame` applies each record to memory:

* Events are appended with global sequence and stream version.
* Projection operations update the in-memory `BTreeMap`.
* Job operations update job state, lease tokens, and queue-partition ordering.

The file is the source of truth; memory is a derived view.
Committed frames are decoded using the hard frame-size bound, so replay does not
reject older records just because the configured `Limits` have changed.

## Durability modes

* `Strict`: `fsync` after each append before returning success.
* `Memory`: no `fsync`; intended for tests and ephemeral instances.

`Strict` is the default. `Memory` must be chosen explicitly.

## Uncertain commits

If an append or sync fails, the store attempts to truncate the file back to its original length.
If the truncate is confirmed, the commit is definitely failed.
If the truncate cannot be confirmed, the store is poisoned and `CommitOutcomeUncertain` is returned.
The application can reopen, query the transaction ID, and decide whether to retry.
