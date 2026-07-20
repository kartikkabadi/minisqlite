# Recovery

## Structural vs semantic failures

`storage::recovery::scan` distinguishes two classes of failure:

1. **Structurally incomplete tail** — the file ends in the middle of a frame header,
   a declared payload runs past the file end, the trailer is too short, or the frame
   header cannot be decoded because fewer than `FRAME_HEADER_SIZE` bytes remain.
   In this case the scanner stops at the last complete valid frame and reports
   `ScanResult::tail_truncated = true`. No mid-file frame is ever skipped.

2. **Corrupt complete frame** — a frame whose declared bytes are all present but
   whose magic, header checksum, trailer, frame checksum, record count, or payload
   contents are invalid. This is reported as `Error::Corruption` and aborts the scan.
   `open` and `open_existing` fail closed; the file is not silently truncated.

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
   * Decode records with the hard `MAX_RECORDS_PER_FRAME` ceiling before allocation.
4. Return the scan result (valid prefix, tail-truncated flag, last valid offset).

## Verification

`StoreBuilder::verify()` opens the file read-only, runs every frame through the full
semantic replay path in a transient `StoreInner`, and never modifies bytes on disk.
It returns `Ok(())` only when all frames are structurally and semantically valid and
end at a clean frame boundary. A structurally torn tail is reported as
`Error::StoreNeedsRepair`; any other error (bad checksum, bad trailer, invalid
records, or replay invariant violation) is reported as `Error::Corruption` carrying
the offset of the offending frame.

`Store::verify()` does the same on an already-open store. It fails immediately with
`Error::StoreNeedsRepair` if the store was opened with an un-repaired tail.

## Incomplete final frame

If the final frame is structurally incomplete, the scanner stops at the last complete
valid frame and records `recovered_tail = true`.

* `open` truncates the torn tail automatically so the store can accept writes immediately.
* `open_existing` leaves the forensic tail on disk and sets `needs_repair`; writes are
  blocked until `Store::repair()` is called explicitly. This separates read-only
  verification from repair.

No earlier frame is affected because the format is append-only and frames are self-contained.

## Mid-file corruption

If a complete frame in the middle of the file fails structural or semantic validation,
the scanner returns `Error::Corruption`. We do not silently skip committed state that may
have been tampered with or truncated by a tool that wrote past a frame boundary.

## Replay

During verification and open, `StoreInner::replay_frame` applies each record to memory
and validates immutable invariants (non-zero IDs, `max_attempts > 0`, lease-token non-zero,
attempt sequence, `lease_expires_at_ms > claimed_at_ms`) before state is updated. All
validation, reconstruction, and regeneration errors are wrapped as `Error::Corruption`
carrying the offending frame offset.

* Events are appended with global sequence and stream version.
* Projection operations update the in-memory `BTreeMap`.
* Job operations update job state, lease tokens, and queue-partition ordering.

The file is the source of truth; memory is a derived view. Committed frames are decoded
using the hard frame-size bound, so replay does not reject older records just because the
configured `Limits` have changed.

## Durability modes

* `Strict`: `fsync` after each append before returning success.
* `Memory`: no `fsync`; intended for tests and ephemeral instances.

`Strict` is the default. `Memory` must be chosen explicitly.

## Uncertain commits and repairs

If an append or sync fails, the store attempts to truncate the file back to its original
length. If the truncate is confirmed, the commit is definitely failed. If the truncate
cannot be confirmed, the store is poisoned and `CommitOutcomeUncertain` is returned.

Likewise, if `DataFile::truncate` is called during repair and `fsync` fails, it returns
`RepairOutcomeUncertain` containing the requested and actual file length. The application
must reopen to discover the durable state.
