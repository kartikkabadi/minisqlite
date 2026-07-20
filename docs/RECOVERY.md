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
   * Bound decoded-record memory by measured in-memory cost: `MAX_RECORD_MEMORY`
     (96 MiB) per record and `MAX_TRANSACTION_MEMORY` (256 MiB) per frame, so a
     valid on-disk frame cannot amplify into unbounded heap usage during replay.
   * All decode-path allocations are fallible (`try_reserve`/`try_reserve_exact`);
     allocation failure is a typed error, never an abort.
   * Frame lengths are validated as `u64` against `MAX_FRAME_SIZE` before any
     `usize` conversion, with checked offset arithmetic.
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

Both verification paths observe a stable snapshot of the file, so verifying while a
writer is actively appending cannot report spurious corruption from a partially
written frame.

## Incomplete final frame

If the final frame is structurally incomplete, the scanner stops at the last complete
valid frame and records `recovered_tail = true`.

* `open` truncates the torn tail automatically so the store can accept writes immediately.
* `open_existing` leaves the forensic tail on disk and sets `needs_repair`; writes are
  blocked until `Store::repair()` is called explicitly. This separates read-only
  verification from repair.
* The `minisqlite repair <database>` CLI command performs the explicit repair. It
  reports the current file length, the last valid offset, and the bytes removed;
  `--force` skips the confirmation prompt and JSON output is available. A clean file
  is a no-op, an uncertain truncation surfaces as `RepairOutcomeUncertain`, and
  complete-frame corruption is refused (fail closed), never "repaired" away.

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
* Job operations update job state, lease tokens, queue-partition ordering, and the
  durable round-robin structures (per-queue cursors and per-partition head indexes),
  so claim fairness is identical after reopen.

The file is the source of truth; memory is a derived view. Committed frames are decoded
using the hard frame-size bound, so replay does not reject older records just because the
configured `Limits` have changed.

## Durability modes

* `Strict`: `fsync` after each append before returning success.
* `Memory`: no `fsync`; intended for tests and ephemeral instances.

`Strict` is the default. `Memory` must be chosen explicitly.

## Uncertain commits, claims, repairs, and backups

If an append or sync fails, the store attempts to truncate the file back to its original
length. If the truncate is confirmed, the commit is definitely failed. If the truncate
cannot be confirmed, the store is poisoned and `CommitOutcomeUncertain` is returned.
The store records the transaction ID that caused the poisoning; every subsequent
`StorePoisoned` error reports that original ID, not the ID of the rejected write.

When the internal commit of `Store::claim_jobs` returns `CommitOutcomeUncertain`, the public
API returns `ClaimOutcome::Uncertain { transaction_id, claims }`. The caller receives the
proposed transaction ID and the `ClaimedJob` values, including the lease tokens. Reopening the
store reveals whether the frame was durably written; if it was, the lease tokens are valid
and can be used to acknowledge or fail the jobs.

Likewise, if `DataFile::truncate` is called during repair and `fsync` fails, it returns
`RepairOutcomeUncertain` containing the requested and actual file length. The application
must reopen to discover the durable state. This path is exercised under `Durability::Strict`
so a memory-only configuration does not mask the uncertainty.

`Store::backup` uses an atomic `hard_link` + `remove_file` publication. If the link or rename
succeeds but a later stage (parent-directory sync, or a simulated failpoint immediately after
link or publication) cannot be confirmed, the operation returns `BackupOutcomeUncertain`. The
destination may already exist and be a valid copy of the durable prefix; the caller must
reopen or verify the backup before relying on it.
