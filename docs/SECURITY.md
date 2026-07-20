# Security

## Threat model

MiniSQLite is designed for local, single-process, single-owner applications.
The trusted boundary is the owning process and the local file system.

## What checksums do and do not protect

CRC32 checksums detect accidental bitrot, torn writes, and truncated files.
They do not protect against deliberate malicious modification.
An attacker with write access to the `.mini` file can construct valid frames that pass checksum validation.

## File permissions

On Unix, the primary data file is created with mode `0o600` (owner read/write only).
There is no separate lock file; the advisory lock is held directly on the primary file.
This reduces the risk of other users reading or modifying the file, but it is not encryption.

If the containing directory is created by the store, it is set to `0o700` so only the owner can
list or access its contents.

## Symlink handling

Opening the primary data file uses `O_NOFOLLOW` (via the audited `libc` crate on Unix and
`FILE_FLAG_OPEN_REPARSE_POINT` on Windows) so an existing symlink is rejected. This avoids
accidentally writing through a symlink placed by another user.

`Store::backup` copies the durable valid prefix to a temporary sibling file, validates the
temporary copy, and then atomically publishes it with `hard_link` + `remove_file`. An existing
destination is refused, a dangling symlink cannot overwrite a real file, and post-link or
post-publication / parent-sync failures are reported as `BackupOutcomeUncertain` so the caller
knows the destination may already exist and must be verified before use.

## Payload privacy

Event, projection, and job payloads are stored as opaque bytes.
They are not encrypted at rest.
An attacker with access to the file can read all stored data.

## Corruption behavior

Mid-file corruption is treated as a hard error.
The store refuses to open so the operator can investigate rather than silently using a possibly-invalid state.

## Dependency security review

A Socket Security scan of PR #9 previously reported a `Warn` alert for `cargo/zerocopy`. That
transitive dependency was removed by replacing `proptest`/`tempfile` and the `libfuzzer-sys`
fuzz crate with `fastrand` and a small custom `TempDir` helper. The `Cargo.lock` no longer
contains `zerocopy`.

`O_NOFOLLOW` is sourced from the audited `libc` crate rather than hand-copied constants.

## Known limitations

* No encryption at rest.
* No integrity protection against malicious modification.
* No multi-user access controls beyond file-system permissions.
* No network exposure by design.
