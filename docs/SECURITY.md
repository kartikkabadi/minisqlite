# Security

## Threat model

MiniSQLite is designed for local, single-process, single-owner applications.
The trusted boundary is the owning process and the local file system.

## What checksums do and do not protect

CRC32 checksums detect accidental bitrot, torn writes, and truncated files.
They do not protect against deliberate malicious modification.
An attacker with write access to the `.mini` file can construct valid frames that pass checksum validation.

## File permissions

On Unix, the primary data file and lock file are created with mode `0o600` (owner read/write only).
This reduces the risk of other users reading or modifying the file, but it is not encryption.

If the containing directory is created by the store, it is set to `0o700` so only the owner can
list or access its contents.

## Symlink handling

Opening the primary data file will fail if the path is an existing symlink. This avoids
accidentally writing through a symlink placed by another user.

## Payload privacy

Event, projection, and job payloads are stored as opaque bytes.
They are not encrypted at rest.
An attacker with access to the file can read all stored data.

## Corruption behavior

Mid-file corruption is treated as a hard error.
The store refuses to open so the operator can investigate rather than silently using a possibly-invalid state.

## Known limitations

* No encryption at rest.
* No integrity protection against malicious modification.
* No multi-user access controls beyond file-system permissions.
* No network exposure by design.
