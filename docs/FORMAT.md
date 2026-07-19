# File Format

## Extension

The primary file uses `.mini`.
The lock file uses `.lock`.

## File header (64 bytes)

Layout, little-endian:

| Offset | Size | Field |
|--------|------|-------|
| 0..8   | 8    | magic: "MINISQL3" |
| 8..10  | 2    | major version |
| 10..12 | 2    | minor version |
| 12..14 | 2    | header length |
| 14..22 | 8    | created_at_ms |
| 22..26 | 4    | flags |
| 26..60 | 34   | reserved |
| 60..64 | 4    | CRC32 of bytes 0..60 |

Opening behavior:

* Wrong magic: `NotMiniSQLite`.
* Unsupported newer major version: fail closed.
* Corrupt checksum: fail closed.
* Legacy MiniSQLite SQL files are not opened as the new format.

## Transaction frame

Each committed batch is one frame:

```text
frame header (64 bytes)
encoded records (variable)
frame trailer (32 bytes)
```

### Frame header (64 bytes)

| Offset | Size | Field |
|--------|------|-------|
| 0..8   | 8    | magic: "MINIFRAM" |
| 8..10  | 2    | frame format version |
| 10..18 | 8    | total frame length |
| 18..26 | 8    | transaction sequence |
| 26..42 | 16   | transaction id |
| 42..50 | 8    | commit_timestamp_ms |
| 50..54 | 4    | record count |
| 54..58 | 4    | payload length |
| 58..60 | 2    | reserved |
| 60..64 | 4    | CRC32 of header bytes 0..60 |

### Frame trailer (32 bytes)

| Offset | Size | Field |
|--------|------|-------|
| 0..8   | 8    | magic: "FRAMETRL" |
| 8..16  | 8    | transaction sequence |
| 16..24 | 8    | total frame length |
| 24..28 | 4    | CRC32 of (header bytes + payload + trailer body excluding checksum) |
| 28..32 | 4    | reserved |

The trailer repeats the sequence and length so recovery can detect torn or misaligned writes and confirm a frame reached the file.

## Record encoding

Each record starts with:

| Field | Size |
|-------|------|
| kind | 1 byte |
| version | 2 bytes |
| flags | 1 byte |
| body length | 4 bytes |
| body | variable |

Record kinds include `Event`, `ProjectionPut`, `ProjectionDelete`, `ProjectionClear`, `ProjectionReplace`, `JobEnqueue`, `JobLease`, `JobAck`, `JobFail`, `JobCancel`, `JobResolve`.
Unknown kernel record kinds are rejected.
Application event types are opaque bytes and may be anything.

## Checksum

CRC32 via `crc32fast`.
Checksums detect accidental corruption; they do not protect against deliberate tampering.

## Size limits

`Limits` provides safe defaults and rejects oversize input before writing.
The defaults are:

* max event payload: 1 MiB
* max metadata: 64 KiB
* max projection key/value: 1 MiB / 4 MiB
* max job payload: 1 MiB
* max records per transaction: 1024
* max transaction frame size: 16 MiB
* max string length: 4096
