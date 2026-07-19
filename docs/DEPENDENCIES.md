# Dependencies

| Dependency | Runtime/dev | Why needed | What it replaces | Removal condition |
| ---------- | ----------- | ---------- | -------------- | ----------------- |
| `crc32fast` | runtime | Well-tested CRC32; safer and faster than a hand-rolled checksum | Custom checksum | Never; rolling our own is unnecessary risk |
| `fs2` | runtime | Cross-platform advisory file locking | OS-specific `flock`/`LockFile` code | Never; the lock surface is small and well-tested |
| `lexopt` | runtime | Minimal, no-panic argument parser for the CLI | `clap` and similar large frameworks | Only if CLI is removed or we hand-roll a simpler parser |
| `serde` | optional runtime | Optional serialization helpers for application types | Manual serde reimplementation | If `serde` feature is dropped |
| `tempfile` | dev | Temporary directories for tests | Manual temp-dir setup | If all tests move to fixed paths |
| `proptest` | dev | Property-based testing for the model-based test suite | Exhaustive hand-written tests | If test strategy changes away from property tests |

Runtime dependencies are intentionally small.
No async runtime, ORM, full database, or heavy CLI framework is used.
