# Dependencies

| Dependency | Runtime/dev | Why needed | What it replaces | Removal condition |
| ---------- | ----------- | ---------- | -------------- | ----------------- |
| `crc32fast` | runtime | Well-tested CRC32; safer and faster than a hand-rolled checksum | Custom checksum | Never; rolling our own is unnecessary risk |
| `serde` | optional runtime | Optional derive serialization for application types | Manual serde reimplementation | If `serde`/`json` features are dropped |
| `serde_json` | optional runtime | JSON serialization for CLI and optional `Serialize`/`Deserialize` impls | Manual JSON encoding | If `json` feature is dropped |
| `fastrand` | dev | Deterministic, seedable randomness for property-style and fuzz tests | `proptest`/`rand` | If test strategy changes away from generated inputs |

Runtime dependencies are intentionally small.
No async runtime, ORM, full database, or heavy CLI framework is used.

## Security review

The Socket Security alerts for `cargo/libc` and `cargo/zerocopy` were resolved by:

* Replacing `fs2` advisory locking with `std::fs::File::lock`/`try_lock` (stable since Rust 1.89),
  which removed the runtime `libc` dependency.
* Replacing `proptest`/`tempfile` with `fastrand` and a tiny custom `TempDir` helper, removing
  the `rand`/`getrandom`/`ppv-lite86`/`zerocopy` dev-dependency subtree.
* Removing the `fuzz/` crate's `libfuzzer-sys` build dependency (which pulled `libc` via
  `cc`/`jobserver`) and folding the same coverage into deterministic `#[test]` fuzz targets
  in `tests/fuzz_targets.rs`.

`minisqlite` no longer depends on `libc` or `zerocopy`.
