# Dependencies

| Dependency | Runtime/dev | Why needed | What it replaces | Removal condition |
| ---------- | ----------- | ---------- | -------------- | ----------------- |
| `crc32fast` | runtime | Well-tested CRC32; safer and faster than a hand-rolled checksum | Custom checksum | Never; rolling our own is unnecessary risk |
| `libc` | runtime (Unix/Windows) | Audited platform `O_NOFOLLOW` constant used to atomically reject symlinked primary paths at `open(2)` | Hand-copied `libc::O_NOFOLLOW` integer literals | If Rust exposes `O_NOFOLLOW` in `std` or we move to `rustix` |
| `serde` | optional runtime | Optional derive serialization for application types | Manual serde reimplementation | If `serde`/`json` features are dropped |
| `serde_json` | optional runtime | JSON serialization for CLI and optional `Serialize`/`Deserialize` impls | Manual JSON encoding | If `json` feature is dropped |
| `fastrand` | dev | Deterministic, seedable randomness for property-style and fuzz tests | `proptest`/`rand` | If test strategy changes away from generated inputs |

Runtime dependencies are intentionally small.
No async runtime, ORM, full database, or heavy CLI framework is used.

## Security review

Socket Security previously flagged `cargo/zerocopy`. That transitive dependency was removed by:

* Replacing `proptest`/`tempfile` with `fastrand` and a tiny custom `TempDir` helper, removing
  the `rand`/`getrandom`/`ppv-lite86`/`zerocopy` dev-dependency subtree.
* Removing the `fuzz/` crate's `libfuzzer-sys` build dependency.

`O_NOFOLLOW` is now sourced from the audited `libc` crate instead of a hand-copied constant.
`minisqlite` no longer depends on `zerocopy`.
