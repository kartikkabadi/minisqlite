#![no_main]

use libfuzzer_sys::fuzz_target;
use minisqlite::codec::frame::FileHeader;

fuzz_target!(|data: &[u8]| {
    let mut bytes = [0u8; 64];
    let n = data.len().min(64);
    bytes[..n].copy_from_slice(&data[..n]);
    let _ = FileHeader::decode(&bytes);
});
