#![no_main]

use libfuzzer_sys::fuzz_target;
use minisqlite::codec::record::decode_records;

fuzz_target!(|data: &[u8]| {
    let _ = decode_records(data);
});
