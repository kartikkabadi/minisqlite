#![no_main]

use libfuzzer_sys::fuzz_target;
use minisqlite::codec::frame::Frame;

fuzz_target!(|data: &[u8]| {
    let _ = Frame::decode(data);
});
