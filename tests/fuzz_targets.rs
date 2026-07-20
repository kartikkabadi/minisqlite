use std::path::PathBuf;

use minisqlite::codec::frame::{FileHeader, Frame};
use minisqlite::codec::record::decode_records;
use minisqlite::config::Durability;
use minisqlite::storage::file::DataFile;
use minisqlite::storage::recovery;

fn fuzz_bytes(seed: u64, max_len: usize) -> Vec<u8> {
    let mut rng = fastrand::Rng::with_seed(seed);
    let len = rng.usize(0..max_len);
    (0..len).map(|_| rng.u8(..)).collect()
}

#[test]
fn header_decode_never_panics() {
    for seed in 0..1024 {
        let mut bytes = [0u8; 64];
        let data = fuzz_bytes(seed, 64);
        bytes[..data.len()].copy_from_slice(&data);
        let _ = FileHeader::decode(&bytes);
    }
}

#[test]
fn frame_decode_never_panics() {
    for seed in 0..1024 {
        let bytes = fuzz_bytes(seed, 2048);
        let _ = Frame::decode(&bytes);
    }
}

#[test]
fn record_decode_never_panics() {
    for seed in 0..1024 {
        let bytes = fuzz_bytes(seed, 2048);
        let _ = decode_records(&bytes);
    }
}

#[test]
fn recovery_scan_never_panics() {
    for seed in 0..256 {
        let bytes = fuzz_bytes(seed, 4096);
        let id = seed;
        let path = PathBuf::from(format!(
            "/tmp/minisqlite_fuzz_recovery_{}_{}",
            std::process::id(),
            id
        ));
        let _ = std::fs::remove_file(&path);

        if let Ok(mut file) = DataFile::open_or_create(&path, Durability::Memory) {
            let _ = file.append_frame(&bytes, bytes.len() as u64);
            let _ = recovery::scan(&mut file);
        }

        let _ = std::fs::remove_file(&path);
    }
}
