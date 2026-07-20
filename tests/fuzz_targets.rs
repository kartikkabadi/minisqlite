#![cfg(feature = "fuzzing")]

use std::path::PathBuf;

use minisqlite::codec::frame::{FileHeader, Frame};
use minisqlite::codec::record::decode_records;
use minisqlite::config::Durability;
use minisqlite::storage::file::DataFile;
use minisqlite::storage::recovery;

mod common;

fn fuzz_bytes(seed: u64, max_len: usize) -> Vec<u8> {
    let mut rng = fastrand::Rng::with_seed(seed);
    let len = rng.usize(0..max_len);
    (0..len).map(|_| rng.u8(..)).collect()
}

fn mutate_bytes(seed: u64, base: &[u8]) -> Vec<u8> {
    let mut rng = fastrand::Rng::with_seed(seed);
    let mut out = base.to_vec();
    if out.is_empty() {
        return fuzz_bytes(seed, 256);
    }
    let mutations = rng.usize(1..=8);
    for _ in 0..mutations {
        match rng.usize(0..4) {
            0 => {
                let i = rng.usize(0..out.len());
                out[i] = rng.u8(..);
            }
            1 => {
                let i = rng.usize(0..out.len());
                out.insert(i, rng.u8(..));
            }
            2 => {
                if out.len() > 1 {
                    let i = rng.usize(0..out.len());
                    out.remove(i);
                }
            }
            _ => {
                let i = rng.usize(0..out.len());
                let j = rng.usize(0..out.len());
                out.swap(i, j);
            }
        }
    }
    out
}

#[test]
fn header_decode_never_panics() {
    let base = FileHeader::new(0).encode().to_vec();
    for seed in 0..1024 {
        let data = fuzz_bytes(seed, 64);
        let mut bytes = [0u8; 64];
        bytes[..data.len().min(64)].copy_from_slice(&data[..data.len().min(64)]);
        let _ = FileHeader::decode(&bytes);

        let mutated = mutate_bytes(seed + 10_000, &base);
        let mut bytes = [0u8; 64];
        bytes[..mutated.len().min(64)].copy_from_slice(&mutated[..mutated.len().min(64)]);
        let _ = FileHeader::decode(&bytes);
    }
}

#[test]
fn frame_decode_never_panics() {
    for seed in 0..1024 {
        let raw = fuzz_bytes(seed, 2048);
        let _ = Frame::decode(&raw);

        let mut header = FileHeader::new(0).encode().to_vec();
        header.extend_from_slice(&raw);
        let _ = Frame::decode(&header);
    }
}

#[test]
fn record_decode_never_panics() {
    for seed in 0..1024 {
        let bytes = fuzz_bytes(seed, 2048);
        let _ = decode_records(&bytes);

        // Mutate a valid roundtrip payload.
        let base = minisqlite::codec::record::encode_records(&[]);
        let mutated = mutate_bytes(seed + 20_000, &base);
        let _ = decode_records(&mutated);
    }
}

#[test]
fn recovery_scan_never_panics() {
    let tmp = common::TempDir::new();
    for seed in 0..256 {
        let path: PathBuf = tmp.path().join(format!("fuzz_recovery_{seed}.mini"));
        let bytes = fuzz_bytes(seed, 4096);
        let _ = std::fs::remove_file(&path);

        let mut file = DataFile::open_or_create(&path, Durability::Memory, false).unwrap();
        let _ = file.append_frame(&bytes, bytes.len() as u64);
        let _ = recovery::scan(&mut file);
        let _ = std::fs::remove_file(&path);
    }
}
