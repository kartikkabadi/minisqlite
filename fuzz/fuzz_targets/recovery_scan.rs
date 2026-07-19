#![no_main]

use std::path::PathBuf;
use std::sync::atomic::{AtomicUsize, Ordering};

use libfuzzer_sys::fuzz_target;
use minisqlite::config::Durability;
use minisqlite::storage::file::DataFile;
use minisqlite::storage::recovery;

static COUNTER: AtomicUsize = AtomicUsize::new(0);

fuzz_target!(|data: &[u8]| {
    let id = COUNTER.fetch_add(1, Ordering::Relaxed);
    let path = PathBuf::from(format!("/tmp/minisqlite_fuzz_recovery_{}_{}", std::process::id(), id));
    let _ = std::fs::remove_file(&path);

    if let Ok(mut file) = DataFile::open_or_create(&path, Durability::Memory) {
        let _ = file.append_frame(data, data.len() as u64);
        let _ = recovery::scan(&mut file);
    }

    let _ = std::fs::remove_file(&path);
});
