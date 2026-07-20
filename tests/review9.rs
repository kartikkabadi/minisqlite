mod common;

use std::sync::Mutex;

use common::TempDir;
use minisqlite::{ClaimRequest, CommitBatch, Error, Event, Id, JobSpec, StoreBuilder};

static FAILPOINT_LOCK: Mutex<()> = Mutex::new(());

/// A one-key mutation of a large projection must not deep-clone the projection.
/// The child builds a ~512 MiB projection, then caps its own address space
/// before performing single-key put/delete commits; a full-state copy during
/// validation would exceed the cap and abort.
#[cfg(unix)]
mod constrained_projection {
    use super::*;
    use minisqlite::Durability;
    use std::process::Command;

    const CHILD_ENV: &str = "REVIEW9_PROJECTION_CHILD";
    const VALUE_LEN: usize = 4 * 1024 * 1024 - 1024;
    const ENTRY_COUNT: usize = 128;
    // ~512 MiB of projection state; the cap leaves no room for a second copy.
    const CHILD_RLIMIT_AS_BYTES: u64 = 900 << 20;

    fn limit_address_space(bytes: u64) {
        let lim = libc::rlimit {
            rlim_cur: bytes,
            rlim_max: bytes,
        };
        let rc = unsafe { libc::setrlimit(libc::RLIMIT_AS, &lim) };
        assert_eq!(rc, 0, "setrlimit(RLIMIT_AS) failed");
    }

    #[test]
    #[ignore]
    fn child_entry() {
        if std::env::var(CHILD_ENV).is_err() {
            return;
        }
        let tmp = TempDir::new();
        let store = StoreBuilder::new(tmp.path().join("large_projection.mini"))
            .durability(Durability::Memory)
            .open()
            .unwrap();
        for i in 0..ENTRY_COUNT {
            store
                .commit(CommitBatch::new(Id::new().unwrap(), 0).projection_put(
                    "big",
                    i as u64 + 1,
                    format!("key-{i:04}").into_bytes(),
                    vec![0xCD; VALUE_LEN],
                ))
                .unwrap();
        }

        limit_address_space(CHILD_RLIMIT_AS_BYTES);

        // Single-key mutations against the large projection must succeed within
        // the cap: no deep clone of the current state.
        let version = ENTRY_COUNT as u64;
        store
            .commit(CommitBatch::new(Id::new().unwrap(), 1).projection_put(
                "big",
                version + 1,
                b"key-0000".to_vec(),
                b"small".to_vec(),
            ))
            .unwrap();
        store
            .commit(CommitBatch::new(Id::new().unwrap(), 2).projection_delete(
                "big",
                version + 2,
                b"key-0001".to_vec(),
            ))
            .unwrap();
        assert_eq!(
            store.get_projection("big", b"key-0000").unwrap().unwrap(),
            b"small".to_vec()
        );
    }

    #[test]
    fn constrained_rss_single_key_mutation_on_large_projection() {
        let exe = std::env::current_exe().unwrap();
        let output = Command::new(exe)
            .env(CHILD_ENV, "1")
            .args(["--exact", "constrained_projection::child_entry"])
            .args(["--ignored", "--nocapture"])
            .output()
            .expect("failed to spawn child test process");
        assert!(
            output.status.success(),
            "single-key mutation under RLIMIT_AS failed: status={:?}\nstdout: {}\nstderr: {}",
            output.status,
            String::from_utf8_lossy(&output.stdout),
            String::from_utf8_lossy(&output.stderr),
        );
    }
}

/// Verification takes a shared lock, so it can never scan a live writer's
/// half-appended frame and misdiagnose it as repairable corruption.
mod verify_stability {
    use super::*;

    #[test]
    fn verify_refuses_store_owned_by_a_writer() {
        let tmp = TempDir::new();
        let path = tmp.path().join("verify_locked.mini");
        let store = StoreBuilder::new(&path).open().unwrap();
        store
            .commit(
                CommitBatch::new(Id::new().unwrap(), 0).append_event(Event::with_json_payload(
                    Id::new().unwrap(),
                    "s",
                    "e",
                    0,
                    b"{}",
                )),
            )
            .unwrap();

        let result = StoreBuilder::new(&path).verify();
        assert!(
            matches!(result, Err(Error::AlreadyOpen)),
            "verify must refuse a store owned by an exclusive writer, got {result:?}"
        );

        drop(store);
        StoreBuilder::new(&path).verify().unwrap();
    }

    /// Process-level proof: pause a writer mid-append with a torn frame on disk,
    /// then confirm verify reports the live writer instead of repairable corruption.
    #[cfg(feature = "failpoint")]
    #[test]
    fn verify_never_labels_live_in_progress_commit_as_repairable() {
        let _guard = FAILPOINT_LOCK.lock().unwrap();
        let tmp = TempDir::new();
        let path = tmp.path().join("verify_pause.mini");
        let signal = tmp.path().join("paused.signal");
        let release = tmp.path().join("paused.release");

        let driver = std::env::var("CARGO_BIN_EXE_crash_driver")
            .map(std::path::PathBuf::from)
            .unwrap_or_else(|_| std::path::PathBuf::from("target/debug/crash_driver"));
        let mut child = std::process::Command::new(driver)
            .arg(&path)
            .arg("pause-during-append")
            .env("MINISQLITE_PAUSE_SIGNAL_FILE", &signal)
            .env("MINISQLITE_PAUSE_RELEASE_FILE", &release)
            .spawn()
            .expect("failed to spawn crash driver");

        // Wait until the writer is paused with a partial frame on disk.
        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(30);
        while !signal.exists() {
            assert!(
                std::time::Instant::now() < deadline,
                "writer never reached the pause failpoint"
            );
            std::thread::sleep(std::time::Duration::from_millis(10));
        }

        // The file now ends in a half-written frame, but the writer is alive and
        // holds the exclusive lock: verify must refuse, not report StoreNeedsRepair.
        let result = StoreBuilder::new(&path).verify();
        assert!(
            matches!(result, Err(Error::AlreadyOpen)),
            "verify must not diagnose a live in-progress commit, got {result:?}"
        );

        // Release the writer, let it finish the append, and verify a clean store.
        std::fs::write(&release, b"go").unwrap();
        let status = child.wait().unwrap();
        assert!(status.success(), "writer failed to complete after release");
        StoreBuilder::new(&path).verify().unwrap();
    }
}

#[cfg(feature = "failpoint")]
#[test]
fn store_poisoned_reports_original_poisoning_transaction_id() {
    let _guard = FAILPOINT_LOCK.lock().unwrap();
    let tmp = TempDir::new();
    let path = tmp.path().join("poison_id.mini");

    let job_id = Id::new().unwrap();
    let store = StoreBuilder::new(&path).open().unwrap();
    store
        .commit(
            CommitBatch::new(Id::new().unwrap(), 0).enqueue_job(JobSpec::new(
                job_id,
                "q",
                "p",
                b"payload".to_vec(),
            )),
        )
        .unwrap();

    // Transaction A becomes uncertain and poisons the store.
    let poison_tx = Id::new().unwrap();
    std::env::set_var("MINISQLITE_FAILPOINT", "commit-uncertain");
    let result = store.commit(CommitBatch::new(poison_tx, 1).append_event(
        Event::with_json_payload(Id::new().unwrap(), "s", "e", 1, b"{}"),
    ));
    std::env::remove_var("MINISQLITE_FAILPOINT");
    assert!(matches!(
        result,
        Err(Error::CommitOutcomeUncertain { transaction_id, .. }) if transaction_id == poison_tx
    ));

    // Every subsequent operation must identify transaction A, not its own id.
    let commit_b = store.commit(CommitBatch::new(Id::new().unwrap(), 2).append_event(
        Event::with_json_payload(Id::new().unwrap(), "s", "e", 2, b"{}"),
    ));
    assert!(matches!(
        commit_b,
        Err(Error::StorePoisoned { transaction_id }) if transaction_id == poison_tx
    ));

    let claim = store.claim_jobs(ClaimRequest {
        queue: "q".into(),
        worker_id: "w".into(),
        now_ms: 2,
        lease_ms: 1000,
        limit: 1,
    });
    assert!(matches!(
        claim,
        Err(Error::StorePoisoned { transaction_id }) if transaction_id == poison_tx
    ));

    let ack = store.ack_job(job_id, Id::new().unwrap(), None, 2);
    assert!(matches!(
        ack,
        Err(Error::StorePoisoned { transaction_id }) if transaction_id == poison_tx
    ));

    let backup = store.backup(tmp.path().join("poison_backup.mini"));
    assert!(matches!(
        backup,
        Err(Error::StorePoisoned { transaction_id }) if transaction_id == poison_tx
    ));
}
