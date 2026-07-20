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
