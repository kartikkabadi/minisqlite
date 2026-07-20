mod common;

use std::sync::Mutex;

use common::TempDir;
use minisqlite::{ClaimRequest, CommitBatch, Error, Event, Id, JobSpec, StoreBuilder};

static FAILPOINT_LOCK: Mutex<()> = Mutex::new(());

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
    let result = store.commit(
        CommitBatch::new(poison_tx, 1).append_event(Event::with_json_payload(
            Id::new().unwrap(),
            "s",
            "e",
            1,
            b"{}",
        )),
    );
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
