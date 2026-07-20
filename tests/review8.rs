mod common;

use std::sync::Mutex;

use common::TempDir;
use minisqlite::{
    ClaimRequest, CommitBatch, Durability, EffectMode, Event, Id, JobSpec, JobState, Limits,
    ProjectionEntry, StoreBuilder,
};

static FAILPOINT_LOCK: Mutex<()> = Mutex::new(());

#[test]
fn max_replace_entries_is_capped_to_hard_format_ceiling() {
    let _guard = FAILPOINT_LOCK.lock().unwrap();
    let tmp = TempDir::new();
    let path = tmp.path().join("limit.mini");
    let limits = Limits {
        max_replace_entries: 1_000_001,
        ..Default::default()
    };
    let result = StoreBuilder::new(&path).limits(limits).open();
    let err = result.map(|_| ()).unwrap_err();
    assert!(
        matches!(err, minisqlite::Error::Validation(_)),
        "Limits must reject max_replace_entries above the hard ceiling: {err:?}"
    );
}

#[cfg(feature = "fuzzing")]
#[test]
fn projection_replace_decoder_rejects_overlarge_entry_count() {
    let _guard = FAILPOINT_LOCK.lock().unwrap();
    use minisqlite::codec::frame::{FileHeader, Frame, FrameHeader};
    use minisqlite::codec::record::{
        decode_records, MAX_REPLACE_ENTRIES_PER_RECORD, PROJECTION_REPLACE, RECORD_FORMAT_VERSION,
    };

    let tmp = TempDir::new();
    let path = tmp.path().join("projection_replace.mini");

    // Build a valid 64 MiB frame whose single ProjectionReplace record claims
    // MAX_REPLACE_ENTRIES_PER_RECORD + 1 entries. The decoder must reject the count
    // before allocating the entry vector.
    let max_payload = minisqlite::codec::frame::MAX_FRAME_SIZE
        - minisqlite::codec::frame::FRAME_HEADER_SIZE
        - minisqlite::codec::frame::FRAME_TRAILER_SIZE;
    let body_len = (max_payload - 7) as u32; // record header is 7 bytes
    let count = MAX_REPLACE_ENTRIES_PER_RECORD + 1;

    let mut payload = Vec::with_capacity(max_payload);
    payload.push(PROJECTION_REPLACE);
    payload.push(RECORD_FORMAT_VERSION);
    payload.push(0); // flags
    payload.extend_from_slice(&body_len.to_le_bytes());
    payload.extend_from_slice(&0u32.to_le_bytes()); // projection length
    payload.extend_from_slice(&1u64.to_le_bytes()); // new_version
    payload.extend_from_slice(&count.to_le_bytes());
    payload.resize(max_payload, 0); // the rest is ignored padding

    let header = FrameHeader {
        version: minisqlite::codec::frame::FRAME_FORMAT_VERSION,
        total_frame_length: 0,
        transaction_sequence: 1,
        transaction_id: Id::new().unwrap(),
        commit_timestamp_ms: 0,
        record_count: 1,
        payload_length: payload.len() as u32,
    };
    let frame = Frame::new(header, payload);
    let bytes = frame.encode();
    assert_eq!(bytes.len(), minisqlite::codec::frame::MAX_FRAME_SIZE);

    let decoded = Frame::decode(&bytes).unwrap();
    let result = decode_records(&decoded.payload, decoded.header.record_count);
    assert!(
        matches!(result, Err(minisqlite::Error::Corruption { .. })),
        "decoder must reject a projection-replace record whose entry count exceeds the hard ceiling: {result:?}"
    );

    // The fixture file should also fail recovery. Prepend a valid file header so the
    // recovery scanner reaches the bad frame.
    let mut file_bytes = FileHeader::new(0).encode().to_vec();
    file_bytes.extend_from_slice(&bytes);
    std::fs::write(&path, &file_bytes).unwrap();
    let open = StoreBuilder::new(&path)
        .durability(Durability::Memory)
        .open();
    let open = open.map(|_| ());
    assert!(
        matches!(open, Err(minisqlite::Error::Corruption { .. })),
        "recovery must reject the overlarge replace record: {open:?}"
    );
}

#[cfg(feature = "failpoint")]
#[test]
fn claim_jobs_uncertain_returns_proposed_claims_and_recoverable_tokens() {
    let _guard = FAILPOINT_LOCK.lock().unwrap();
    let tmp = TempDir::new();
    let path = tmp.path().join("claim_uncertain.mini");

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
    drop(store);

    std::env::set_var("MINISQLITE_FAILPOINT", "commit-uncertain");
    let store = StoreBuilder::new(&path).open().unwrap();
    let outcome = store
        .claim_jobs(ClaimRequest {
            queue: "q".into(),
            worker_id: "w".into(),
            now_ms: 0,
            lease_ms: 1000,
            limit: 1,
        })
        .unwrap();
    std::env::remove_var("MINISQLITE_FAILPOINT");

    assert!(
        outcome.is_uncertain(),
        "claim_jobs must report uncertain commit"
    );
    let claims = outcome.claims();
    assert_eq!(claims.len(), 1);
    let token = claims[0].lease_token;
    assert_ne!(token, Id::ZERO);

    // The store is poisoned; reopen to recover durable state.
    drop(store);
    let store = StoreBuilder::new(&path).open().unwrap();
    let jobs = store.jobs(0, Some("q".into()), None);
    let job = jobs.iter().find(|j| j.job_id == job_id).unwrap();
    assert_eq!(job.state, JobState::Leased);
    assert_eq!(
        job.lease_token,
        Some(token),
        "lease token must survive reopen"
    );

    // The recovered token can be used to complete the job.
    store.ack_job(job_id, token, None, 1).unwrap();
    assert_eq!(store.job_state(job_id, 1).unwrap(), JobState::Succeeded);

    drop(store);
    let store = StoreBuilder::new(&path).open().unwrap();
    assert_eq!(store.job_state(job_id, 1).unwrap(), JobState::Succeeded);
}

#[test]
fn claim_jobs_limit_one_uses_strict_lexicographic_priority() {
    let _guard = FAILPOINT_LOCK.lock().unwrap();
    let tmp = TempDir::new();
    let path = tmp.path().join("lex.mini");

    fn run_phase(store: &minisqlite::Store, a1: Id, a2: Id, b1: Id) {
        // Enqueue b before a, but lexicographic order still puts a first.
        store
            .commit(
                CommitBatch::new(Id::new().unwrap(), 0)
                    .enqueue_job(JobSpec::new(b1, "q", "b", b"b1".to_vec()))
                    .enqueue_job(JobSpec::new(a1, "q", "a", b"a1".to_vec())),
            )
            .unwrap();

        // First limit=1 claim: a is lexicographically first.
        let first = store
            .claim_jobs(ClaimRequest {
                queue: "q".into(),
                worker_id: "w".into(),
                now_ms: 0,
                lease_ms: 1000,
                limit: 1,
            })
            .unwrap();
        assert_eq!(first.len(), 1);
        assert_eq!(first.claims()[0].partition, "a");
        assert_eq!(first.claims()[0].job_id, a1);

        // Acknowledge a1 and add another a while b is still waiting.
        // With no active lease in a, a2 is still lexicographically preferred over b1.
        store
            .ack_job(a1, first.claims()[0].lease_token, None, 1)
            .unwrap();
        store
            .commit(
                CommitBatch::new(Id::new().unwrap(), 1).enqueue_job(JobSpec::new(
                    a2,
                    "q",
                    "a",
                    b"a2".to_vec(),
                )),
            )
            .unwrap();

        let second = store
            .claim_jobs(ClaimRequest {
                queue: "q".into(),
                worker_id: "w".into(),
                now_ms: 1,
                lease_ms: 1000,
                limit: 1,
            })
            .unwrap();
        assert_eq!(second.len(), 1);
        assert_eq!(second.claims()[0].partition, "a");
        assert_eq!(second.claims()[0].job_id, a2);

        // Acknowledge a2. Now b1 is the only ready job.
        store
            .ack_job(a2, second.claims()[0].lease_token, None, 2)
            .unwrap();
        let third = store
            .claim_jobs(ClaimRequest {
                queue: "q".into(),
                worker_id: "w".into(),
                now_ms: 2,
                lease_ms: 1000,
                limit: 1,
            })
            .unwrap();
        assert_eq!(third.len(), 1);
        assert_eq!(third.claims()[0].partition, "b");
        assert_eq!(third.claims()[0].job_id, b1);

        store
            .ack_job(b1, third.claims()[0].lease_token, None, 3)
            .unwrap();
    }

    let store = StoreBuilder::new(&path).open().unwrap();
    let a1 = Id::new().unwrap();
    let a2 = Id::new().unwrap();
    let b1 = Id::new().unwrap();
    run_phase(&store, a1, a2, b1);
    drop(store);

    // Reopen and replay: the behavior must be deterministic from durable state.
    let store = StoreBuilder::new(&path).open().unwrap();
    let a3 = Id::new().unwrap();
    let a4 = Id::new().unwrap();
    let b2 = Id::new().unwrap();
    run_phase(&store, a3, a4, b2);
}

#[test]
fn duplicate_enqueue_preserves_lease_token_for_ack_and_fail() {
    let _guard = FAILPOINT_LOCK.lock().unwrap();
    let tmp = TempDir::new();
    let path = tmp.path().join("duplicate_enqueue.mini");

    let job_id = Id::new().unwrap();
    let store = StoreBuilder::new(&path).open().unwrap();
    let spec = JobSpec::new(job_id, "q", "p", b"payload".to_vec())
        .with_max_attempts(1)
        .with_effect_mode(EffectMode::Idempotent);
    store
        .commit(CommitBatch::new(Id::new().unwrap(), 0).enqueue_job(spec.clone()))
        .unwrap();

    let claimed = store
        .claim_jobs(ClaimRequest {
            queue: "q".into(),
            worker_id: "w".into(),
            now_ms: 0,
            lease_ms: 1000,
            limit: 1,
        })
        .unwrap();
    assert_eq!(claimed.len(), 1);
    let token = claimed.claims()[0].lease_token;

    // Re-assert the same job spec and immediately acknowledge. The duplicate enqueue
    // must be a no-op for state-machine simulation, so the existing lease token remains valid.
    store
        .commit(
            CommitBatch::new(Id::new().unwrap(), 1)
                .enqueue_job(spec.clone())
                .acknowledge_job(job_id, token, None),
        )
        .unwrap();
    assert_eq!(store.job_state(job_id, 2).unwrap(), JobState::Succeeded);
    drop(store);

    let store = StoreBuilder::new(&path).open().unwrap();
    assert_eq!(store.job_state(job_id, 2).unwrap(), JobState::Succeeded);

    // Test the fail path with a fresh job.
    let job2 = Id::new().unwrap();
    let spec2 = JobSpec::new(job2, "q", "p", b"payload".to_vec())
        .with_max_attempts(1)
        .with_effect_mode(EffectMode::Idempotent);
    store
        .commit(CommitBatch::new(Id::new().unwrap(), 3).enqueue_job(spec2.clone()))
        .unwrap();
    let claimed2 = store
        .claim_jobs(ClaimRequest {
            queue: "q".into(),
            worker_id: "w".into(),
            now_ms: 4,
            lease_ms: 1000,
            limit: 1,
        })
        .unwrap();
    assert_eq!(claimed2.len(), 1);
    let token2 = claimed2.claims()[0].lease_token;

    store
        .commit(
            CommitBatch::new(Id::new().unwrap(), 5)
                .enqueue_job(spec2)
                .fail_job(job2, token2, "boom", None),
        )
        .unwrap();
    assert_eq!(store.job_state(job2, 6).unwrap(), JobState::Dead);
    drop(store);

    let store = StoreBuilder::new(&path).open().unwrap();
    assert_eq!(store.job_state(job2, 6).unwrap(), JobState::Dead);
}

#[cfg(feature = "failpoint")]
#[test]
fn backup_is_rejected_while_store_is_poisoned() {
    let _guard = FAILPOINT_LOCK.lock().unwrap();
    let tmp = TempDir::new();
    let path = tmp.path().join("poisoned_backup.mini");
    let backup = tmp.path().join("backup.mini");

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
    drop(store);

    std::env::set_var("MINISQLITE_FAILPOINT", "commit-uncertain");
    let store = StoreBuilder::new(&path).open().unwrap();
    let _ = store
        .claim_jobs(ClaimRequest {
            queue: "q".into(),
            worker_id: "w".into(),
            now_ms: 0,
            lease_ms: 1000,
            limit: 1,
        })
        .unwrap();
    std::env::remove_var("MINISQLITE_FAILPOINT");
    assert!(store.is_poisoned());

    let result = store.backup(&backup);
    assert!(
        matches!(result, Err(minisqlite::Error::StorePoisoned { .. })),
        "backup must refuse a poisoned store: {result:?}"
    );
    assert!(
        !backup.exists(),
        "backup file must not be created when the store is poisoned"
    );
    drop(store);

    // After reopen the store is un-poisoned; backup should succeed.
    let store = StoreBuilder::new(&path).open().unwrap();
    assert!(!store.is_poisoned());
    store.backup(&backup).unwrap();
    assert!(backup.exists());
}

#[cfg(feature = "failpoint")]
#[test]
fn backup_after_link_returns_outcome_uncertain_and_leaves_destination() {
    let _guard = FAILPOINT_LOCK.lock().unwrap();
    let tmp = TempDir::new();
    let path = tmp.path().join("primary.mini");
    let backup = tmp.path().join("backup.mini");

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

    std::env::set_var("MINISQLITE_FAILPOINT", "backup-after-link");
    let result = store.backup(&backup);
    std::env::remove_var("MINISQLITE_FAILPOINT");

    assert!(
        matches!(
            result,
            Err(minisqlite::Error::BackupOutcomeUncertain { .. })
        ),
        "backup-after-link must return BackupOutcomeUncertain: {result:?}"
    );
    assert!(backup.exists(), "backup destination must exist after link");
    assert!(
        backup.metadata().unwrap().len() > 0,
        "backup destination must contain data"
    );
    // The temp file must also be left in place because we cannot prove the rename completed.
    let temp_left = std::fs::read_dir(tmp.path())
        .unwrap()
        .filter_map(|e| e.ok())
        .any(|e| e.file_name().to_string_lossy().contains(".mini.tmp."));
    assert!(
        temp_left,
        "source temp link must remain while outcome is uncertain"
    );
}

#[cfg(feature = "failpoint")]
#[test]
fn backup_after_publication_returns_outcome_uncertain_with_valid_destination() {
    let _guard = FAILPOINT_LOCK.lock().unwrap();
    let tmp = TempDir::new();
    let path = tmp.path().join("primary.mini");
    let backup = tmp.path().join("backup.mini");

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

    std::env::set_var("MINISQLITE_FAILPOINT", "backup-after-publication");
    let result = store.backup(&backup);
    std::env::remove_var("MINISQLITE_FAILPOINT");

    assert!(
        matches!(
            result,
            Err(minisqlite::Error::BackupOutcomeUncertain { .. })
        ),
        "backup-after-publication must return BackupOutcomeUncertain: {result:?}"
    );
    assert!(
        backup.exists(),
        "backup destination must exist after publication"
    );
    // The temp file has been unlinked; only the primary and backup remain.
    let temp_left = std::fs::read_dir(tmp.path())
        .unwrap()
        .filter_map(|e| e.ok())
        .any(|e| e.file_name().to_string_lossy().contains(".mini.tmp."));
    assert!(!temp_left, "source temp must be unlinked after publication");

    // Reopen the backup and verify it is a readable copy.
    let backup_store = StoreBuilder::new(&backup).open().unwrap();
    let events = backup_store.events_after(0, 1);
    assert_eq!(events.len(), 1);
}

#[cfg(feature = "failpoint")]
#[test]
fn repair_on_strict_sync_failure_returns_outcome_uncertain() {
    let _guard = FAILPOINT_LOCK.lock().unwrap();
    let tmp = TempDir::new();
    let path = tmp.path().join("repair_sync.mini");

    // Create a valid frame and then append a partial torn tail.
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
    drop(store);

    let mut file = std::fs::OpenOptions::new()
        .append(true)
        .open(&path)
        .unwrap();
    use std::io::Write;
    file.write_all(&[0xff, 0xff]).unwrap();
    file.sync_all().unwrap();
    drop(file);

    // open_existing leaves the torn tail and marks the store as needing repair.
    let store = StoreBuilder::new(&path).open_existing().unwrap();
    std::env::set_var("MINISQLITE_FAILPOINT", "truncate-sync-error");
    let result = store.repair();
    std::env::remove_var("MINISQLITE_FAILPOINT");

    assert!(
        matches!(
            result,
            Err(minisqlite::Error::RepairOutcomeUncertain { .. })
        ),
        "repair with Strict sync failure must return RepairOutcomeUncertain: {result:?}"
    );
}

#[test]
fn projection_replace_within_limit_roundtrips() {
    let _guard = FAILPOINT_LOCK.lock().unwrap();
    let tmp = TempDir::new();
    let path = tmp.path().join("replace_ok.mini");

    let mut entries = Vec::new();
    for i in 0..100usize {
        let key = format!("key-{i}").into_bytes();
        let value = format!("value-{i}").into_bytes();
        entries.push(ProjectionEntry::new(key, value));
    }

    let store = StoreBuilder::new(&path).open().unwrap();
    store
        .commit(CommitBatch::new(Id::new().unwrap(), 0).projection_replace("kv", 1, entries))
        .unwrap();

    let scanned = store.scan_projection_prefix("kv", b"").unwrap();
    assert_eq!(scanned.len(), 100);
}
