//! Skeleton for the Synara provider-turn vertical slice.
//!
//! DESIGN ONLY — the `ControlPlaneStore` API shown here does not exist yet.
//! See `docs/SYNARA_INTEGRATION.md` for the full specification. The intended
//! usage is kept in a doc-comment block below so this file compiles today;
//! it will be turned into a runnable example once the SQLite-backed kernel
//! (Phases 1–3 of the rewrite plan) lands.
//!
//! ```rust,ignore
//! use minisqlite::{
//!     ClaimError, ClaimOutcome, CommitBatch, ControlPlaneStore, Event, JobSpec, Operation,
//!     ProjectionMutation, ProjectionPatch,
//! };
//!
//! fn main() -> Result<(), Box<dyn std::error::Error>> {
//!     let store = ControlPlaneStore::open("synara-control-plane.db")?;
//!
//!     // --- 1. Thread created + 2-4. turn requested atomically -------------
//!     // One CommitBatch: event + projection "queued" + provider job (outbox).
//!     let thread_id = "thread:t-123";
//!     store.commit(CommitBatch {
//!         transaction_id: new_id(),
//!         expected_stream_versions: vec![expect(thread_id, 1)],
//!         operations: vec![
//!             Operation::AppendEvent(Event::new(thread_id, "thread.turn-requested", payload())),
//!             Operation::ApplyProjectionPatch(ProjectionPatch {
//!                 projection: "threads".into(),
//!                 expected_version: 7,
//!                 new_version: 8,
//!                 mutations: vec![ProjectionMutation::Put {
//!                     key: b"t-123".to_vec(),
//!                     value: b"{\"status\":\"queued\"}".to_vec(),
//!                 }],
//!             }),
//!             Operation::EnqueueJob(JobSpec::reconcilable(
//!                 "provider-command", // queue
//!                 thread_id,          // partition key: per-thread FIFO
//!                 provider_payload(),
//!             )),
//!         ],
//!         ..Default::default()
//!     })?;
//!
//!     // --- 5. Worker claims the provider job ------------------------------
//!     match store.claim_jobs("provider-command", "worker-1", 1, now_ms()) {
//!         Ok(ClaimOutcome::Committed(claims)) => {
//!             for job in claims.jobs() {
//!                 // --- 6. Provider effect, heartbeating the lease ---------
//!                 let _guard = heartbeat(&store, job); // extend_lease loop
//!                 let result = call_provider(job)?;
//!
//!                 // --- 7-9. Completion event + idle + ack atomically ------
//!                 store.commit(completion_batch(thread_id, job, result))?;
//!             }
//!         }
//!         Ok(ClaimOutcome::MaintenanceCommitted(_)) => { /* poll again immediately */ }
//!         Ok(ClaimOutcome::Noop) => { /* back off */ }
//!         Err(ClaimError::Indeterminate(claim)) => {
//!             // No executable data here — only a transaction id.
//!             // Reopen, then recover_claim(claim.transaction_id):
//!             //   Committed -> run with recovered lease tokens (exactly once)
//!             //   Absent    -> job is still claimable; no effect ran
//!             let _tx = claim.transaction_id;
//!         }
//!         Err(other) => return Err(other.into()),
//!     }
//!     Ok(())
//! }
//! ```

fn main() {
    println!("Design skeleton only — see docs/SYNARA_INTEGRATION.md");
}
