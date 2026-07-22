use std::collections::HashMap;

use rusqlite::{Connection, OptionalExtension, Transaction, TransactionBehavior};

use crate::config::Limits;
use crate::error::{CommitError, Conflict, IndeterminateCommit, StorageError, ValidationError};
use crate::id::Id;
use crate::transaction::{CommitBatch, CommitReceipt, Operation, TransactionRecovery};

/// Run the full commit pipeline for one batch.
pub(crate) fn commit(
    conn: &mut Connection,
    limits: &Limits,
    batch: &CommitBatch,
) -> Result<CommitReceipt, CommitError> {
    validate_batch(limits, batch)?;
    let digest = batch.request_digest();

    let tx = conn
        .transaction_with_behavior(TransactionBehavior::Immediate)
        .map_err(StorageError::from_sqlite)?;

    // Idempotent resubmission: a transaction ID that already committed with the same
    // digest returns the original receipt; a different digest is a hard error.
    if let Some((sequence, committed_at_ms, stored_digest)) =
        lookup_transaction(&tx, batch.transaction_id)?
    {
        if stored_digest == digest {
            return Ok(CommitReceipt {
                transaction_id: batch.transaction_id,
                transaction_sequence: sequence,
                committed_at_ms,
            });
        }
        return Err(CommitError::DuplicateIdWithDifferentContent);
    }

    // Load and validate expected stream versions against durable state.
    let mut stream_versions: HashMap<String, u64> = HashMap::new();
    for expected in &batch.expected_stream_versions {
        let actual = current_stream_version(&tx, &expected.stream_id)?;
        if actual != expected.version {
            return Err(CommitError::Conflict(Conflict::StreamVersion {
                stream_id: expected.stream_id.clone(),
                expected: expected.version,
                actual,
            }));
        }
        stream_versions.insert(expected.stream_id.clone(), actual);
    }

    let sequence: u64 = tx
        .query_row(
            "SELECT COALESCE(MAX(transaction_sequence), 0) + 1 FROM transactions",
            [],
            |row| row.get::<_, i64>(0),
        )
        .map_err(StorageError::from_sqlite)? as u64;

    tx.execute(
        "INSERT INTO transactions (transaction_id, transaction_sequence, committed_at_ms, correlation_id, metadata, request_digest, operation_count) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7)",
        rusqlite::params![
            batch.transaction_id.as_bytes().as_slice(),
            sequence as i64,
            batch.committed_at_ms,
            batch.correlation_id.map(|id| id.as_bytes().to_vec()),
            batch.metadata,
            digest.as_slice(),
            batch.operations.len() as i64,
        ],
    )
    .map_err(StorageError::from_sqlite)?;

    for operation in &batch.operations {
        match operation {
            Operation::AppendEvent(event) => {
                let current = match stream_versions.get(&event.stream_id) {
                    Some(v) => *v,
                    None => {
                        let v = current_stream_version(&tx, &event.stream_id)?;
                        stream_versions.insert(event.stream_id.clone(), v);
                        v
                    }
                };
                let next = current.checked_add(1).ok_or_else(|| {
                    ValidationError(format!("stream {} version overflow", event.stream_id))
                })?;
                crate::store::events::insert_event(&tx, batch.transaction_id, event, next)?;
                stream_versions.insert(event.stream_id.clone(), next);
            }
            Operation::ProjectionPatch(patch) => {
                crate::store::projections::apply_projection_patch(&tx, patch)?;
            }
            Operation::EnqueueJob(spec) => {
                crate::store::jobs::apply_enqueue(&tx, batch.transaction_id, spec)?;
            }
            Operation::AckJob(ack) => {
                crate::store::jobs::apply_ack(
                    &tx,
                    batch.transaction_id,
                    batch.committed_at_ms,
                    ack,
                )?;
            }
            Operation::FailJob(failure) => {
                crate::store::jobs::apply_fail(
                    &tx,
                    batch.transaction_id,
                    batch.committed_at_ms,
                    failure,
                )?;
            }
            Operation::CancelJob(cancellation) => {
                crate::store::jobs::apply_cancel(
                    &tx,
                    batch.transaction_id,
                    batch.committed_at_ms,
                    cancellation,
                )?;
            }
            Operation::ResolveJob(resolution) => {
                crate::store::jobs::apply_resolve(
                    &tx,
                    batch.transaction_id,
                    batch.committed_at_ms,
                    resolution,
                )?;
            }
            Operation::ExtendLease(extension) => {
                crate::store::jobs::apply_extend_lease(&tx, extension, batch.committed_at_ms)
                    .map_err(|e| match e {
                        crate::error::LeaseError::Conflict(c) => {
                            CommitError::Conflict(Conflict::Lease(c))
                        }
                        crate::error::LeaseError::Storage(s) => CommitError::Storage(s),
                        // apply_extend_lease never commits, so it cannot be indeterminate.
                        crate::error::LeaseError::Indeterminate(i) => CommitError::Storage(
                            StorageError::Sqlite(i.storage_error().to_string()),
                        ),
                    })?;
            }
        }
    }

    for (stream_id, version) in &stream_versions {
        tx.execute(
            "INSERT INTO streams (stream_id, current_version) VALUES (?1, ?2) ON CONFLICT(stream_id) DO UPDATE SET current_version = excluded.current_version",
            rusqlite::params![stream_id, *version as i64],
        )
        .map_err(StorageError::from_sqlite)?;
    }

    // Any failure of the COMMIT step itself may or may not have persisted; report
    // indeterminacy and let the caller recover via `recover_transaction`.
    tx.commit().map_err(|e| {
        CommitError::Indeterminate(IndeterminateCommit {
            transaction_id: batch.transaction_id,
            storage_error: StorageError::from_sqlite(e).to_string(),
        })
    })?;

    #[cfg(feature = "failpoints")]
    if crate::store::failpoints::take_fail_commit() {
        return Err(CommitError::Indeterminate(IndeterminateCommit {
            transaction_id: batch.transaction_id,
            storage_error: "failpoint: COMMIT outcome unknown".into(),
        }));
    }

    Ok(CommitReceipt {
        transaction_id: batch.transaction_id,
        transaction_sequence: sequence,
        committed_at_ms: batch.committed_at_ms,
    })
}

/// Look up a committed transaction and reconstruct its receipt.
pub(crate) fn recover_transaction(
    conn: &Connection,
    transaction_id: Id,
) -> Result<TransactionRecovery, StorageError> {
    let row: Option<(i64, i64)> = conn
        .query_row(
            "SELECT transaction_sequence, committed_at_ms FROM transactions WHERE transaction_id = ?1",
            [transaction_id.as_bytes().as_slice()],
            |row| Ok((row.get(0)?, row.get(1)?)),
        )
        .optional().map_err(StorageError::from_sqlite)?;
    Ok(match row {
        Some((sequence, committed_at_ms)) => TransactionRecovery::Committed(CommitReceipt {
            transaction_id,
            transaction_sequence: sequence as u64,
            committed_at_ms,
        }),
        // A missing row after a successful read means the transaction rolled back;
        // SQLite commits are atomic, so there is no lingering indeterminate state.
        None => TransactionRecovery::Absent,
    })
}

fn lookup_transaction(
    tx: &Transaction<'_>,
    transaction_id: Id,
) -> Result<Option<(u64, i64, Vec<u8>)>, StorageError> {
    let row = tx
        .query_row(
            "SELECT transaction_sequence, committed_at_ms, request_digest FROM transactions WHERE transaction_id = ?1",
            [transaction_id.as_bytes().as_slice()],
            |row| {
                let sequence: i64 = row.get(0)?;
                let committed_at_ms: i64 = row.get(1)?;
                let digest: Vec<u8> = row.get(2)?;
                Ok((sequence, committed_at_ms, digest))
            },
        )
        .optional()
        .map_err(StorageError::from_sqlite)?;
    // The digest is compared as an opaque blob: a row written by an older build
    // with a different digest algorithm simply fails the resubmission match.
    Ok(row.map(|(sequence, committed_at_ms, digest)| (sequence as u64, committed_at_ms, digest)))
}

fn current_stream_version(tx: &Transaction<'_>, stream_id: &str) -> Result<u64, CommitError> {
    let version: Option<i64> = tx
        .query_row(
            "SELECT current_version FROM streams WHERE stream_id = ?1",
            [stream_id],
            |row| row.get(0),
        )
        .optional()
        .map_err(StorageError::from_sqlite)?;
    Ok(version.unwrap_or(0) as u64)
}

fn validate_batch(limits: &Limits, batch: &CommitBatch) -> Result<(), CommitError> {
    if batch.transaction_id == Id::ZERO {
        return Err(ValidationError("transaction id cannot be zero".into()).into());
    }
    if batch.operations.len() > limits.max_operations_per_commit {
        return Err(ValidationError(format!(
            "operation count {} exceeds limit {}",
            batch.operations.len(),
            limits.max_operations_per_commit
        ))
        .into());
    }
    limits.validate_metadata(batch.metadata.len())?;
    for expected in &batch.expected_stream_versions {
        limits.validate_string("stream id", &expected.stream_id)?;
    }
    for operation in &batch.operations {
        match operation {
            Operation::AppendEvent(event) => {
                if event.event_id == Id::ZERO {
                    return Err(ValidationError("event id cannot be zero".into()).into());
                }
                limits.validate_string("stream id", &event.stream_id)?;
                limits.validate_string("event type", &event.event_type)?;
                limits.validate_event(event.payload.len(), event.metadata.len())?;
            }
            Operation::ProjectionPatch(patch) => {
                limits.validate_string("projection", &patch.projection)?;
                patch.validate()?;
                for mutation in &patch.mutations {
                    use crate::projection::ProjectionMutation;
                    match mutation {
                        ProjectionMutation::Put { key, value } => {
                            limits.validate_projection_key(key.len())?;
                            limits.validate_projection_value(value.len())?;
                        }
                        ProjectionMutation::Delete { key } => {
                            limits.validate_projection_key(key.len())?;
                        }
                        ProjectionMutation::Clear => {}
                        ProjectionMutation::Replace { entries } => {
                            if entries.len() > limits.max_replace_entries {
                                return Err(ValidationError(format!(
                                    "replace entry count {} exceeds limit {}",
                                    entries.len(),
                                    limits.max_replace_entries
                                ))
                                .into());
                            }
                            for entry in entries {
                                limits.validate_projection_key(entry.key.len())?;
                                limits.validate_projection_value(entry.value.len())?;
                            }
                        }
                    }
                }
            }
            Operation::EnqueueJob(spec) => {
                if spec.job_id == Id::ZERO {
                    return Err(ValidationError("job id cannot be zero".into()).into());
                }
                if spec.max_attempts == 0 {
                    return Err(
                        ValidationError("job max_attempts must be at least 1".into()).into(),
                    );
                }
                limits.validate_string("queue", &spec.queue)?;
                limits.validate_string("partition key", &spec.partition_key)?;
                limits.validate_job_payload(spec.payload.len())?;
                if let Some(key) = &spec.idempotency_key {
                    limits.validate_string("idempotency key", key)?;
                }
            }
            Operation::AckJob(ack) => {
                if ack.lease_token == Id::ZERO {
                    return Err(ValidationError("lease token cannot be zero".into()).into());
                }
            }
            Operation::FailJob(failure) => {
                if failure.lease_token == Id::ZERO {
                    return Err(ValidationError("lease token cannot be zero".into()).into());
                }
                limits.validate_summary(&failure.error_summary)?;
            }
            Operation::CancelJob(cancellation) => {
                if cancellation.lease_token == Some(Id::ZERO) {
                    return Err(ValidationError("lease token cannot be zero".into()).into());
                }
            }
            Operation::ResolveJob(_) => {}
            Operation::ExtendLease(extension) => {
                if extension.lease_token == Id::ZERO {
                    return Err(ValidationError("lease token cannot be zero".into()).into());
                }
            }
        }
    }
    Ok(())
}
