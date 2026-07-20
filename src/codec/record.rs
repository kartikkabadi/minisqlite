use crate::codec::{Reader, Writer};
use crate::config::EffectMode;
use crate::id::Id;
use crate::Error;

pub const EVENT: u8 = 0x01;
pub const PROJECTION_PUT: u8 = 0x10;
pub const PROJECTION_DELETE: u8 = 0x11;
pub const PROJECTION_CLEAR: u8 = 0x12;
pub const PROJECTION_REPLACE: u8 = 0x13;
pub const JOB_ENQUEUE: u8 = 0x20;
pub const JOB_LEASE: u8 = 0x21;
pub const JOB_ACK: u8 = 0x22;
pub const JOB_FAIL: u8 = 0x23;
pub const JOB_CANCEL: u8 = 0x24;
pub const JOB_RESOLVE: u8 = 0x25;
pub const JOB_EXPIRE: u8 = 0x26;
pub const TRANSACTION_META: u8 = 0x30;

pub const RECORD_FORMAT_VERSION: u8 = 1;
const RECORD_SUPPORTED_FLAGS: u8 = 0;

/// Hard ceiling on the number of records in any frame. This limits recovery allocation
/// before a potentially malicious payload is decoded, independent of the user-tunable
/// `Limits::max_records_per_transaction`.
pub const MAX_RECORDS_PER_FRAME: u32 = 1 << 20; // 1,048,576

/// Smallest possible encoded record size (an empty `TransactionMeta` record).
/// Used to bound `expected_count` by payload geometry before allocating memory.
const MIN_ENCODED_RECORD_SIZE: usize = 12;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct EventRecord {
    pub global_sequence: u64,
    pub stream_version: u64,
    pub event_id: Id,
    pub stream_id: String,
    pub event_type: String,
    pub schema_version: u32,
    pub occurred_at_ms: i64,
    pub causation_id: Option<Id>,
    pub correlation_id: Option<Id>,
    pub payload: Vec<u8>,
    pub metadata: Vec<u8>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Record {
    Event(EventRecord),
    ProjectionPut {
        projection: String,
        version: u64,
        key: Vec<u8>,
        value: Vec<u8>,
    },
    ProjectionDelete {
        projection: String,
        version: u64,
        key: Vec<u8>,
    },
    ProjectionClear {
        projection: String,
        new_version: u64,
    },
    ProjectionReplace {
        projection: String,
        new_version: u64,
        entries: Vec<(Vec<u8>, Vec<u8>)>,
    },
    JobEnqueue {
        job_id: Id,
        queue: String,
        partition: String,
        payload: Vec<u8>,
        not_before_ms: i64,
        max_attempts: u32,
        effect_mode: EffectMode,
        idempotency_key: Option<String>,
    },
    JobLease {
        job_id: Id,
        lease_token: Id,
        worker_id: String,
        attempt: u32,
        lease_expires_at_ms: i64,
        claimed_at_ms: i64,
    },
    JobAck {
        job_id: Id,
        lease_token: Id,
        result_digest: Option<Vec<u8>>,
        acknowledged_at_ms: i64,
    },
    JobFail {
        job_id: Id,
        lease_token: Id,
        error_summary: String,
        attempt: u32,
        retry_after_ms: i64,
        terminal: bool,
        failed_at_ms: i64,
    },
    JobCancel {
        job_id: Id,
        lease_token: Option<Id>,
        cancelled_at_ms: i64,
    },
    JobResolve {
        job_id: Id,
        resolution: Resolution,
        resolved_at_ms: i64,
    },
    JobExpire {
        job_id: Id,
        lease_token: Id,
        attempt: u32,
        expired_at_ms: i64,
    },
    TransactionMeta {
        correlation_id: Option<Id>,
        metadata: Vec<u8>,
    },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Resolution {
    Retry,
    MarkSucceeded,
    MarkDead,
}

impl Resolution {
    fn to_u8(self) -> u8 {
        match self {
            Resolution::Retry => 0,
            Resolution::MarkSucceeded => 1,
            Resolution::MarkDead => 2,
        }
    }

    fn from_u8(v: u8) -> Result<Self, Error> {
        match v {
            0 => Ok(Resolution::Retry),
            1 => Ok(Resolution::MarkSucceeded),
            2 => Ok(Resolution::MarkDead),
            _ => Err(Error::Corruption {
                message: format!("unknown job resolution {v}"),
                offset: 0,
            }),
        }
    }
}

impl Record {
    pub fn encode(&self) -> Vec<u8> {
        let mut body = Writer::new();
        match self {
            Record::Event(e) => {
                body.write_u64(e.global_sequence);
                body.write_u64(e.stream_version);
                body.write_id(e.event_id);
                body.write_string(&e.stream_id);
                body.write_string(&e.event_type);
                body.write_u32(e.schema_version);
                body.write_i64(e.occurred_at_ms);
                body.write_optional_id(e.causation_id);
                body.write_optional_id(e.correlation_id);
                body.write_bytes(&e.payload);
                body.write_bytes(&e.metadata);
            }
            Record::ProjectionPut {
                projection,
                version,
                key,
                value,
            } => {
                body.write_string(projection);
                body.write_u64(*version);
                body.write_bytes(key);
                body.write_bytes(value);
            }
            Record::ProjectionDelete {
                projection,
                version,
                key,
            } => {
                body.write_string(projection);
                body.write_u64(*version);
                body.write_bytes(key);
            }
            Record::ProjectionClear {
                projection,
                new_version,
            } => {
                body.write_string(projection);
                body.write_u64(*new_version);
            }
            Record::ProjectionReplace {
                projection,
                new_version,
                entries,
            } => {
                body.write_string(projection);
                body.write_u64(*new_version);
                body.write_u32(entries.len() as u32);
                for (k, v) in entries {
                    body.write_bytes(k);
                    body.write_bytes(v);
                }
            }
            Record::JobEnqueue {
                job_id,
                queue,
                partition,
                payload,
                not_before_ms,
                max_attempts,
                effect_mode,
                idempotency_key,
            } => {
                body.write_id(*job_id);
                body.write_string(queue);
                body.write_string(partition);
                body.write_bytes(payload);
                body.write_i64(*not_before_ms);
                body.write_u32(*max_attempts);
                body.write_u8(effect_mode.to_u8());
                body.write_optional_string(idempotency_key.as_deref());
            }
            Record::JobLease {
                job_id,
                lease_token,
                worker_id,
                attempt,
                lease_expires_at_ms,
                claimed_at_ms,
            } => {
                body.write_id(*job_id);
                body.write_id(*lease_token);
                body.write_string(worker_id);
                body.write_u32(*attempt);
                body.write_i64(*lease_expires_at_ms);
                body.write_i64(*claimed_at_ms);
            }
            Record::JobAck {
                job_id,
                lease_token,
                result_digest,
                acknowledged_at_ms,
            } => {
                body.write_id(*job_id);
                body.write_id(*lease_token);
                body.write_optional_bytes(result_digest.as_deref());
                body.write_i64(*acknowledged_at_ms);
            }
            Record::JobFail {
                job_id,
                lease_token,
                error_summary,
                attempt,
                retry_after_ms,
                terminal,
                failed_at_ms,
            } => {
                body.write_id(*job_id);
                body.write_id(*lease_token);
                body.write_string(error_summary);
                body.write_u32(*attempt);
                body.write_i64(*retry_after_ms);
                body.write_u8(*terminal as u8);
                body.write_i64(*failed_at_ms);
            }
            Record::JobCancel {
                job_id,
                lease_token,
                cancelled_at_ms,
            } => {
                body.write_id(*job_id);
                body.write_optional_id(*lease_token);
                body.write_i64(*cancelled_at_ms);
            }
            Record::JobResolve {
                job_id,
                resolution,
                resolved_at_ms,
            } => {
                body.write_id(*job_id);
                body.write_u8(resolution.to_u8());
                body.write_i64(*resolved_at_ms);
            }
            Record::JobExpire {
                job_id,
                lease_token,
                attempt,
                expired_at_ms,
            } => {
                body.write_id(*job_id);
                body.write_id(*lease_token);
                body.write_u32(*attempt);
                body.write_i64(*expired_at_ms);
            }
            Record::TransactionMeta {
                correlation_id,
                metadata,
            } => {
                body.write_optional_id(*correlation_id);
                body.write_bytes(metadata);
            }
        }

        let mut out = Writer::with_capacity(1 + 1 + 1 + 4 + body.len());
        out.write_u8(self.kind());
        out.write_u8(RECORD_FORMAT_VERSION);
        out.write_u8(RECORD_SUPPORTED_FLAGS);
        out.write_u32(body.len() as u32);
        out.bytes.extend_from_slice(&body.bytes);
        out.bytes
    }

    fn kind(&self) -> u8 {
        match self {
            Record::Event(_) => EVENT,
            Record::ProjectionPut { .. } => PROJECTION_PUT,
            Record::ProjectionDelete { .. } => PROJECTION_DELETE,
            Record::ProjectionClear { .. } => PROJECTION_CLEAR,
            Record::ProjectionReplace { .. } => PROJECTION_REPLACE,
            Record::JobEnqueue { .. } => JOB_ENQUEUE,
            Record::JobLease { .. } => JOB_LEASE,
            Record::JobAck { .. } => JOB_ACK,
            Record::JobFail { .. } => JOB_FAIL,
            Record::JobCancel { .. } => JOB_CANCEL,
            Record::JobResolve { .. } => JOB_RESOLVE,
            Record::JobExpire { .. } => JOB_EXPIRE,
            Record::TransactionMeta { .. } => TRANSACTION_META,
        }
    }

    pub fn decode(reader: &mut Reader<'_>) -> Result<Option<Self>, Error> {
        if reader.is_empty() {
            return Ok(None);
        }
        let kind = reader.read_u8()?;
        let version = reader.read_u8()?;
        if version != RECORD_FORMAT_VERSION {
            return Err(Error::Corruption {
                message: format!("unsupported record format version {version}"),
                offset: 0,
            });
        }
        let flags = reader.read_u8()?;
        if flags != RECORD_SUPPORTED_FLAGS {
            return Err(Error::Corruption {
                message: format!("unsupported record flags {flags}"),
                offset: 0,
            });
        }
        let body_len = reader.read_u32()? as usize;
        let body = reader.read_slice(body_len)?;
        let mut r = Reader::new(body);
        let record = match kind {
            EVENT => Record::Event(EventRecord {
                global_sequence: r.read_u64()?,
                stream_version: r.read_u64()?,
                event_id: r.read_id()?,
                stream_id: r.read_string()?,
                event_type: r.read_string()?,
                schema_version: r.read_u32()?,
                occurred_at_ms: r.read_i64()?,
                causation_id: r.read_optional_id()?,
                correlation_id: r.read_optional_id()?,
                payload: r.read_bytes()?,
                metadata: r.read_bytes()?,
            }),
            PROJECTION_PUT => Record::ProjectionPut {
                projection: r.read_string()?,
                version: r.read_u64()?,
                key: r.read_bytes()?,
                value: r.read_bytes()?,
            },
            PROJECTION_DELETE => Record::ProjectionDelete {
                projection: r.read_string()?,
                version: r.read_u64()?,
                key: r.read_bytes()?,
            },
            PROJECTION_CLEAR => Record::ProjectionClear {
                projection: r.read_string()?,
                new_version: r.read_u64()?,
            },
            PROJECTION_REPLACE => {
                let projection = r.read_string()?;
                let new_version = r.read_u64()?;
                let count = r.read_u32()? as usize;
                // Each entry needs at least two 4-byte length prefixes, so
                // clamp capacity to the number that can actually fit in the body.
                let max_count = r.remaining() / 8;
                let mut entries = Vec::with_capacity(count.min(max_count));
                for _ in 0..count {
                    let key = r.read_bytes()?;
                    let value = r.read_bytes()?;
                    entries.push((key, value));
                }
                Record::ProjectionReplace {
                    projection,
                    new_version,
                    entries,
                }
            }
            JOB_ENQUEUE => Record::JobEnqueue {
                job_id: r.read_id()?,
                queue: r.read_string()?,
                partition: r.read_string()?,
                payload: r.read_bytes()?,
                not_before_ms: r.read_i64()?,
                max_attempts: r.read_u32()?,
                effect_mode: EffectMode::from_u8(r.read_u8()?)?,
                idempotency_key: r.read_optional_string()?,
            },
            JOB_LEASE => Record::JobLease {
                job_id: r.read_id()?,
                lease_token: r.read_id()?,
                worker_id: r.read_string()?,
                attempt: r.read_u32()?,
                lease_expires_at_ms: r.read_i64()?,
                claimed_at_ms: r.read_i64()?,
            },
            JOB_ACK => Record::JobAck {
                job_id: r.read_id()?,
                lease_token: r.read_id()?,
                result_digest: r.read_optional_bytes()?,
                acknowledged_at_ms: r.read_i64()?,
            },
            JOB_FAIL => {
                let job_id = r.read_id()?;
                let lease_token = r.read_id()?;
                let error_summary = r.read_string()?;
                let attempt = r.read_u32()?;
                let retry_after_ms = r.read_i64()?;
                let terminal_marker = r.read_u8()?;
                let terminal = match terminal_marker {
                    0 => false,
                    1 => true,
                    _ => {
                        return Err(Error::Corruption {
                            message: format!("invalid JobFail terminal marker {terminal_marker}"),
                            offset: 0,
                        })
                    }
                };
                let failed_at_ms = r.read_i64()?;
                Record::JobFail {
                    job_id,
                    lease_token,
                    error_summary,
                    attempt,
                    retry_after_ms,
                    terminal,
                    failed_at_ms,
                }
            }
            JOB_CANCEL => Record::JobCancel {
                job_id: r.read_id()?,
                lease_token: r.read_optional_id()?,
                cancelled_at_ms: r.read_i64()?,
            },
            JOB_RESOLVE => Record::JobResolve {
                job_id: r.read_id()?,
                resolution: Resolution::from_u8(r.read_u8()?)?,
                resolved_at_ms: r.read_i64()?,
            },
            JOB_EXPIRE => Record::JobExpire {
                job_id: r.read_id()?,
                lease_token: r.read_id()?,
                attempt: r.read_u32()?,
                expired_at_ms: r.read_i64()?,
            },
            TRANSACTION_META => Record::TransactionMeta {
                correlation_id: r.read_optional_id()?,
                metadata: r.read_bytes()?,
            },
            _ => {
                return Err(Error::Corruption {
                    message: format!("unknown record kind 0x{kind:02x}"),
                    offset: 0,
                });
            }
        };
        if !r.is_empty() {
            return Err(Error::Corruption {
                message: "trailing bytes in record body".into(),
                offset: 0,
            });
        }
        Ok(Some(record))
    }
}

impl Writer {
    fn write_optional_string(&mut self, s: Option<&str>) {
        self.write_u8(s.is_some() as u8);
        if let Some(s) = s {
            self.write_string(s);
        }
    }

    fn write_optional_bytes(&mut self, b: Option<&[u8]>) {
        self.write_u8(b.is_some() as u8);
        if let Some(b) = b {
            self.write_bytes(b);
        }
    }
}

impl Reader<'_> {
    fn read_optional_string(&mut self) -> Result<Option<String>, Error> {
        let present = self.read_u8()?;
        match present {
            0 => Ok(None),
            1 => Ok(Some(self.read_string()?)),
            _ => Err(Error::Corruption {
                message: format!("invalid optional string marker {present}"),
                offset: self.pos as u64,
            }),
        }
    }

    fn read_optional_bytes(&mut self) -> Result<Option<Vec<u8>>, Error> {
        let present = self.read_u8()?;
        match present {
            0 => Ok(None),
            1 => Ok(Some(self.read_bytes()?)),
            _ => Err(Error::Corruption {
                message: format!("invalid optional bytes marker {present}"),
                offset: self.pos as u64,
            }),
        }
    }
}

impl EffectMode {
    pub(crate) fn to_u8(self) -> u8 {
        match self {
            EffectMode::Idempotent => 0,
            EffectMode::UncertainOnLeaseExpiry => 1,
        }
    }

    pub(crate) fn from_u8(v: u8) -> Result<Self, Error> {
        match v {
            0 => Ok(EffectMode::Idempotent),
            1 => Ok(EffectMode::UncertainOnLeaseExpiry),
            _ => Err(Error::Corruption {
                message: format!("unknown effect mode {v}"),
                offset: 0,
            }),
        }
    }
}

/// Encode a sequence of records into a single payload buffer.
pub fn encode_records(records: &[Record]) -> Vec<u8> {
    let mut out = Writer::new();
    for record in records {
        out.bytes.extend_from_slice(&record.encode());
    }
    out.bytes
}

/// Decode a payload buffer into a sequence of records.
///
/// `expected_count` is the record count declared by the frame header. Allocation and the
/// decoded count are bounded by [`MAX_RECORDS_PER_FRAME`] to avoid unbounded memory growth
/// from a valid-but-enormous frame. Before reserving, `expected_count` is also bounded by
/// payload geometry so a tiny frame cannot force a giant allocation.
pub fn decode_records(bytes: &[u8], expected_count: u32) -> Result<Vec<Record>, Error> {
    if expected_count > MAX_RECORDS_PER_FRAME {
        return Err(Error::Corruption {
            message: format!(
                "record count {expected_count} exceeds maximum {MAX_RECORDS_PER_FRAME}"
            ),
            offset: 0,
        });
    }
    let max_records_by_geometry = bytes.len() / MIN_ENCODED_RECORD_SIZE;
    if expected_count as usize > max_records_by_geometry {
        return Err(Error::Corruption {
            message: format!(
                "record count {expected_count} cannot fit in payload of {} bytes (min {MIN_ENCODED_RECORD_SIZE} bytes/record)",
                bytes.len()
            ),
            offset: 0,
        });
    }
    let mut reader = Reader::new(bytes);
    let mut records = Vec::with_capacity(expected_count as usize);
    while let Some(record) = Record::decode(&mut reader)? {
        if records.len() >= expected_count as usize {
            return Err(Error::Corruption {
                message: "decoded more records than frame header declared".into(),
                offset: 0,
            });
        }
        records.push(record);
    }
    if records.len() != expected_count as usize {
        return Err(Error::Corruption {
            message: format!(
                "frame record count {expected_count} does not match decoded records {}",
                records.len()
            ),
            offset: 0,
        });
    }
    Ok(records)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn records_arbitrary_bytes_never_panics() {
        for seed in 0..512 {
            let mut rng = fastrand::Rng::with_seed(seed);
            let len = rng.usize(0..1024);
            let bytes: Vec<u8> = (0..len).map(|_| rng.u8(..)).collect();
            let _ = decode_records(&bytes, u32::MAX);
        }
    }

    #[test]
    fn roundtrip_records() {
        let records = vec![
            Record::Event(EventRecord {
                global_sequence: 1,
                stream_version: 1,
                event_id: Id::new().unwrap(),
                stream_id: "thread:abc".into(),
                event_type: "thread.created".into(),
                schema_version: 1,
                occurred_at_ms: 123456789,
                causation_id: None,
                correlation_id: Some(Id::new().unwrap()),
                payload: vec![1, 2, 3],
                metadata: vec![],
            }),
            Record::ProjectionPut {
                projection: "threads".into(),
                version: 1,
                key: b"abc".to_vec(),
                value: b"{}".to_vec(),
            },
            Record::JobEnqueue {
                job_id: Id::new().unwrap(),
                queue: "provider-command".into(),
                partition: "thread:abc".into(),
                payload: b"cmd".to_vec(),
                not_before_ms: 0,
                max_attempts: 3,
                effect_mode: EffectMode::Idempotent,
                idempotency_key: Some("key".into()),
            },
        ];

        let payload = encode_records(&records);
        let decoded = decode_records(&payload, records.len() as u32).unwrap();
        assert_eq!(records, decoded);
    }

    #[test]
    fn unknown_record_version_is_rejected() {
        let mut bytes = Record::Event(EventRecord {
            global_sequence: 1,
            stream_version: 1,
            event_id: Id::new().unwrap(),
            stream_id: "thread:abc".into(),
            event_type: "thread.created".into(),
            schema_version: 1,
            occurred_at_ms: 123456789,
            causation_id: None,
            correlation_id: None,
            payload: vec![1, 2, 3],
            metadata: vec![],
        })
        .encode();
        bytes[1] = RECORD_FORMAT_VERSION + 1;
        assert!(decode_records(&bytes, 1).is_err());
    }

    #[test]
    fn unknown_record_flags_are_rejected() {
        let mut bytes = Record::Event(EventRecord {
            global_sequence: 1,
            stream_version: 1,
            event_id: Id::new().unwrap(),
            stream_id: "thread:abc".into(),
            event_type: "thread.created".into(),
            schema_version: 1,
            occurred_at_ms: 123456789,
            causation_id: None,
            correlation_id: None,
            payload: vec![1, 2, 3],
            metadata: vec![],
        })
        .encode();
        bytes[2] = 0xff;
        assert!(decode_records(&bytes, 1).is_err());
    }

    #[test]
    fn trailing_record_body_bytes_are_rejected() {
        let mut bytes = Record::Event(EventRecord {
            global_sequence: 1,
            stream_version: 1,
            event_id: Id::new().unwrap(),
            stream_id: "thread:abc".into(),
            event_type: "thread.created".into(),
            schema_version: 1,
            occurred_at_ms: 123456789,
            causation_id: None,
            correlation_id: None,
            payload: vec![1, 2, 3],
            metadata: vec![],
        })
        .encode();
        let body_len = u32::from_le_bytes([bytes[3], bytes[4], bytes[5], bytes[6]]) as usize;
        let new_len = (body_len + 1) as u32;
        bytes[3..7].copy_from_slice(&new_len.to_le_bytes());
        bytes.push(0);
        assert!(decode_records(&bytes, 1).is_err());
    }
}
