use std::fmt;
use std::str::FromStr;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{SystemTime, UNIX_EPOCH};

/// A 128-bit opaque identifier used for transaction IDs, event IDs, job IDs, and lease tokens.
///
/// `Id` is intentionally simple: sixteen bytes, ordered by lexicographic byte order, and
/// printable as lower-case hexadecimal. New identifiers are generated from the process-wide
/// monotonic counter and wall-clock nanoseconds, which is sufficient for a single-owner store.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub struct Id(pub [u8; 16]);

impl Id {
    /// The zero ID. Not valid as a transaction, event, or job ID, but useful as a sentinel.
    pub const ZERO: Self = Self([0; 16]);

    /// Generate a new identifier that is unique within this process and extremely unlikely to
    /// collide across processes.
    pub fn new() -> Self {
        static COUNTER: AtomicU64 = AtomicU64::new(0);

        let counter = COUNTER.fetch_add(1, Ordering::Relaxed);
        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|d| d.as_nanos() as u64)
            .unwrap_or(counter);

        let mut bytes = [0u8; 16];
        bytes[..8].copy_from_slice(&nanos.to_le_bytes());
        bytes[8..].copy_from_slice(&counter.to_le_bytes());
        Self(bytes)
    }

    /// Construct an ID from a fixed 16-byte slice.
    pub const fn from_bytes(bytes: [u8; 16]) -> Self {
        Self(bytes)
    }

    /// Return the raw bytes.
    pub const fn as_bytes(&self) -> &[u8; 16] {
        &self.0
    }

    /// Parse a 32-character lower-case hexadecimal string into an ID.
    pub fn from_hex(s: &str) -> Result<Self, InvalidId> {
        if s.len() != 32 {
            return Err(InvalidId);
        }
        let mut bytes = [0u8; 16];
        for (i, chunk) in s.as_bytes().chunks_exact(2).enumerate() {
            let high = hex_value(chunk[0]).ok_or(InvalidId)?;
            let low = hex_value(chunk[1]).ok_or(InvalidId)?;
            bytes[i] = (high << 4) | low;
        }
        Ok(Self(bytes))
    }

    /// Format the ID as 32 lower-case hexadecimal characters.
    pub fn to_hex(&self) -> String {
        const HEX: &[u8; 16] = b"0123456789abcdef";
        let mut s = String::with_capacity(32);
        for &b in &self.0 {
            s.push(HEX[(b >> 4) as usize] as char);
            s.push(HEX[(b & 0xf) as usize] as char);
        }
        s
    }
}

impl Default for Id {
    fn default() -> Self {
        Self::new()
    }
}

impl fmt::Display for Id {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.to_hex())
    }
}

impl FromStr for Id {
    type Err = InvalidId;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        Self::from_hex(s)
    }
}

impl From<u128> for Id {
    fn from(value: u128) -> Self {
        Self(value.to_le_bytes())
    }
}

impl From<Id> for u128 {
    fn from(id: Id) -> Self {
        u128::from_le_bytes(id.0)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct InvalidId;

impl fmt::Display for InvalidId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "invalid id")
    }
}

impl std::error::Error for InvalidId {}

fn hex_value(b: u8) -> Option<u8> {
    match b {
        b'0'..=b'9' => Some(b - b'0'),
        b'a'..=b'f' => Some(b - b'a' + 10),
        b'A'..=b'F' => Some(b - b'A' + 10),
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn roundtrip_hex() {
        let id = Id::new();
        let s = id.to_hex();
        assert_eq!(s.len(), 32);
        let parsed = Id::from_hex(&s).unwrap();
        assert_eq!(id, parsed);
    }

    #[test]
    fn ordering_is_lexicographic() {
        let a = Id::from(1u128);
        let b = Id::from(2u128);
        assert!(a < b);
    }
}
