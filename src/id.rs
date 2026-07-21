use std::fmt;
use std::io;
#[cfg(unix)]
use std::io::Read;
use std::str::FromStr;

/// A 128-bit opaque identifier used for transaction IDs, event IDs, job IDs, and lease tokens.
///
/// `Id` is sixteen bytes, ordered by lexicographic byte order, and printable as lower-case
/// hexadecimal. New identifiers are 128 bits from the OS CSPRNG, so distinct processes and
/// restarts collide only with negligible probability. The zero ID is reserved as a sentinel
/// and is not valid as a transaction, event, or job ID.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct Id(pub [u8; 16]);

impl Id {
    /// The zero ID. Not valid as a transaction, event, or job ID, but useful as a sentinel.
    pub const ZERO: Self = Self([0; 16]);

    /// Generate a new 128-bit identifier from the OS CSPRNG.
    ///
    /// Entropy failures are returned as an error instead of panicking, so callers
    /// decide how to surface the outage.
    pub fn new() -> Result<Self, crate::Error> {
        let mut bytes = [0u8; 16];
        secure_random(&mut bytes)
            .map_err(|e| crate::Error::Storage(crate::StorageError::Io(e.to_string())))?;
        if bytes == [0; 16] {
            // The zero ID is reserved; this is astronomically unlikely.
            bytes[15] = 1;
        }
        Ok(Self(bytes))
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

// `Id` intentionally does not implement `Default`: a random identifier requires a
// fallible OS call, and a zero default would be a dangerous silent error.

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

/// Error returned when parsing a malformed hexadecimal ID string.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct InvalidId;

impl fmt::Display for InvalidId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "invalid id")
    }
}

impl std::error::Error for InvalidId {}

fn secure_random(buf: &mut [u8]) -> io::Result<()> {
    #[cfg(unix)]
    {
        let mut file = std::fs::File::open("/dev/urandom")?;
        file.read_exact(buf)?;
        Ok(())
    }

    // The crate forbids `unsafe`, so platforms without a file-based CSPRNG source
    // are not yet supported.
    #[cfg(not(unix))]
    {
        Err(io::Error::other(
            "no secure random source for this platform",
        ))
    }
}

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
        let id = Id::new().unwrap();
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

    #[test]
    fn new_is_non_zero() {
        let mut saw_non_zero = false;
        for _ in 0..32 {
            if Id::new().unwrap() != Id::ZERO {
                saw_non_zero = true;
            }
        }
        assert!(saw_non_zero);
    }
}
