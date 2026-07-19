use crc32fast::Hasher;

/// Compute a CRC32 checksum over the concatenated byte slices.
pub fn crc32(parts: &[&[u8]]) -> u32 {
    let mut h = Hasher::new();
    for part in parts {
        h.update(part);
    }
    h.finalize()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn known_empty_crc() {
        assert_eq!(crc32(&[]), 0x0000_0000);
    }

    #[test]
    fn concatenation_matches() {
        let a = b"hello ";
        let b = b"world";
        let split = crc32(&[a, b]);
        let joined = crc32(&[b"hello world"]);
        assert_eq!(split, joined);
    }
}
