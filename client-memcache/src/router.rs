/// Route a key to a shard index using FNV-1a hash.
///
/// Single-server case returns 0 (no hashing needed).
pub fn route_key(key: &[u8], shard_count: usize) -> usize {
    if shard_count <= 1 {
        return 0;
    }
    let hash = fnv1a(key);
    (hash as usize) % shard_count
}

/// FNV-1a hash (32-bit).
fn fnv1a(data: &[u8]) -> u32 {
    let mut hash: u32 = 0x811c_9dc5;
    for &byte in data {
        hash ^= byte as u32;
        hash = hash.wrapping_mul(0x0100_0193);
    }
    hash
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn single_shard_always_zero() {
        assert_eq!(route_key(b"any-key", 1), 0);
        assert_eq!(route_key(b"", 1), 0);
    }

    #[test]
    fn deterministic() {
        let a = route_key(b"test-key", 3);
        let b = route_key(b"test-key", 3);
        assert_eq!(a, b);
    }

    #[test]
    fn distributes() {
        let mut counts = [0u32; 4];
        for i in 0..1000u32 {
            let key = format!("key-{i}");
            let shard = route_key(key.as_bytes(), 4);
            counts[shard] += 1;
        }
        // Each shard should get at least some keys
        for count in &counts {
            assert!(*count > 100, "poor distribution: {counts:?}");
        }
    }
}
