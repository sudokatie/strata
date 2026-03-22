//! Bloom filter for SSTable key membership testing.
//!
//! Uses double hashing: h(i) = h1 + i * h2

use std::hash::{Hash, Hasher};
use std::collections::hash_map::DefaultHasher;

/// Default bits per key for bloom filter.
pub const DEFAULT_BITS_PER_KEY: usize = 10;

/// Bloom filter.
pub struct BloomFilter {
    bits: Vec<u8>,
    num_hash: usize,
}

impl BloomFilter {
    /// Create a new bloom filter for the given number of keys.
    pub fn new(num_keys: usize, bits_per_key: usize) -> Self {
        let bits_per_key = bits_per_key.max(1);
        let num_bits = (num_keys * bits_per_key).max(64);
        let num_bytes = (num_bits + 7) / 8;

        // Calculate optimal number of hash functions: k = ln(2) * (m/n)
        // Capped between 1 and 30
        let num_hash = ((bits_per_key as f64 * 0.69) as usize).clamp(1, 30);

        Self {
            bits: vec![0u8; num_bytes],
            num_hash,
        }
    }

    /// Create from existing bytes (for reading from disk).
    pub fn from_bytes(data: Vec<u8>) -> Option<Self> {
        if data.is_empty() {
            return None;
        }

        let num_hash = data[data.len() - 1] as usize;
        if num_hash == 0 || num_hash > 30 {
            return None;
        }

        let bits = data[..data.len() - 1].to_vec();
        Some(Self { bits, num_hash })
    }

    /// Add a key to the filter.
    pub fn add(&mut self, key: &[u8]) {
        let (h1, h2) = self.hash_pair(key);
        let num_bits = self.bits.len() * 8;

        for i in 0..self.num_hash {
            let bit_pos = (h1.wrapping_add((i as u64).wrapping_mul(h2))) % (num_bits as u64);
            self.set_bit(bit_pos as usize);
        }
    }

    /// Check if a key might be in the filter.
    /// Returns true if possibly present, false if definitely not present.
    pub fn may_contain(&self, key: &[u8]) -> bool {
        let (h1, h2) = self.hash_pair(key);
        let num_bits = self.bits.len() * 8;

        for i in 0..self.num_hash {
            let bit_pos = (h1.wrapping_add((i as u64).wrapping_mul(h2))) % (num_bits as u64);
            if !self.get_bit(bit_pos as usize) {
                return false;
            }
        }
        true
    }

    /// Encode to bytes for storage.
    pub fn encode(&self) -> Vec<u8> {
        let mut result = self.bits.clone();
        result.push(self.num_hash as u8);
        result
    }

    /// Number of bits in the filter.
    pub fn num_bits(&self) -> usize {
        self.bits.len() * 8
    }

    /// Number of hash functions.
    pub fn num_hashes(&self) -> usize {
        self.num_hash
    }

    fn set_bit(&mut self, pos: usize) {
        let byte_idx = pos / 8;
        let bit_idx = pos % 8;
        if byte_idx < self.bits.len() {
            self.bits[byte_idx] |= 1 << bit_idx;
        }
    }

    fn get_bit(&self, pos: usize) -> bool {
        let byte_idx = pos / 8;
        let bit_idx = pos % 8;
        if byte_idx < self.bits.len() {
            (self.bits[byte_idx] & (1 << bit_idx)) != 0
        } else {
            false
        }
    }

    fn hash_pair(&self, key: &[u8]) -> (u64, u64) {
        // Use two different hash functions
        let mut hasher1 = DefaultHasher::new();
        key.hash(&mut hasher1);
        let h1 = hasher1.finish();

        // Second hash: add a salt
        let mut hasher2 = DefaultHasher::new();
        0xDEADBEEFu64.hash(&mut hasher2);
        key.hash(&mut hasher2);
        let h2 = hasher2.finish();

        (h1, h2)
    }
}

/// Builder for bloom filters.
pub struct BloomFilterBuilder {
    keys: Vec<Vec<u8>>,
    bits_per_key: usize,
}

impl BloomFilterBuilder {
    /// Create a new builder.
    pub fn new(bits_per_key: usize) -> Self {
        Self {
            keys: Vec::new(),
            bits_per_key,
        }
    }

    /// Add a key.
    pub fn add(&mut self, key: &[u8]) {
        self.keys.push(key.to_vec());
    }

    /// Build the filter.
    pub fn build(self) -> BloomFilter {
        let mut filter = BloomFilter::new(self.keys.len(), self.bits_per_key);
        for key in &self.keys {
            filter.add(key);
        }
        filter
    }
}

impl Default for BloomFilterBuilder {
    fn default() -> Self {
        Self::new(DEFAULT_BITS_PER_KEY)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_bloom_add_contains() {
        let mut filter = BloomFilter::new(100, 10);
        filter.add(b"hello");
        filter.add(b"world");

        assert!(filter.may_contain(b"hello"));
        assert!(filter.may_contain(b"world"));
    }

    #[test]
    fn test_bloom_not_found() {
        let mut filter = BloomFilter::new(100, 10);
        filter.add(b"hello");

        // Key not added should likely return false (may have false positives)
        // Test with many keys to reduce chance of all being false positives
        let mut false_negatives = 0;
        for i in 0..1000 {
            let key = format!("notfound{}", i);
            if !filter.may_contain(key.as_bytes()) {
                false_negatives += 1;
            }
        }
        // Should have mostly negatives for keys not added
        assert!(false_negatives > 900, "too many false positives");
    }

    #[test]
    fn test_bloom_encode_decode() {
        let mut filter = BloomFilter::new(100, 10);
        filter.add(b"key1");
        filter.add(b"key2");

        let encoded = filter.encode();
        let decoded = BloomFilter::from_bytes(encoded).unwrap();

        assert!(decoded.may_contain(b"key1"));
        assert!(decoded.may_contain(b"key2"));
    }

    #[test]
    fn test_bloom_builder() {
        let mut builder = BloomFilterBuilder::new(10);
        builder.add(b"apple");
        builder.add(b"banana");
        builder.add(b"cherry");

        let filter = builder.build();
        assert!(filter.may_contain(b"apple"));
        assert!(filter.may_contain(b"banana"));
        assert!(filter.may_contain(b"cherry"));
    }

    #[test]
    fn test_bloom_false_positive_rate() {
        // Add 1000 keys, check false positive rate on 1000 other keys
        let mut builder = BloomFilterBuilder::new(10);
        for i in 0..1000 {
            let key = format!("key{}", i);
            builder.add(key.as_bytes());
        }
        let filter = builder.build();

        // All added keys should be found
        for i in 0..1000 {
            let key = format!("key{}", i);
            assert!(filter.may_contain(key.as_bytes()));
        }

        // Count false positives on non-existent keys
        let mut false_positives = 0;
        for i in 1000..2000 {
            let key = format!("key{}", i);
            if filter.may_contain(key.as_bytes()) {
                false_positives += 1;
            }
        }

        // With 10 bits per key, expected FP rate is ~1%
        // Allow up to 5% for test stability
        assert!(
            false_positives < 50,
            "false positive rate too high: {}%",
            false_positives as f64 / 10.0
        );
    }

    #[test]
    fn test_bloom_empty() {
        let filter = BloomFilter::new(0, 10);
        // Empty filter should still work
        assert!(!filter.may_contain(b"anything"));
    }
}
