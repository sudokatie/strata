//! Bloom filter for efficient key lookups.
//!
//! Bloom filters provide probabilistic set membership testing, allowing
//! us to skip disk reads when a key definitely doesn't exist.

use std::hash::{Hash, Hasher};
use std::io::{Read, Write};

/// Bloom filter for key existence testing.
#[derive(Debug, Clone)]
pub struct BloomFilter {
    /// Bit array.
    bits: Vec<u64>,
    /// Number of bits.
    num_bits: usize,
    /// Number of hash functions.
    num_hashes: u32,
}

impl BloomFilter {
    /// Create a new bloom filter with the given parameters.
    ///
    /// # Arguments
    /// * `num_keys` - Expected number of keys
    /// * `false_positive_rate` - Desired false positive rate (0.0 to 1.0)
    pub fn new(num_keys: usize, false_positive_rate: f64) -> Self {
        let fp = false_positive_rate.clamp(0.0001, 0.5);
        
        // Optimal number of bits: -n * ln(p) / (ln(2)^2)
        let num_bits = (-(num_keys as f64) * fp.ln() / (2.0_f64.ln().powi(2)))
            .ceil() as usize;
        let num_bits = num_bits.max(64); // Minimum 64 bits
        
        // Optimal number of hash functions: (m/n) * ln(2)
        let num_hashes = ((num_bits as f64 / num_keys as f64) * 2.0_f64.ln())
            .ceil() as u32;
        let num_hashes = num_hashes.clamp(1, 30);
        
        // Round up to 64-bit words
        let num_words = (num_bits + 63) / 64;
        
        BloomFilter {
            bits: vec![0; num_words],
            num_bits: num_words * 64,
            num_hashes,
        }
    }

    /// Create a bloom filter with explicit parameters.
    pub fn with_params(num_bits: usize, num_hashes: u32) -> Self {
        let num_words = (num_bits + 63) / 64;
        BloomFilter {
            bits: vec![0; num_words],
            num_bits: num_words * 64,
            num_hashes: num_hashes.clamp(1, 30),
        }
    }

    /// Add a key to the filter.
    pub fn insert(&mut self, key: &[u8]) {
        let (h1, h2) = self.hash_key(key);
        
        for i in 0..self.num_hashes {
            let bit_idx = self.get_bit_index(h1, h2, i);
            let word_idx = bit_idx / 64;
            let bit_offset = bit_idx % 64;
            self.bits[word_idx] |= 1 << bit_offset;
        }
    }

    /// Check if a key might be in the filter.
    ///
    /// Returns:
    /// - `false` if the key is definitely not present
    /// - `true` if the key might be present (with some false positive probability)
    pub fn may_contain(&self, key: &[u8]) -> bool {
        let (h1, h2) = self.hash_key(key);
        
        for i in 0..self.num_hashes {
            let bit_idx = self.get_bit_index(h1, h2, i);
            let word_idx = bit_idx / 64;
            let bit_offset = bit_idx % 64;
            if (self.bits[word_idx] & (1 << bit_offset)) == 0 {
                return false;
            }
        }
        true
    }

    /// Get the size in bytes.
    pub fn size_bytes(&self) -> usize {
        self.bits.len() * 8
    }

    /// Get the number of bits.
    pub fn num_bits(&self) -> usize {
        self.num_bits
    }

    /// Get the number of hash functions.
    pub fn num_hashes(&self) -> u32 {
        self.num_hashes
    }

    /// Serialize to bytes.
    pub fn to_bytes(&self) -> Vec<u8> {
        let mut buf = Vec::with_capacity(8 + self.bits.len() * 8);
        
        // Header: num_bits (4 bytes) + num_hashes (4 bytes)
        buf.extend_from_slice(&(self.num_bits as u32).to_le_bytes());
        buf.extend_from_slice(&self.num_hashes.to_le_bytes());
        
        // Bit array
        for word in &self.bits {
            buf.extend_from_slice(&word.to_le_bytes());
        }
        
        buf
    }

    /// Deserialize from bytes.
    pub fn from_bytes(data: &[u8]) -> Option<Self> {
        if data.len() < 8 {
            return None;
        }
        
        let num_bits = u32::from_le_bytes(data[0..4].try_into().ok()?) as usize;
        let num_hashes = u32::from_le_bytes(data[4..8].try_into().ok()?);
        
        let num_words = (num_bits + 63) / 64;
        let expected_len = 8 + num_words * 8;
        
        if data.len() < expected_len {
            return None;
        }
        
        let mut bits = Vec::with_capacity(num_words);
        for i in 0..num_words {
            let offset = 8 + i * 8;
            let word = u64::from_le_bytes(data[offset..offset + 8].try_into().ok()?);
            bits.push(word);
        }
        
        Some(BloomFilter {
            bits,
            num_bits,
            num_hashes,
        })
    }

    /// Write to a writer.
    pub fn write_to<W: Write>(&self, writer: &mut W) -> std::io::Result<()> {
        let bytes = self.to_bytes();
        writer.write_all(&bytes)
    }

    /// Read from a reader.
    pub fn read_from<R: Read>(reader: &mut R) -> std::io::Result<Self> {
        // Read header
        let mut header = [0u8; 8];
        reader.read_exact(&mut header)?;
        
        let num_bits = u32::from_le_bytes(header[0..4].try_into().unwrap()) as usize;
        let num_hashes = u32::from_le_bytes(header[4..8].try_into().unwrap());
        
        let num_words = (num_bits + 63) / 64;
        let mut bits = Vec::with_capacity(num_words);
        
        for _ in 0..num_words {
            let mut word_bytes = [0u8; 8];
            reader.read_exact(&mut word_bytes)?;
            bits.push(u64::from_le_bytes(word_bytes));
        }
        
        Ok(BloomFilter {
            bits,
            num_bits,
            num_hashes,
        })
    }

    /// Compute two hash values for double hashing.
    fn hash_key(&self, key: &[u8]) -> (u64, u64) {
        // Use xxhash-style mixing for speed
        let mut h1: u64 = 0xcbf29ce484222325; // FNV offset
        let mut h2: u64 = 0x9e3779b97f4a7c15; // Golden ratio
        
        for &byte in key {
            h1 ^= byte as u64;
            h1 = h1.wrapping_mul(0x100000001b3); // FNV prime
            h2 = h2.wrapping_add(byte as u64);
            h2 = h2.rotate_left(31).wrapping_mul(0x85ebca6b);
        }
        
        // Final mix
        h1 ^= h1 >> 33;
        h1 = h1.wrapping_mul(0xff51afd7ed558ccd);
        h1 ^= h1 >> 33;
        
        h2 ^= h2 >> 33;
        h2 = h2.wrapping_mul(0xc4ceb9fe1a85ec53);
        h2 ^= h2 >> 33;
        
        (h1, h2)
    }

    /// Get bit index using double hashing: h1 + i*h2.
    fn get_bit_index(&self, h1: u64, h2: u64, i: u32) -> usize {
        let hash = h1.wrapping_add((i as u64).wrapping_mul(h2));
        (hash as usize) % self.num_bits
    }
}

/// Builder for creating bloom filters.
pub struct BloomFilterBuilder {
    keys: Vec<Vec<u8>>,
    false_positive_rate: f64,
}

impl BloomFilterBuilder {
    /// Create a new builder with the given false positive rate.
    pub fn new(false_positive_rate: f64) -> Self {
        BloomFilterBuilder {
            keys: Vec::new(),
            false_positive_rate,
        }
    }

    /// Add a key.
    pub fn add_key(&mut self, key: &[u8]) {
        self.keys.push(key.to_vec());
    }

    /// Build the filter.
    pub fn build(self) -> BloomFilter {
        let num_keys = self.keys.len().max(1);
        let mut filter = BloomFilter::new(num_keys, self.false_positive_rate);
        
        for key in &self.keys {
            filter.insert(key);
        }
        
        filter
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_bloom_filter_new() {
        let filter = BloomFilter::new(1000, 0.01);
        assert!(filter.num_bits() > 0);
        assert!(filter.num_hashes() > 0);
    }

    #[test]
    fn test_insert_and_lookup() {
        let mut filter = BloomFilter::new(100, 0.01);
        
        filter.insert(b"key1");
        filter.insert(b"key2");
        filter.insert(b"key3");
        
        assert!(filter.may_contain(b"key1"));
        assert!(filter.may_contain(b"key2"));
        assert!(filter.may_contain(b"key3"));
    }

    #[test]
    fn test_missing_key_returns_false() {
        let mut filter = BloomFilter::new(100, 0.01);
        
        filter.insert(b"existing");
        
        // Most non-existent keys should return false
        let mut false_negatives = 0;
        for i in 0..1000 {
            let key = format!("nonexistent-{}", i);
            if !filter.may_contain(key.as_bytes()) {
                false_negatives += 1;
            }
        }
        
        // Should have very few false positives with 0.01 rate
        assert!(false_negatives > 900, "Too many false positives");
    }

    #[test]
    fn test_serialization_roundtrip() {
        let mut filter = BloomFilter::new(100, 0.01);
        filter.insert(b"key1");
        filter.insert(b"key2");
        
        let bytes = filter.to_bytes();
        let restored = BloomFilter::from_bytes(&bytes).unwrap();
        
        assert_eq!(filter.num_bits(), restored.num_bits());
        assert_eq!(filter.num_hashes(), restored.num_hashes());
        assert!(restored.may_contain(b"key1"));
        assert!(restored.may_contain(b"key2"));
    }

    #[test]
    fn test_read_write_roundtrip() {
        let mut filter = BloomFilter::new(100, 0.01);
        filter.insert(b"test");
        
        let mut buf = Vec::new();
        filter.write_to(&mut buf).unwrap();
        
        let mut cursor = std::io::Cursor::new(buf);
        let restored = BloomFilter::read_from(&mut cursor).unwrap();
        
        assert!(restored.may_contain(b"test"));
    }

    #[test]
    fn test_builder() {
        let mut builder = BloomFilterBuilder::new(0.01);
        builder.add_key(b"a");
        builder.add_key(b"b");
        builder.add_key(b"c");
        
        let filter = builder.build();
        
        assert!(filter.may_contain(b"a"));
        assert!(filter.may_contain(b"b"));
        assert!(filter.may_contain(b"c"));
    }

    #[test]
    fn test_empty_filter() {
        let filter = BloomFilter::new(1, 0.01);
        
        // Empty filter should return false for all keys
        assert!(!filter.may_contain(b"anything"));
    }

    #[test]
    fn test_size_bytes() {
        let filter = BloomFilter::new(1000, 0.01);
        assert!(filter.size_bytes() > 0);
        assert_eq!(filter.size_bytes(), filter.bits.len() * 8);
    }

    #[test]
    fn test_with_params() {
        let filter = BloomFilter::with_params(1024, 7);
        assert_eq!(filter.num_bits(), 1024);
        assert_eq!(filter.num_hashes(), 7);
    }

    #[test]
    fn test_false_positive_rate() {
        // Create filter for 1000 keys with 1% FP rate
        let mut filter = BloomFilter::new(1000, 0.01);
        
        // Insert 1000 keys
        for i in 0..1000 {
            let key = format!("key-{}", i);
            filter.insert(key.as_bytes());
        }
        
        // Test 10000 non-existent keys
        let mut false_positives = 0;
        for i in 1000..11000 {
            let key = format!("key-{}", i);
            if filter.may_contain(key.as_bytes()) {
                false_positives += 1;
            }
        }
        
        // With 1% target, expect ~100 FPs, allow up to 3%
        let fp_rate = false_positives as f64 / 10000.0;
        assert!(fp_rate < 0.03, "FP rate {} too high", fp_rate);
    }
}
