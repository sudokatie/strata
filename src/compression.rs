//! Block compression for SSTables.
//!
//! Supports Snappy (fast) and Zstd (better ratio) compression.

use crate::options::Compression;
use crate::{Error, Result};
use std::sync::atomic::{AtomicU64, Ordering};

/// Compression statistics.
#[derive(Debug, Default)]
pub struct CompressionStats {
    /// Total bytes before compression.
    pub raw_bytes: AtomicU64,
    /// Total bytes after compression.
    pub compressed_bytes: AtomicU64,
    /// Number of blocks compressed.
    pub blocks_compressed: AtomicU64,
    /// Number of blocks decompressed.
    pub blocks_decompressed: AtomicU64,
}

impl CompressionStats {
    /// Create new stats.
    pub fn new() -> Self {
        Self::default()
    }

    /// Record a compression operation.
    pub fn record_compress(&self, raw_size: usize, compressed_size: usize) {
        self.raw_bytes.fetch_add(raw_size as u64, Ordering::Relaxed);
        self.compressed_bytes
            .fetch_add(compressed_size as u64, Ordering::Relaxed);
        self.blocks_compressed.fetch_add(1, Ordering::Relaxed);
    }

    /// Record a decompression operation.
    pub fn record_decompress(&self) {
        self.blocks_decompressed.fetch_add(1, Ordering::Relaxed);
    }

    /// Get compression ratio (compressed/raw). Lower is better.
    pub fn ratio(&self) -> f64 {
        let raw = self.raw_bytes.load(Ordering::Relaxed) as f64;
        let compressed = self.compressed_bytes.load(Ordering::Relaxed) as f64;
        if raw > 0.0 {
            compressed / raw
        } else {
            1.0
        }
    }

    /// Get space savings percentage.
    pub fn savings_percent(&self) -> f64 {
        (1.0 - self.ratio()) * 100.0
    }

    /// Get total raw bytes.
    pub fn total_raw(&self) -> u64 {
        self.raw_bytes.load(Ordering::Relaxed)
    }

    /// Get total compressed bytes.
    pub fn total_compressed(&self) -> u64 {
        self.compressed_bytes.load(Ordering::Relaxed)
    }

    /// Get blocks compressed count.
    pub fn blocks_in(&self) -> u64 {
        self.blocks_compressed.load(Ordering::Relaxed)
    }

    /// Get blocks decompressed count.
    pub fn blocks_out(&self) -> u64 {
        self.blocks_decompressed.load(Ordering::Relaxed)
    }

    /// Reset all stats.
    pub fn reset(&self) {
        self.raw_bytes.store(0, Ordering::Relaxed);
        self.compressed_bytes.store(0, Ordering::Relaxed);
        self.blocks_compressed.store(0, Ordering::Relaxed);
        self.blocks_decompressed.store(0, Ordering::Relaxed);
    }
}

/// Compress data using the specified algorithm.
///
/// Returns (compressed_data, compression_type_byte).
/// The type byte is stored with the block for decoding.
pub fn compress(data: &[u8], compression: Compression) -> Result<(Vec<u8>, u8)> {
    match compression {
        Compression::None => Ok((data.to_vec(), 0x00)),
        Compression::Snappy => {
            let compressed = snap::raw::Encoder::new()
                .compress_vec(data)
                .map_err(|e| Error::Io(std::io::Error::other(e)))?;
            Ok((compressed, 0x01))
        }
        Compression::Zstd => {
            // Use compression level 3 for good balance of speed/ratio
            let compressed = zstd::encode_all(data, 3)
                .map_err(|e| Error::Io(std::io::Error::other(e)))?;
            Ok((compressed, 0x02))
        }
    }
}

/// Decompress data based on the compression type byte.
pub fn decompress(data: &[u8], compression_type: u8) -> Result<Vec<u8>> {
    match compression_type {
        0x00 => Ok(data.to_vec()),
        0x01 => snap::raw::Decoder::new()
            .decompress_vec(data)
            .map_err(|e| Error::Io(std::io::Error::other(e))),
        0x02 => zstd::decode_all(data)
            .map_err(|e| Error::Io(std::io::Error::other(e))),
        _ => Err(Error::Corruption(format!(
            "unknown compression type: {}",
            compression_type
        ))),
    }
}

/// Get the compression type from its byte representation.
pub fn compression_from_byte(byte: u8) -> Option<Compression> {
    match byte {
        0x00 => Some(Compression::None),
        0x01 => Some(Compression::Snappy),
        0x02 => Some(Compression::Zstd),
        _ => None,
    }
}

/// Get the byte representation of a compression type.
pub fn compression_to_byte(compression: Compression) -> u8 {
    match compression {
        Compression::None => 0x00,
        Compression::Snappy => 0x01,
        Compression::Zstd => 0x02,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_compress_none() {
        let data = b"hello world";
        let (compressed, typ) = compress(data, Compression::None).unwrap();
        assert_eq!(typ, 0x00);
        assert_eq!(compressed, data);
    }

    #[test]
    fn test_compress_snappy() {
        let data = b"hello world hello world hello world";
        let (compressed, typ) = compress(data, Compression::Snappy).unwrap();
        assert_eq!(typ, 0x01);
        // Snappy should compress this repetitive data
        assert!(compressed.len() < data.len());
    }

    #[test]
    fn test_compress_zstd() {
        let data = b"hello world hello world hello world";
        let (compressed, typ) = compress(data, Compression::Zstd).unwrap();
        assert_eq!(typ, 0x02);
        // Zstd should compress this repetitive data
        assert!(compressed.len() < data.len());
    }

    #[test]
    fn test_decompress_none() {
        let data = b"hello world";
        let decompressed = decompress(data, 0x00).unwrap();
        assert_eq!(decompressed, data);
    }

    #[test]
    fn test_roundtrip_snappy() {
        let data = b"The quick brown fox jumps over the lazy dog";
        let (compressed, typ) = compress(data, Compression::Snappy).unwrap();
        let decompressed = decompress(&compressed, typ).unwrap();
        assert_eq!(decompressed, data);
    }

    #[test]
    fn test_roundtrip_zstd() {
        let data = b"The quick brown fox jumps over the lazy dog";
        let (compressed, typ) = compress(data, Compression::Zstd).unwrap();
        let decompressed = decompress(&compressed, typ).unwrap();
        assert_eq!(decompressed, data);
    }

    #[test]
    fn test_decompress_unknown_type() {
        let data = b"hello";
        let result = decompress(data, 0xFF);
        assert!(result.is_err());
    }

    #[test]
    fn test_compression_stats() {
        let stats = CompressionStats::new();
        
        stats.record_compress(1000, 500);
        stats.record_compress(2000, 1000);
        
        assert_eq!(stats.total_raw(), 3000);
        assert_eq!(stats.total_compressed(), 1500);
        assert_eq!(stats.blocks_in(), 2);
        assert!((stats.ratio() - 0.5).abs() < 0.001);
        assert!((stats.savings_percent() - 50.0).abs() < 0.001);
    }

    #[test]
    fn test_compression_stats_reset() {
        let stats = CompressionStats::new();
        stats.record_compress(1000, 500);
        stats.reset();
        
        assert_eq!(stats.total_raw(), 0);
        assert_eq!(stats.total_compressed(), 0);
    }

    #[test]
    fn test_large_data_snappy() {
        // Test with larger data that should definitely compress
        let data: Vec<u8> = (0..10000).map(|i| (i % 256) as u8).collect();
        let (compressed, typ) = compress(&data, Compression::Snappy).unwrap();
        let decompressed = decompress(&compressed, typ).unwrap();
        assert_eq!(decompressed, data);
    }

    #[test]
    fn test_large_data_zstd() {
        // Test with larger data that should definitely compress
        let data: Vec<u8> = (0..10000).map(|i| (i % 256) as u8).collect();
        let (compressed, typ) = compress(&data, Compression::Zstd).unwrap();
        let decompressed = decompress(&compressed, typ).unwrap();
        assert_eq!(decompressed, data);
    }

    #[test]
    fn test_compression_from_byte() {
        assert_eq!(compression_from_byte(0x00), Some(Compression::None));
        assert_eq!(compression_from_byte(0x01), Some(Compression::Snappy));
        assert_eq!(compression_from_byte(0x02), Some(Compression::Zstd));
        assert_eq!(compression_from_byte(0xFF), None);
    }

    #[test]
    fn test_compression_to_byte() {
        assert_eq!(compression_to_byte(Compression::None), 0x00);
        assert_eq!(compression_to_byte(Compression::Snappy), 0x01);
        assert_eq!(compression_to_byte(Compression::Zstd), 0x02);
    }
}
