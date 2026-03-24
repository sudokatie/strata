//! Block compression support.

use crate::options::Compression;
use crate::Result;

/// Compress data using the specified algorithm.
pub fn compress(data: &[u8], compression: Compression) -> Result<Vec<u8>> {
    match compression {
        Compression::None => Ok(data.to_vec()),
        Compression::Snappy => {
            let mut encoder = snap::raw::Encoder::new();
            encoder
                .compress_vec(data)
                .map_err(|e| crate::Error::Io(std::io::Error::other(e.to_string())))
        }
        Compression::Zstd => {
            zstd::encode_all(data, 3)
                .map_err(crate::Error::Io)
        }
    }
}

/// Decompress data using the specified algorithm.
pub fn decompress(data: &[u8], compression: Compression) -> Result<Vec<u8>> {
    match compression {
        Compression::None => Ok(data.to_vec()),
        Compression::Snappy => {
            let mut decoder = snap::raw::Decoder::new();
            decoder
                .decompress_vec(data)
                .map_err(|e| crate::Error::Io(std::io::Error::other(e.to_string())))
        }
        Compression::Zstd => {
            zstd::decode_all(data)
                .map_err(crate::Error::Io)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_no_compression() {
        let data = b"hello world";
        let compressed = compress(data, Compression::None).unwrap();
        let decompressed = decompress(&compressed, Compression::None).unwrap();
        assert_eq!(decompressed, data);
    }

    #[test]
    fn test_snappy_compression() {
        let data = b"hello world hello world hello world";
        let compressed = compress(data, Compression::Snappy).unwrap();
        let decompressed = decompress(&compressed, Compression::Snappy).unwrap();
        assert_eq!(decompressed, data);
        // Snappy should compress repetitive data
        assert!(compressed.len() < data.len());
    }

    #[test]
    fn test_zstd_compression() {
        let data = b"hello world hello world hello world";
        let compressed = compress(data, Compression::Zstd).unwrap();
        let decompressed = decompress(&compressed, Compression::Zstd).unwrap();
        assert_eq!(decompressed, data);
        // Zstd should compress repetitive data
        assert!(compressed.len() < data.len());
    }

    #[test]
    fn test_large_data_compression() {
        // Test with larger data
        let data: Vec<u8> = (0..10000).map(|i| (i % 256) as u8).collect();
        
        for compression in [Compression::None, Compression::Snappy, Compression::Zstd] {
            let compressed = compress(&data, compression).unwrap();
            let decompressed = decompress(&compressed, compression).unwrap();
            assert_eq!(decompressed, data);
        }
    }
}
