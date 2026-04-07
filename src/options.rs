//! Configuration options.

/// Compression algorithm.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum Compression {
    /// No compression.
    #[default]
    None,
    /// Snappy compression.
    Snappy,
    /// Zstd compression.
    Zstd,
}

/// Per-level compression configuration.
///
/// Allows different compression algorithms for different LSM levels.
/// Common strategy: fast compression (Snappy) for L0-L2, better ratio (Zstd) for L3+.
#[derive(Debug, Clone)]
pub struct PerLevelCompression {
    /// Compression per level. Index = level number.
    levels: Vec<Compression>,
    /// Default compression for levels not explicitly configured.
    default: Compression,
}

impl Default for PerLevelCompression {
    fn default() -> Self {
        Self {
            levels: Vec::new(),
            default: Compression::None,
        }
    }
}

impl PerLevelCompression {
    /// Create with a default compression for all levels.
    pub fn new(default: Compression) -> Self {
        Self {
            levels: Vec::new(),
            default,
        }
    }

    /// Set compression for a specific level.
    pub fn set_level(mut self, level: usize, compression: Compression) -> Self {
        if level >= self.levels.len() {
            self.levels.resize(level + 1, self.default);
        }
        self.levels[level] = compression;
        self
    }

    /// Get compression for a level.
    pub fn for_level(&self, level: usize) -> Compression {
        self.levels.get(level).copied().unwrap_or(self.default)
    }

    /// Create a balanced configuration: Snappy for L0-L2, Zstd for deeper levels.
    pub fn balanced() -> Self {
        Self {
            levels: vec![
                Compression::Snappy, // L0
                Compression::Snappy, // L1
                Compression::Snappy, // L2
            ],
            default: Compression::Zstd,
        }
    }

    /// Create a fast configuration: Snappy everywhere.
    pub fn fast() -> Self {
        Self::new(Compression::Snappy)
    }

    /// Create a compact configuration: Zstd everywhere.
    pub fn compact() -> Self {
        Self::new(Compression::Zstd)
    }
}

/// Database options.
#[derive(Debug, Clone)]
pub struct Options {
    /// MemTable size limit before flush (default 4MB).
    pub memtable_size: usize,
    /// Data block size (default 4KB).
    pub block_size: usize,
    /// Bloom filter bits per key (default 10).
    pub bloom_bits_per_key: usize,
    /// L0 file count trigger for compaction (default 4).
    pub l0_compaction_trigger: usize,
    /// Size ratio between levels (default 10).
    pub level_ratio: usize,
    /// Maximum number of levels (default 7).
    pub max_levels: usize,
    /// Compression algorithm (default for all levels).
    pub compression: Compression,
    /// Per-level compression configuration (overrides `compression` when set).
    pub per_level_compression: Option<PerLevelCompression>,
    /// Create database if missing.
    pub create_if_missing: bool,
    /// Error if database exists.
    pub error_if_exists: bool,
    /// Write buffer size (default 4MB).
    pub write_buffer_size: usize,
    /// Max open files (default 1000).
    pub max_open_files: usize,
}

impl Default for Options {
    fn default() -> Self {
        Self {
            memtable_size: 4 * 1024 * 1024,       // 4MB
            block_size: 4 * 1024,                  // 4KB
            bloom_bits_per_key: 10,
            l0_compaction_trigger: 4,
            level_ratio: 10,
            max_levels: 7,
            compression: Compression::None,
            per_level_compression: None,
            create_if_missing: true,
            error_if_exists: false,
            write_buffer_size: 4 * 1024 * 1024,   // 4MB
            max_open_files: 1000,
        }
    }
}

impl Options {
    /// Create new options with defaults.
    pub fn new() -> Self {
        Self::default()
    }

    /// Set memtable size.
    pub fn memtable_size(mut self, size: usize) -> Self {
        self.memtable_size = size;
        self
    }

    /// Set block size.
    pub fn block_size(mut self, size: usize) -> Self {
        self.block_size = size;
        self
    }

    /// Set compression (applies to all levels).
    pub fn compression(mut self, compression: Compression) -> Self {
        self.compression = compression;
        self
    }

    /// Set per-level compression configuration.
    pub fn per_level_compression(mut self, config: PerLevelCompression) -> Self {
        self.per_level_compression = Some(config);
        self
    }

    /// Set create if missing.
    pub fn create_if_missing(mut self, create: bool) -> Self {
        self.create_if_missing = create;
        self
    }

    /// Get compression for a specific level.
    ///
    /// Uses per-level configuration if set, otherwise falls back to global compression.
    pub fn compression_for_level(&self, level: usize) -> Compression {
        if let Some(ref per_level) = self.per_level_compression {
            per_level.for_level(level)
        } else {
            self.compression
        }
    }
}

/// Read options.
#[derive(Debug, Clone, Default)]
pub struct ReadOptions {
    /// Verify checksums on read.
    pub verify_checksums: bool,
    /// Fill cache on read.
    pub fill_cache: bool,
    /// Snapshot to read from (None for current).
    pub snapshot: Option<u64>,
}

impl ReadOptions {
    /// Create new read options.
    pub fn new() -> Self {
        Self::default()
    }

    /// Set verify checksums.
    pub fn verify_checksums(mut self, verify: bool) -> Self {
        self.verify_checksums = verify;
        self
    }
}

/// Write options.
#[derive(Debug, Clone, Default)]
pub struct WriteOptions {
    /// Sync writes to disk.
    pub sync: bool,
}

impl WriteOptions {
    /// Create new write options.
    pub fn new() -> Self {
        Self::default()
    }

    /// Set sync.
    pub fn sync(mut self, sync: bool) -> Self {
        self.sync = sync;
        self
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_default_options() {
        let opts = Options::default();
        assert_eq!(opts.memtable_size, 4 * 1024 * 1024);
        assert_eq!(opts.block_size, 4 * 1024);
        assert!(opts.create_if_missing);
    }

    #[test]
    fn test_options_builder() {
        let opts = Options::new()
            .memtable_size(8 * 1024 * 1024)
            .compression(Compression::Snappy);

        assert_eq!(opts.memtable_size, 8 * 1024 * 1024);
        assert_eq!(opts.compression, Compression::Snappy);
    }

    #[test]
    fn test_read_options() {
        let opts = ReadOptions::new().verify_checksums(true);
        assert!(opts.verify_checksums);
    }

    #[test]
    fn test_write_options() {
        let opts = WriteOptions::new().sync(true);
        assert!(opts.sync);
    }

    #[test]
    fn test_per_level_compression_default() {
        let config = PerLevelCompression::new(Compression::Snappy);
        assert_eq!(config.for_level(0), Compression::Snappy);
        assert_eq!(config.for_level(5), Compression::Snappy);
        assert_eq!(config.for_level(100), Compression::Snappy);
    }

    #[test]
    fn test_per_level_compression_set_level() {
        let config = PerLevelCompression::new(Compression::None)
            .set_level(0, Compression::Snappy)
            .set_level(3, Compression::Zstd);

        assert_eq!(config.for_level(0), Compression::Snappy);
        assert_eq!(config.for_level(1), Compression::None);
        assert_eq!(config.for_level(2), Compression::None);
        assert_eq!(config.for_level(3), Compression::Zstd);
        assert_eq!(config.for_level(4), Compression::None);
    }

    #[test]
    fn test_per_level_compression_balanced() {
        let config = PerLevelCompression::balanced();
        assert_eq!(config.for_level(0), Compression::Snappy);
        assert_eq!(config.for_level(1), Compression::Snappy);
        assert_eq!(config.for_level(2), Compression::Snappy);
        assert_eq!(config.for_level(3), Compression::Zstd);
        assert_eq!(config.for_level(6), Compression::Zstd);
    }

    #[test]
    fn test_per_level_compression_fast() {
        let config = PerLevelCompression::fast();
        assert_eq!(config.for_level(0), Compression::Snappy);
        assert_eq!(config.for_level(5), Compression::Snappy);
    }

    #[test]
    fn test_per_level_compression_compact() {
        let config = PerLevelCompression::compact();
        assert_eq!(config.for_level(0), Compression::Zstd);
        assert_eq!(config.for_level(5), Compression::Zstd);
    }

    #[test]
    fn test_options_compression_for_level_global() {
        let opts = Options::new().compression(Compression::Zstd);
        assert_eq!(opts.compression_for_level(0), Compression::Zstd);
        assert_eq!(opts.compression_for_level(3), Compression::Zstd);
    }

    #[test]
    fn test_options_compression_for_level_per_level() {
        let opts = Options::new()
            .compression(Compression::None)
            .per_level_compression(PerLevelCompression::balanced());

        assert_eq!(opts.compression_for_level(0), Compression::Snappy);
        assert_eq!(opts.compression_for_level(2), Compression::Snappy);
        assert_eq!(opts.compression_for_level(3), Compression::Zstd);
        assert_eq!(opts.compression_for_level(6), Compression::Zstd);
    }
}
