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
    /// Compression algorithm.
    pub compression: Compression,
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

    /// Set compression.
    pub fn compression(mut self, compression: Compression) -> Self {
        self.compression = compression;
        self
    }

    /// Set create if missing.
    pub fn create_if_missing(mut self, create: bool) -> Self {
        self.create_if_missing = create;
        self
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
}
