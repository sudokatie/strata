//! SSTable builder.
//!
//! File format:
//! ```text
//! [data block 0]
//! [data block 1]
//! ...
//! [data block n]
//! [index block]
//! [bloom filter block]
//! [footer]
//! ```
//!
//! Footer (48 bytes):
//! - index_offset: u64
//! - index_size: u64
//! - bloom_offset: u64
//! - bloom_size: u64
//! - magic: u64 (0x5354524154414442 = "STRATADB")
//! - checksum: u64

use std::fs::File;
use std::io::{BufWriter, Write};
use std::path::Path;

use crate::types::{Key, Value};
use crate::Result;
use super::block::{BlockBuilder, BLOCK_SIZE};
use super::bloom::{BloomFilterBuilder, DEFAULT_BITS_PER_KEY};

/// Magic number for SSTable files.
const MAGIC: u64 = 0x5354524154414442; // "STRATADB"

/// Footer size in bytes.
pub const FOOTER_SIZE: usize = 48;

/// SSTable builder.
pub struct SSTableBuilder {
    writer: BufWriter<File>,
    data_block: BlockBuilder,
    index_block: BlockBuilder,
    bloom_builder: BloomFilterBuilder,
    pending_index_entry: Option<(Key, u64)>,
    offset: u64,
    num_entries: usize,
}

impl SSTableBuilder {
    /// Create a new SSTable builder.
    pub fn new(path: &Path) -> Result<Self> {
        let file = File::create(path)?;
        Ok(Self {
            writer: BufWriter::new(file),
            data_block: BlockBuilder::new(),
            index_block: BlockBuilder::new(),
            bloom_builder: BloomFilterBuilder::new(DEFAULT_BITS_PER_KEY),
            pending_index_entry: None,
            offset: 0,
            num_entries: 0,
        })
    }

    /// Add a key-value pair. Keys must be added in sorted order.
    pub fn add(&mut self, key: &Key, value: &Value) -> Result<()> {
        // Add to bloom filter
        self.bloom_builder.add(key.as_bytes());

        // If this is first key in a new block, record it for index
        if self.data_block.is_empty() {
            self.pending_index_entry = Some((key.clone(), self.offset));
        }

        // Add to current data block
        self.data_block.add(key, value);
        self.num_entries += 1;

        // Flush block if too large
        if self.data_block.size_estimate() >= BLOCK_SIZE {
            self.flush_data_block()?;
        }

        Ok(())
    }

    /// Finish building and close the file.
    pub fn finish(mut self) -> Result<SSTableMeta> {
        // Flush any remaining data block
        if !self.data_block.is_empty() {
            self.flush_data_block()?;
        }

        // Write index block
        let index_offset = self.offset;
        let index_data = self.index_block.finish();
        self.writer.write_all(&index_data)?;
        let index_size = index_data.len() as u64;
        self.offset += index_size;

        // Write bloom filter block
        let bloom_offset = self.offset;
        let bloom_builder = std::mem::take(&mut self.bloom_builder);
        let bloom_data = bloom_builder.build().encode();
        self.writer.write_all(&bloom_data)?;
        let bloom_size = bloom_data.len() as u64;
        self.offset += bloom_size;

        // Write footer
        let footer = Self::make_footer(index_offset, index_size, bloom_offset, bloom_size);
        self.writer.write_all(&footer)?;

        self.writer.flush()?;

        Ok(SSTableMeta {
            index_offset,
            index_size,
            bloom_offset,
            bloom_size,
            num_entries: self.num_entries,
        })
    }

    fn flush_data_block(&mut self) -> Result<()> {
        // Build and write block
        let mut block = BlockBuilder::new();
        std::mem::swap(&mut block, &mut self.data_block);
        let data = block.finish();

        self.writer.write_all(&data)?;

        // Add index entry for this block
        if let Some((key, offset)) = self.pending_index_entry.take() {
            // Store offset as 8-byte value
            let offset_bytes = offset.to_le_bytes();
            self.index_block.add(&key, &Value::new(offset_bytes.to_vec()));
        }

        self.offset += data.len() as u64;
        Ok(())
    }

    fn make_footer(
        index_offset: u64,
        index_size: u64,
        bloom_offset: u64,
        bloom_size: u64,
    ) -> Vec<u8> {
        let mut footer = Vec::with_capacity(FOOTER_SIZE);

        footer.extend_from_slice(&index_offset.to_le_bytes());
        footer.extend_from_slice(&index_size.to_le_bytes());
        footer.extend_from_slice(&bloom_offset.to_le_bytes());
        footer.extend_from_slice(&bloom_size.to_le_bytes());
        footer.extend_from_slice(&MAGIC.to_le_bytes());

        // Checksum of footer contents (before checksum field)
        let checksum = crc32fast::hash(&footer);
        footer.extend_from_slice(&(checksum as u64).to_le_bytes());

        footer
    }
}

/// Metadata returned after building an SSTable.
#[derive(Debug)]
pub struct SSTableMeta {
    pub index_offset: u64,
    pub index_size: u64,
    pub bloom_offset: u64,
    pub bloom_size: u64,
    pub num_entries: usize,
}

/// Parse footer from bytes.
pub fn parse_footer(data: &[u8]) -> Result<(u64, u64, u64, u64)> {
    if data.len() < FOOTER_SIZE {
        return Err(crate::Error::Corruption("footer too short".into()));
    }

    let footer = &data[data.len() - FOOTER_SIZE..];

    let index_offset = u64::from_le_bytes(footer[0..8].try_into().unwrap());
    let index_size = u64::from_le_bytes(footer[8..16].try_into().unwrap());
    let bloom_offset = u64::from_le_bytes(footer[16..24].try_into().unwrap());
    let bloom_size = u64::from_le_bytes(footer[24..32].try_into().unwrap());
    let magic = u64::from_le_bytes(footer[32..40].try_into().unwrap());
    let stored_checksum = u64::from_le_bytes(footer[40..48].try_into().unwrap());

    // Verify magic
    if magic != MAGIC {
        return Err(crate::Error::Corruption("invalid magic number".into()));
    }

    // Verify checksum
    let checksum = crc32fast::hash(&footer[..40]) as u64;
    if checksum != stored_checksum {
        return Err(crate::Error::Corruption("footer checksum mismatch".into()));
    }

    Ok((index_offset, index_size, bloom_offset, bloom_size))
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::NamedTempFile;

    #[test]
    fn test_build_empty() {
        let tmp = NamedTempFile::new().unwrap();
        let builder = SSTableBuilder::new(tmp.path()).unwrap();
        let meta = builder.finish().unwrap();
        assert_eq!(meta.num_entries, 0);
    }

    #[test]
    fn test_build_single_entry() {
        let tmp = NamedTempFile::new().unwrap();
        let mut builder = SSTableBuilder::new(tmp.path()).unwrap();
        builder.add(&Key::from("hello"), &Value::from("world")).unwrap();
        let meta = builder.finish().unwrap();
        assert_eq!(meta.num_entries, 1);
    }

    #[test]
    fn test_build_multiple_entries() {
        let tmp = NamedTempFile::new().unwrap();
        let mut builder = SSTableBuilder::new(tmp.path()).unwrap();

        for i in 0..100 {
            let key = format!("key{:03}", i);
            let value = format!("value{}", i);
            builder.add(&Key::from(key.as_str()), &Value::from(value.as_str())).unwrap();
        }

        let meta = builder.finish().unwrap();
        assert_eq!(meta.num_entries, 100);
    }

    #[test]
    fn test_build_large_spans_blocks() {
        let tmp = NamedTempFile::new().unwrap();
        let mut builder = SSTableBuilder::new(tmp.path()).unwrap();

        // Add enough entries to span multiple blocks
        for i in 0..1000 {
            let key = format!("key{:05}", i);
            let value = format!("value{:05}", i);
            builder.add(&Key::from(key.as_str()), &Value::from(value.as_str())).unwrap();
        }

        let meta = builder.finish().unwrap();
        assert_eq!(meta.num_entries, 1000);
        // Should have created multiple index entries
        assert!(meta.index_size > 0);
    }

    #[test]
    fn test_footer_parse() {
        let tmp = NamedTempFile::new().unwrap();
        let mut builder = SSTableBuilder::new(tmp.path()).unwrap();
        builder.add(&Key::from("test"), &Value::from("data")).unwrap();
        let _meta = builder.finish().unwrap();

        // Read file and parse footer
        let data = std::fs::read(tmp.path()).unwrap();
        let (index_offset, index_size, bloom_offset, bloom_size) = parse_footer(&data).unwrap();

        assert!(index_offset > 0);
        assert!(index_size > 0);
        assert!(bloom_offset > index_offset);
        assert!(bloom_size > 0);
    }
}
