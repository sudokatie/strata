//! SSTable reader.

use std::fs::File;
use std::io::{Read, Seek, SeekFrom};
use std::path::Path;

use crate::types::{Key, Value};
use crate::Result;
use super::block::Block;
use super::bloom::BloomFilter;
use super::builder::{parse_footer, FOOTER_SIZE};

/// SSTable reader.
pub struct SSTableReader {
    file: File,
    index_block: Block,
    bloom_filter: Option<BloomFilter>,
    file_size: u64,
}

impl SSTableReader {
    /// Open an SSTable file.
    pub fn open(path: &Path) -> Result<Self> {
        let mut file = File::open(path)?;
        let file_size = file.metadata()?.len();

        if file_size < FOOTER_SIZE as u64 {
            return Err(crate::Error::Corruption("file too small".into()));
        }

        // Read and parse footer
        file.seek(SeekFrom::End(-(FOOTER_SIZE as i64)))?;
        let mut footer_buf = vec![0u8; FOOTER_SIZE];
        file.read_exact(&mut footer_buf)?;

        let (index_offset, index_size, bloom_offset, bloom_size) =
            parse_footer(&footer_buf)?;

        // Read index block
        file.seek(SeekFrom::Start(index_offset))?;
        let mut index_data = vec![0u8; index_size as usize];
        file.read_exact(&mut index_data)?;
        let index_block = Block::new(index_data)?;

        // Read bloom filter
        let bloom_filter = if bloom_size > 0 {
            file.seek(SeekFrom::Start(bloom_offset))?;
            let mut bloom_data = vec![0u8; bloom_size as usize];
            file.read_exact(&mut bloom_data)?;
            BloomFilter::from_bytes(bloom_data)
        } else {
            None
        };

        Ok(Self {
            file,
            index_block,
            bloom_filter,
            file_size,
        })
    }

    /// Check if a key might be in this SSTable.
    pub fn may_contain(&self, key: &Key) -> bool {
        match &self.bloom_filter {
            Some(bf) => bf.may_contain(key.as_bytes()),
            None => true,
        }
    }

    /// Get value for a key.
    pub fn get(&mut self, key: &Key) -> Result<Option<Value>> {
        // Check bloom filter first
        if !self.may_contain(key) {
            return Ok(None);
        }

        // Binary search index to find candidate block
        let mut iter = self.index_block.seek(key);
        if !iter.valid() {
            // Key might be in last block
            iter = self.index_block.iter();
        }

        // Get block offset from index entry
        if let Some((_, offset_value)) = iter.next_entry() {
            let offset_bytes: [u8; 8] = offset_value.as_bytes()
                .try_into()
                .map_err(|_| crate::Error::Corruption("invalid index entry".into()))?;
            let block_offset = u64::from_le_bytes(offset_bytes);

            // Read and search the data block
            let block = self.read_block(block_offset)?;
            let mut block_iter = block.seek(key);

            if let Some((found_key, value)) = block_iter.next_entry() {
                if found_key == *key {
                    return Ok(Some(value));
                }
            }
        }

        Ok(None)
    }

    fn read_block(&mut self, offset: u64) -> Result<Block> {
        // Calculate block size (read until next block or index)
        // For simplicity, read a fixed max size and let Block handle it
        const MAX_BLOCK_SIZE: usize = 64 * 1024;

        self.file.seek(SeekFrom::Start(offset))?;
        let mut data = vec![0u8; MAX_BLOCK_SIZE];
        let n = self.file.read(&mut data)?;
        data.truncate(n);

        Block::new(data)
    }

    /// Iterate over all entries.
    pub fn iter(&mut self) -> Result<SSTableIterator<'_>> {
        SSTableIterator::new(self)
    }
}

/// Iterator over SSTable entries.
pub struct SSTableIterator<'a> {
    reader: &'a mut SSTableReader,
    index_iter: std::vec::IntoIter<(Key, u64)>,
    current_block: Option<Block>,
    block_iter: Option<Box<dyn Iterator<Item = (Key, Value)>>>,
}

impl<'a> SSTableIterator<'a> {
    fn new(reader: &'a mut SSTableReader) -> Result<Self> {
        // Collect all index entries
        let mut entries = Vec::new();
        for (key, value) in reader.index_block.iter() {
            let offset_bytes: [u8; 8] = value.as_bytes()
                .try_into()
                .map_err(|_| crate::Error::Corruption("invalid index entry".into()))?;
            let offset = u64::from_le_bytes(offset_bytes);
            entries.push((key, offset));
        }

        Ok(Self {
            reader,
            index_iter: entries.into_iter(),
            current_block: None,
            block_iter: None,
        })
    }
}

impl Iterator for SSTableIterator<'_> {
    type Item = Result<(Key, Value)>;

    fn next(&mut self) -> Option<Self::Item> {
        loop {
            // Try current block iterator
            if let Some(ref mut iter) = self.block_iter {
                if let Some((key, value)) = iter.next() {
                    return Some(Ok((key, value)));
                }
            }

            // Load next block
            let (_, offset) = self.index_iter.next()?;
            match self.reader.read_block(offset) {
                Ok(block) => {
                    self.current_block = Some(block);
                    let block_ref = self.current_block.as_ref().unwrap();
                    // Create iterator - need to collect since block_iter owns Block
                    let entries: Vec<_> = block_ref.iter().collect();
                    self.block_iter = Some(Box::new(entries.into_iter()));
                }
                Err(e) => return Some(Err(e)),
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use super::super::SSTableBuilder;
    use tempfile::NamedTempFile;

    #[test]
    fn test_read_empty() {
        let tmp = NamedTempFile::new().unwrap();
        let builder = SSTableBuilder::new(tmp.path()).unwrap();
        builder.finish().unwrap();

        let mut reader = SSTableReader::open(tmp.path()).unwrap();
        // Empty SSTable - get should return None
        assert!(reader.get(&Key::from("anything")).unwrap().is_none());
    }

    #[test]
    fn test_read_single() {
        let tmp = NamedTempFile::new().unwrap();
        let mut builder = SSTableBuilder::new(tmp.path()).unwrap();
        builder.add(&Key::from("hello"), &Value::from("world")).unwrap();
        builder.finish().unwrap();

        let mut reader = SSTableReader::open(tmp.path()).unwrap();
        let value = reader.get(&Key::from("hello")).unwrap().unwrap();
        assert_eq!(value.as_bytes(), b"world");
    }

    #[test]
    fn test_read_not_found() {
        let tmp = NamedTempFile::new().unwrap();
        let mut builder = SSTableBuilder::new(tmp.path()).unwrap();
        builder.add(&Key::from("hello"), &Value::from("world")).unwrap();
        builder.finish().unwrap();

        let mut reader = SSTableReader::open(tmp.path()).unwrap();
        let value = reader.get(&Key::from("missing")).unwrap();
        assert!(value.is_none());
    }

    #[test]
    fn test_read_multiple() {
        let tmp = NamedTempFile::new().unwrap();
        let mut builder = SSTableBuilder::new(tmp.path()).unwrap();

        for i in 0..100 {
            let key = format!("key{:03}", i);
            let value = format!("value{}", i);
            builder.add(&Key::from(key.as_str()), &Value::from(value.as_str())).unwrap();
        }
        builder.finish().unwrap();

        let mut reader = SSTableReader::open(tmp.path()).unwrap();

        // Test some lookups
        let v = reader.get(&Key::from("key000")).unwrap().unwrap();
        assert_eq!(v.as_bytes(), b"value0");

        let v = reader.get(&Key::from("key050")).unwrap().unwrap();
        assert_eq!(v.as_bytes(), b"value50");

        let v = reader.get(&Key::from("key099")).unwrap().unwrap();
        assert_eq!(v.as_bytes(), b"value99");
    }

    #[test]
    fn test_bloom_filter_skip() {
        let tmp = NamedTempFile::new().unwrap();
        let mut builder = SSTableBuilder::new(tmp.path()).unwrap();
        builder.add(&Key::from("exists"), &Value::from("yes")).unwrap();
        builder.finish().unwrap();

        let reader = SSTableReader::open(tmp.path()).unwrap();

        // Key that exists should pass bloom filter
        assert!(reader.may_contain(&Key::from("exists")));

        // Most non-existent keys should fail bloom filter
        // (some false positives expected)
    }
}
