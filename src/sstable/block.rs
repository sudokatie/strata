//! Block format for SSTables.
//!
//! Format:
//! ```text
//! [entries...][restarts...][num_restarts: u32]
//!
//! Entry:
//! [shared_len: varint][unshared_len: varint][value_len: varint]
//! [unshared_key_bytes][value_bytes]
//! ```

use crate::types::{Key, Value};
use crate::{Error, Result};

/// Restart interval for prefix compression.
const RESTART_INTERVAL: usize = 16;

/// Maximum block size (before restarts).
pub const BLOCK_SIZE: usize = 4 * 1024; // 4KB

/// Block builder for creating SSTable blocks.
pub struct BlockBuilder {
    buffer: Vec<u8>,
    restarts: Vec<u32>,
    last_key: Vec<u8>,
    counter: usize,
}

impl BlockBuilder {
    /// Create a new block builder.
    pub fn new() -> Self {
        
        Self {
            buffer: Vec::new(),
            restarts: vec![0], // First entry is always a restart
            last_key: Vec::new(),
            counter: 0,
        }
    }

    /// Add a key-value pair.
    pub fn add(&mut self, key: &Key, value: &Value) {
        let key_bytes = key.as_bytes();

        // Determine shared prefix length
        let shared = if self.counter.is_multiple_of(RESTART_INTERVAL) {
            // Restart point - no prefix sharing
            self.restarts.push(self.buffer.len() as u32);
            0
        } else {
            shared_prefix_len(&self.last_key, key_bytes)
        };

        let unshared = key_bytes.len() - shared;
        let value_len = value.as_bytes().len();

        // Write entry: shared_len | unshared_len | value_len | unshared_key | value
        self.put_varint(shared);
        self.put_varint(unshared);
        self.put_varint(value_len);
        self.buffer.extend_from_slice(&key_bytes[shared..]);
        self.buffer.extend_from_slice(value.as_bytes());

        self.last_key = key_bytes.to_vec();
        self.counter += 1;
    }

    /// Finish building the block.
    pub fn finish(mut self) -> Vec<u8> {
        // Remove the initial 0 if we added restarts
        if self.restarts.len() > 1 {
            self.restarts.remove(0);
        }

        // Append restart points
        for restart in &self.restarts {
            self.buffer.extend_from_slice(&restart.to_le_bytes());
        }

        // Append number of restarts
        self.buffer.extend_from_slice(&(self.restarts.len() as u32).to_le_bytes());

        self.buffer
    }

    /// Current size estimate.
    pub fn size_estimate(&self) -> usize {
        self.buffer.len() + self.restarts.len() * 4 + 4
    }

    /// Check if block is empty.
    pub fn is_empty(&self) -> bool {
        self.counter == 0
    }

    /// Reset the builder for reuse.
    pub fn reset(&mut self) {
        self.buffer.clear();
        self.restarts.clear();
        self.restarts.push(0);
        self.last_key.clear();
        self.counter = 0;
    }

    fn put_varint(&mut self, mut value: usize) {
        while value >= 0x80 {
            self.buffer.push((value as u8) | 0x80);
            value >>= 7;
        }
        self.buffer.push(value as u8);
    }
}

impl Default for BlockBuilder {
    fn default() -> Self {
        Self::new()
    }
}

/// Read a block.
pub struct Block {
    data: Vec<u8>,
    restarts_offset: usize,
    num_restarts: usize,
}

impl Block {
    /// Parse a block from bytes.
    pub fn new(data: Vec<u8>) -> Result<Self> {
        if data.len() < 4 {
            return Err(Error::Corruption("block too short".into()));
        }

        let num_restarts = u32::from_le_bytes([
            data[data.len() - 4],
            data[data.len() - 3],
            data[data.len() - 2],
            data[data.len() - 1],
        ]) as usize;

        if data.len() < 4 + num_restarts * 4 {
            return Err(Error::Corruption("block restart data truncated".into()));
        }

        let restarts_offset = data.len() - 4 - num_restarts * 4;

        Ok(Self {
            data,
            restarts_offset,
            num_restarts,
        })
    }

    /// Get restart point offset.
    fn restart(&self, index: usize) -> usize {
        let offset = self.restarts_offset + index * 4;
        u32::from_le_bytes([
            self.data[offset],
            self.data[offset + 1],
            self.data[offset + 2],
            self.data[offset + 3],
        ]) as usize
    }

    /// Iterate over all entries.
    pub fn iter(&self) -> BlockIterator<'_> {
        BlockIterator {
            block: self,
            offset: 0,
            key: Vec::new(),
        }
    }

    /// Binary search for a key using restart points.
    pub fn seek(&self, target: &Key) -> BlockIterator<'_> {
        let target_bytes = target.as_bytes();

        // Binary search over restart points
        let mut left = 0;
        let mut right = self.num_restarts;

        while left < right {
            let mid = left + (right - left).div_ceil(2);
            let restart_offset = self.restart(mid);

            // Decode key at restart point (full key, no shared prefix)
            let (key, _) = self.decode_entry_at(restart_offset).unwrap_or_default();

            if key.as_slice() < target_bytes {
                left = mid;
            } else {
                right = mid - 1;
            }
        }

        // Linear search from restart point
        let start_offset = self.restart(left);
        let mut offset = start_offset;
        let mut key = Vec::new();

        // Scan entries to find one >= target
        while offset < self.restarts_offset {
            let entry_offset = offset;

            // Parse entry header
            let (shared, o1) = match get_varint(&self.data[offset..]) {
                Some(v) => v,
                None => break,
            };
            let (unshared, o2) = match get_varint(&self.data[offset + o1..]) {
                Some(v) => v,
                None => break,
            };
            let (value_len, o3) = match get_varint(&self.data[offset + o1 + o2..]) {
                Some(v) => v,
                None => break,
            };

            let key_start = offset + o1 + o2 + o3;
            let value_start = key_start + unshared;
            let value_end = value_start + value_len;

            if value_end > self.restarts_offset {
                break;
            }

            // Build key
            key.truncate(shared);
            key.extend_from_slice(&self.data[key_start..value_start]);

            if key.as_slice() >= target_bytes {
                // Found - return iterator positioned at this entry
                return BlockIterator {
                    block: self,
                    offset: entry_offset,
                    key,
                };
            }

            offset = value_end;
        }

        // Not found - return invalid iterator
        BlockIterator {
            block: self,
            offset: self.restarts_offset,
            key: Vec::new(),
        }
    }

    fn decode_entry_at(&self, offset: usize) -> Option<(Vec<u8>, Vec<u8>)> {
        if offset >= self.restarts_offset {
            return None;
        }

        let (_shared, o1) = get_varint(&self.data[offset..])?;
        let (unshared, o2) = get_varint(&self.data[offset + o1..])?;
        let (value_len, o3) = get_varint(&self.data[offset + o1 + o2..])?;

        let key_start = offset + o1 + o2 + o3;
        let value_start = key_start + unshared;
        let value_end = value_start + value_len;

        if value_end > self.restarts_offset {
            return None;
        }

        // At restart points, shared is 0 so we get full key
        let key = self.data[key_start..value_start].to_vec();
        let value = self.data[value_start..value_end].to_vec();

        Some((key, value))
    }
}

/// Iterator over block entries.
pub struct BlockIterator<'a> {
    block: &'a Block,
    offset: usize,
    key: Vec<u8>,
}

impl<'a> BlockIterator<'a> {
    /// Check if iterator is valid.
    pub fn valid(&self) -> bool {
        self.offset < self.block.restarts_offset
    }

    /// Current key (only valid after read_current()).
    pub fn key(&self) -> &[u8] {
        &self.key
    }

    /// Read current entry and advance offset.
    /// Returns (key, value) if successful.
    fn read_current(&mut self) -> Option<(Vec<u8>, Vec<u8>)> {
        if !self.valid() {
            return None;
        }

        let (shared, o1) = get_varint(&self.block.data[self.offset..])?;
        let (unshared, o2) = get_varint(&self.block.data[self.offset + o1..])?;
        let (value_len, o3) = get_varint(&self.block.data[self.offset + o1 + o2..])?;

        let key_start = self.offset + o1 + o2 + o3;
        let value_start = key_start + unshared;
        let value_end = value_start + value_len;

        if value_end > self.block.restarts_offset {
            self.offset = self.block.restarts_offset;
            return None;
        }

        // Build full key from prefix
        self.key.truncate(shared);
        self.key.extend_from_slice(&self.block.data[key_start..value_start]);

        let value = self.block.data[value_start..value_end].to_vec();

        // Advance offset for next read
        self.offset = value_end;

        Some((self.key.clone(), value))
    }

    /// Move to next entry (for manual iteration).
    pub fn next_entry(&mut self) -> Option<(Key, Value)> {
        let (key, value) = self.read_current()?;
        Some((Key::new(key), Value::new(value)))
    }
}

impl<'a> Iterator for BlockIterator<'a> {
    type Item = (Key, Value);

    fn next(&mut self) -> Option<Self::Item> {
        self.next_entry()
    }
}

/// Calculate shared prefix length.
fn shared_prefix_len(a: &[u8], b: &[u8]) -> usize {
    a.iter().zip(b.iter()).take_while(|(x, y)| x == y).count()
}

/// Decode a varint, return (value, bytes_read).
fn get_varint(data: &[u8]) -> Option<(usize, usize)> {
    let mut result = 0usize;
    let mut shift = 0;

    for (i, &byte) in data.iter().enumerate() {
        result |= ((byte & 0x7F) as usize) << shift;
        if byte & 0x80 == 0 {
            return Some((result, i + 1));
        }
        shift += 7;
        if shift >= 64 {
            return None;
        }
    }

    None
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_block_single_entry() {
        let mut builder = BlockBuilder::new();
        builder.add(&Key::from("hello"), &Value::from("world"));
        let data = builder.finish();

        let block = Block::new(data).unwrap();
        let entries: Vec<_> = block.iter().collect();
        assert_eq!(entries.len(), 1);
        assert_eq!(entries[0].0.as_bytes(), b"hello");
        assert_eq!(entries[0].1.as_bytes(), b"world");
    }

    #[test]
    fn test_block_multiple_entries() {
        let mut builder = BlockBuilder::new();
        builder.add(&Key::from("apple"), &Value::from("1"));
        builder.add(&Key::from("apricot"), &Value::from("2"));
        builder.add(&Key::from("banana"), &Value::from("3"));
        let data = builder.finish();

        let block = Block::new(data).unwrap();
        let entries: Vec<_> = block.iter().collect();
        assert_eq!(entries.len(), 3);
        assert_eq!(entries[0].0.as_bytes(), b"apple");
        assert_eq!(entries[1].0.as_bytes(), b"apricot");
        assert_eq!(entries[2].0.as_bytes(), b"banana");
    }

    #[test]
    fn test_block_prefix_compression() {
        let mut builder = BlockBuilder::new();
        // Keys with common prefix should compress
        builder.add(&Key::from("prefix_aaa"), &Value::from("1"));
        builder.add(&Key::from("prefix_aab"), &Value::from("2"));
        builder.add(&Key::from("prefix_aac"), &Value::from("3"));
        let data = builder.finish();

        let block = Block::new(data).unwrap();
        let entries: Vec<_> = block.iter().collect();
        assert_eq!(entries.len(), 3);
        assert_eq!(entries[0].0.as_bytes(), b"prefix_aaa");
        assert_eq!(entries[1].0.as_bytes(), b"prefix_aab");
        assert_eq!(entries[2].0.as_bytes(), b"prefix_aac");
    }

    #[test]
    fn test_block_seek() {
        let mut builder = BlockBuilder::new();
        for i in 0..100 {
            let key = format!("key{:03}", i);
            builder.add(&Key::from(key.as_str()), &Value::from("v"));
        }
        let data = builder.finish();

        let block = Block::new(data).unwrap();

        // Seek to existing key
        let iter = block.seek(&Key::from("key050"));
        assert!(iter.valid());
        assert_eq!(iter.key(), b"key050");

        // Seek to non-existing key (should land on next)
        let iter = block.seek(&Key::from("key050a"));
        assert!(iter.valid());
        assert_eq!(iter.key(), b"key051");
    }

    #[test]
    fn test_block_empty() {
        let builder = BlockBuilder::new();
        assert!(builder.is_empty());
    }

    #[test]
    fn test_block_restart_points() {
        let mut builder = BlockBuilder::new();
        // Add more than RESTART_INTERVAL entries
        for i in 0..50 {
            let key = format!("key{:03}", i);
            builder.add(&Key::from(key.as_str()), &Value::from("value"));
        }
        let data = builder.finish();

        let block = Block::new(data).unwrap();
        // Should have multiple restart points
        assert!(block.num_restarts > 1);

        // Iteration should still work
        let entries: Vec<_> = block.iter().collect();
        assert_eq!(entries.len(), 50);
    }
}
