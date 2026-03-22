//! MemTable - in-memory sorted buffer for writes.

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

use crate::types::{Key, Value, Entry, EntryType, Sequence};
use super::skiplist::SkipList;

/// MemTable for buffering writes before flush to SSTable.
pub struct MemTable {
    list: SkipList,
    /// Next sequence number.
    next_seq: AtomicU64,
}

impl MemTable {
    /// Create a new memtable.
    pub fn new() -> Self {
        Self {
            list: SkipList::new(),
            next_seq: AtomicU64::new(1),
        }
    }

    /// Create with a starting sequence number.
    pub fn with_sequence(seq: Sequence) -> Self {
        Self {
            list: SkipList::new(),
            next_seq: AtomicU64::new(seq),
        }
    }

    /// Put a key-value pair.
    pub fn put(&mut self, key: Key, value: Value) {
        let seq = self.next_seq.fetch_add(1, Ordering::SeqCst);
        self.list.insert(key, value, seq, EntryType::Put);
    }

    /// Delete a key (tombstone).
    pub fn delete(&mut self, key: Key) {
        let seq = self.next_seq.fetch_add(1, Ordering::SeqCst);
        self.list.insert(key, Value::new(vec![]), seq, EntryType::Delete);
    }

    /// Get the latest value for a key.
    /// Returns None if not found or if deleted.
    pub fn get(&self, key: &Key) -> Option<Value> {
        match self.list.get(key) {
            Some(entry) => match entry.entry_type {
                EntryType::Put => Some(entry.value),
                EntryType::Delete => None,
            },
            None => None,
        }
    }

    /// Get entry (including tombstones).
    pub fn get_entry(&self, key: &Key) -> Option<Entry> {
        self.list.get(key)
    }

    /// Approximate memory usage in bytes.
    pub fn approximate_size(&self) -> usize {
        self.list.approximate_size()
    }

    /// Number of entries (including tombstones).
    pub fn len(&self) -> usize {
        self.list.len()
    }

    /// Check if empty.
    pub fn is_empty(&self) -> bool {
        self.list.is_empty()
    }

    /// Current sequence number.
    pub fn sequence(&self) -> Sequence {
        self.next_seq.load(Ordering::SeqCst)
    }

    /// Iterate over all entries in sorted order.
    pub fn iter(&self) -> impl Iterator<Item = Entry> + '_ {
        self.list.iter()
    }

    /// Freeze into immutable memtable for flushing.
    pub fn freeze(self) -> ImmutableMemTable {
        ImmutableMemTable {
            list: Arc::new(self.list),
            sequence: self.next_seq.load(Ordering::SeqCst),
        }
    }
}

impl Default for MemTable {
    fn default() -> Self {
        Self::new()
    }
}

/// Immutable memtable ready for flushing to SSTable.
pub struct ImmutableMemTable {
    list: Arc<SkipList>,
    sequence: Sequence,
}

impl ImmutableMemTable {
    /// Get value for key.
    pub fn get(&self, key: &Key) -> Option<Value> {
        match self.list.get(key) {
            Some(entry) => match entry.entry_type {
                EntryType::Put => Some(entry.value),
                EntryType::Delete => None,
            },
            None => None,
        }
    }

    /// Get entry (including tombstones).
    pub fn get_entry(&self, key: &Key) -> Option<Entry> {
        self.list.get(key)
    }

    /// Approximate memory usage.
    pub fn approximate_size(&self) -> usize {
        self.list.approximate_size()
    }

    /// Number of entries.
    pub fn len(&self) -> usize {
        self.list.len()
    }

    /// Check if empty.
    pub fn is_empty(&self) -> bool {
        self.list.is_empty()
    }

    /// Sequence when frozen.
    pub fn sequence(&self) -> Sequence {
        self.sequence
    }

    /// Iterate over all entries.
    pub fn iter(&self) -> impl Iterator<Item = Entry> + '_ {
        self.list.iter()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_memtable_put_get() {
        let mut mt = MemTable::new();
        mt.put(Key::from("foo"), Value::from("bar"));
        mt.put(Key::from("baz"), Value::from("qux"));

        assert_eq!(mt.get(&Key::from("foo")).unwrap().as_bytes(), b"bar");
        assert_eq!(mt.get(&Key::from("baz")).unwrap().as_bytes(), b"qux");
        assert!(mt.get(&Key::from("missing")).is_none());
    }

    #[test]
    fn test_memtable_overwrite() {
        let mut mt = MemTable::new();
        mt.put(Key::from("key"), Value::from("old"));
        mt.put(Key::from("key"), Value::from("new"));

        assert_eq!(mt.get(&Key::from("key")).unwrap().as_bytes(), b"new");
    }

    #[test]
    fn test_memtable_delete() {
        let mut mt = MemTable::new();
        mt.put(Key::from("key"), Value::from("value"));
        assert!(mt.get(&Key::from("key")).is_some());

        mt.delete(Key::from("key"));
        assert!(mt.get(&Key::from("key")).is_none());

        // Entry should still exist as tombstone
        let entry = mt.get_entry(&Key::from("key")).unwrap();
        assert_eq!(entry.entry_type, EntryType::Delete);
    }

    #[test]
    fn test_memtable_size_tracking() {
        let mut mt = MemTable::new();
        assert_eq!(mt.len(), 0);
        assert!(mt.is_empty());

        mt.put(Key::from("key"), Value::from("value"));
        assert_eq!(mt.len(), 1);
        assert!(!mt.is_empty());
        assert!(mt.approximate_size() > 0);
    }

    #[test]
    fn test_memtable_sequence() {
        let mut mt = MemTable::new();
        assert_eq!(mt.sequence(), 1);

        mt.put(Key::from("a"), Value::from("1"));
        assert_eq!(mt.sequence(), 2);

        mt.put(Key::from("b"), Value::from("2"));
        assert_eq!(mt.sequence(), 3);
    }

    #[test]
    fn test_memtable_iterator() {
        let mut mt = MemTable::new();
        mt.put(Key::from("c"), Value::from("3"));
        mt.put(Key::from("a"), Value::from("1"));
        mt.put(Key::from("b"), Value::from("2"));

        let entries: Vec<_> = mt.iter().collect();
        assert_eq!(entries.len(), 3);
        assert_eq!(entries[0].key.as_bytes(), b"a");
        assert_eq!(entries[1].key.as_bytes(), b"b");
        assert_eq!(entries[2].key.as_bytes(), b"c");
    }

    #[test]
    fn test_immutable_memtable() {
        let mut mt = MemTable::new();
        mt.put(Key::from("key"), Value::from("value"));

        let imm = mt.freeze();
        assert_eq!(imm.get(&Key::from("key")).unwrap().as_bytes(), b"value");
        assert_eq!(imm.len(), 1);
        assert_eq!(imm.sequence(), 2);
    }
}
