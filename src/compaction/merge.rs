//! Merge iterator for combining multiple sorted iterators.

use std::cmp::Ordering;
use std::collections::BinaryHeap;

use crate::types::{Key, Value, Entry, EntryType, Sequence};

/// Item from a source iterator with source index for tie-breaking.
struct HeapEntry {
    /// The entry.
    entry: MergeEntry,
    /// Index of the source iterator (lower = newer for duplicate handling).
    source_idx: usize,
}

impl PartialEq for HeapEntry {
    fn eq(&self, other: &Self) -> bool {
        self.entry.key == other.entry.key
            && self.entry.sequence == other.entry.sequence
    }
}

impl Eq for HeapEntry {}

impl Ord for HeapEntry {
    fn cmp(&self, other: &Self) -> Ordering {
        // Min-heap: reverse the comparison
        // First compare by key (ascending)
        match other.entry.key.cmp(&self.entry.key) {
            Ordering::Equal => {
                // Same key: prefer newer (higher sequence number)
                // For min-heap, we want higher sequence to come out first
                // So we compare self > other (reversed)
                match self.entry.sequence.cmp(&other.entry.sequence) {
                    Ordering::Equal => {
                        // Same sequence: prefer lower source index (newer source)
                        other.source_idx.cmp(&self.source_idx)
                    }
                    ord => ord,
                }
            }
            ord => ord,
        }
    }
}

impl PartialOrd for HeapEntry {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

/// Entry type for merge iterator.
#[derive(Debug, Clone)]
pub struct MergeEntry {
    pub key: Key,
    pub value: Value,
    pub sequence: Sequence,
    pub entry_type: EntryType,
}

impl MergeEntry {
    /// Create from an Entry.
    pub fn from_entry(entry: Entry) -> Self {
        Self {
            key: entry.key,
            value: entry.value,
            sequence: entry.sequence,
            entry_type: entry.entry_type,
        }
    }

    /// Create from key-value with default sequence (for SSTables without sequence info).
    pub fn from_kv(key: Key, value: Value, sequence: Sequence) -> Self {
        Self {
            key,
            value,
            sequence,
            entry_type: EntryType::Put,
        }
    }

    /// Check if this is a tombstone.
    pub fn is_tombstone(&self) -> bool {
        self.entry_type == EntryType::Delete
    }
}

/// Trait for iterators that can be merged.
pub trait MergeSource: Iterator<Item = MergeEntry> {}

impl<T: Iterator<Item = MergeEntry>> MergeSource for T {}

/// Merge iterator combining multiple sorted iterators.
/// 
/// Produces entries in key order. For duplicate keys, only the newest
/// entry (highest sequence number) is returned.
pub struct MergeIterator {
    /// Min-heap of current entries from each source.
    heap: BinaryHeap<HeapEntry>,
    /// Source iterators.
    sources: Vec<Box<dyn Iterator<Item = MergeEntry>>>,
    /// Last key returned (for deduplication).
    last_key: Option<Key>,
}

impl MergeIterator {
    /// Create a new merge iterator.
    /// 
    /// Sources should be provided in order from newest to oldest.
    /// Index 0 = newest (e.g., memtable), higher indices = older (e.g., lower levels).
    pub fn new(sources: Vec<Box<dyn Iterator<Item = MergeEntry>>>) -> Self {
        let mut iter = Self {
            heap: BinaryHeap::new(),
            sources,
            last_key: None,
        };
        iter.init();
        iter
    }

    /// Initialize by getting first entry from each source.
    fn init(&mut self) {
        for (idx, source) in self.sources.iter_mut().enumerate() {
            if let Some(entry) = source.next() {
                self.heap.push(HeapEntry {
                    entry,
                    source_idx: idx,
                });
            }
        }
    }

    /// Advance source at given index and push to heap if not exhausted.
    fn advance_source(&mut self, source_idx: usize) {
        if let Some(entry) = self.sources[source_idx].next() {
            self.heap.push(HeapEntry {
                entry,
                source_idx,
            });
        }
    }
}

impl Iterator for MergeIterator {
    type Item = MergeEntry;

    fn next(&mut self) -> Option<Self::Item> {
        loop {
            let heap_entry = self.heap.pop()?;
            let entry = heap_entry.entry;
            let source_idx = heap_entry.source_idx;

            // Advance the source we just consumed
            self.advance_source(source_idx);

            // Skip duplicates (same key as last returned)
            if let Some(ref last) = self.last_key {
                if entry.key == *last {
                    continue;
                }
            }

            // Skip any remaining entries with the same key (they have lower sequence)
            while let Some(top) = self.heap.peek() {
                if top.entry.key == entry.key {
                    let popped = self.heap.pop().unwrap();
                    self.advance_source(popped.source_idx);
                } else {
                    break;
                }
            }

            self.last_key = Some(entry.key.clone());
            return Some(entry);
        }
    }
}

/// Helper to create a merge entry iterator from a memtable iterator.
pub fn entries_to_merge<I>(iter: I) -> impl Iterator<Item = MergeEntry>
where
    I: Iterator<Item = Entry>,
{
    iter.map(MergeEntry::from_entry)
}

/// Helper to create a merge entry iterator from key-value pairs with a base sequence.
pub fn kv_to_merge<I>(iter: I, base_sequence: Sequence) -> impl Iterator<Item = MergeEntry>
where
    I: Iterator<Item = (Key, Value)>,
{
    let mut seq = base_sequence;
    iter.map(move |(k, v)| {
        let entry = MergeEntry::from_kv(k, v, seq);
        seq = seq.saturating_sub(1); // Decreasing sequence within level
        entry
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn make_entry(key: &str, value: &str, seq: Sequence) -> MergeEntry {
        MergeEntry {
            key: Key::from(key),
            value: Value::from(value),
            sequence: seq,
            entry_type: EntryType::Put,
        }
    }

    fn make_delete(key: &str, seq: Sequence) -> MergeEntry {
        MergeEntry {
            key: Key::from(key),
            value: Value::new(vec![]),
            sequence: seq,
            entry_type: EntryType::Delete,
        }
    }

    #[test]
    fn test_merge_two_iterators() {
        let iter1 = vec![
            make_entry("a", "1", 10),
            make_entry("c", "3", 10),
            make_entry("e", "5", 10),
        ].into_iter();

        let iter2 = vec![
            make_entry("b", "2", 5),
            make_entry("d", "4", 5),
            make_entry("f", "6", 5),
        ].into_iter();

        let sources: Vec<Box<dyn Iterator<Item = MergeEntry>>> = vec![
            Box::new(iter1),
            Box::new(iter2),
        ];

        let merged: Vec<_> = MergeIterator::new(sources).collect();

        assert_eq!(merged.len(), 6);
        assert_eq!(merged[0].key.as_bytes(), b"a");
        assert_eq!(merged[1].key.as_bytes(), b"b");
        assert_eq!(merged[2].key.as_bytes(), b"c");
        assert_eq!(merged[3].key.as_bytes(), b"d");
        assert_eq!(merged[4].key.as_bytes(), b"e");
        assert_eq!(merged[5].key.as_bytes(), b"f");
    }

    #[test]
    fn test_merge_many_iterators() {
        let iter1 = vec![
            make_entry("a", "a1", 100),
            make_entry("d", "d1", 100),
        ].into_iter();

        let iter2 = vec![
            make_entry("b", "b2", 50),
            make_entry("e", "e2", 50),
        ].into_iter();

        let iter3 = vec![
            make_entry("c", "c3", 25),
            make_entry("f", "f3", 25),
        ].into_iter();

        let sources: Vec<Box<dyn Iterator<Item = MergeEntry>>> = vec![
            Box::new(iter1),
            Box::new(iter2),
            Box::new(iter3),
        ];

        let merged: Vec<_> = MergeIterator::new(sources).collect();

        assert_eq!(merged.len(), 6);
        let keys: Vec<_> = merged.iter().map(|e| std::str::from_utf8(e.key.as_bytes()).unwrap()).collect();
        assert_eq!(keys, vec!["a", "b", "c", "d", "e", "f"]);
    }

    #[test]
    fn test_handle_duplicates() {
        // Same key with different sequences - should take newest
        let iter1 = vec![
            make_entry("key", "new", 100),
        ].into_iter();

        let iter2 = vec![
            make_entry("key", "old", 50),
        ].into_iter();

        let sources: Vec<Box<dyn Iterator<Item = MergeEntry>>> = vec![
            Box::new(iter1),
            Box::new(iter2),
        ];

        let merged: Vec<_> = MergeIterator::new(sources).collect();

        assert_eq!(merged.len(), 1);
        assert_eq!(merged[0].value.as_bytes(), b"new");
        assert_eq!(merged[0].sequence, 100);
    }

    #[test]
    fn test_handle_tombstones() {
        // Delete should be preserved
        let iter1 = vec![
            make_delete("deleted", 100),
        ].into_iter();

        let iter2 = vec![
            make_entry("deleted", "old_value", 50),
        ].into_iter();

        let sources: Vec<Box<dyn Iterator<Item = MergeEntry>>> = vec![
            Box::new(iter1),
            Box::new(iter2),
        ];

        let merged: Vec<_> = MergeIterator::new(sources).collect();

        assert_eq!(merged.len(), 1);
        assert!(merged[0].is_tombstone());
    }

    #[test]
    fn test_empty_iterators() {
        let iter1: Vec<MergeEntry> = vec![];
        let iter2 = vec![
            make_entry("a", "1", 10),
        ].into_iter();
        let iter3: Vec<MergeEntry> = vec![];

        let sources: Vec<Box<dyn Iterator<Item = MergeEntry>>> = vec![
            Box::new(iter1.into_iter()),
            Box::new(iter2),
            Box::new(iter3.into_iter()),
        ];

        let merged: Vec<_> = MergeIterator::new(sources).collect();

        assert_eq!(merged.len(), 1);
        assert_eq!(merged[0].key.as_bytes(), b"a");
    }

    #[test]
    fn test_all_empty() {
        let sources: Vec<Box<dyn Iterator<Item = MergeEntry>>> = vec![
            Box::new(std::iter::empty()),
            Box::new(std::iter::empty()),
        ];

        let merged: Vec<_> = MergeIterator::new(sources).collect();
        assert!(merged.is_empty());
    }

    #[test]
    fn test_single_source() {
        let iter = vec![
            make_entry("a", "1", 10),
            make_entry("b", "2", 10),
            make_entry("c", "3", 10),
        ].into_iter();

        let sources: Vec<Box<dyn Iterator<Item = MergeEntry>>> = vec![
            Box::new(iter),
        ];

        let merged: Vec<_> = MergeIterator::new(sources).collect();

        assert_eq!(merged.len(), 3);
    }

    #[test]
    fn test_interleaved_duplicates() {
        // Multiple sources with overlapping keys
        let iter1 = vec![
            make_entry("a", "a1", 100),
            make_entry("b", "b1", 100),
            make_entry("c", "c1", 100),
        ].into_iter();

        let iter2 = vec![
            make_entry("a", "a2", 50),
            make_entry("b", "b2", 50),
            make_entry("d", "d2", 50),
        ].into_iter();

        let iter3 = vec![
            make_entry("a", "a3", 25),
            make_entry("c", "c3", 25),
            make_entry("e", "e3", 25),
        ].into_iter();

        let sources: Vec<Box<dyn Iterator<Item = MergeEntry>>> = vec![
            Box::new(iter1),
            Box::new(iter2),
            Box::new(iter3),
        ];

        let merged: Vec<_> = MergeIterator::new(sources).collect();

        // Should have 5 unique keys: a, b, c, d, e
        assert_eq!(merged.len(), 5);

        // Check newest values won
        let entries: std::collections::HashMap<_, _> = merged.iter()
            .map(|e| (std::str::from_utf8(e.key.as_bytes()).unwrap(), e))
            .collect();

        assert_eq!(entries["a"].value.as_bytes(), b"a1");
        assert_eq!(entries["b"].value.as_bytes(), b"b1");
        assert_eq!(entries["c"].value.as_bytes(), b"c1");
        assert_eq!(entries["d"].value.as_bytes(), b"d2");
        assert_eq!(entries["e"].value.as_bytes(), b"e3");
    }
}
