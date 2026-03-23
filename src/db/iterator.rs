//! Database iterator.

use std::collections::BinaryHeap;
use std::cmp::Ordering;

use crate::types::{Key, Value, Entry, EntryType};

/// Item for the merge heap.
struct HeapItem {
    key: Key,
    value: Value,
    entry_type: EntryType,
    sequence: u64,
    source_idx: usize,
}

impl PartialEq for HeapItem {
    fn eq(&self, other: &Self) -> bool {
        self.key == other.key && self.sequence == other.sequence
    }
}

impl Eq for HeapItem {}

impl Ord for HeapItem {
    fn cmp(&self, other: &Self) -> Ordering {
        // Min-heap by key, then max sequence
        match other.key.cmp(&self.key) {
            Ordering::Equal => self.sequence.cmp(&other.sequence),
            ord => ord,
        }
    }
}

impl PartialOrd for HeapItem {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

/// Database iterator that merges multiple sources.
pub struct DBIterator {
    /// Merged entries in sorted order.
    entries: Vec<(Key, Value)>,
    /// Current position.
    position: usize,
}

impl DBIterator {
    /// Create a new iterator from multiple entry sources.
    pub fn new(sources: Vec<Vec<Entry>>) -> Self {
        let mut heap: BinaryHeap<HeapItem> = BinaryHeap::new();
        let mut source_iters: Vec<std::vec::IntoIter<Entry>> = 
            sources.into_iter().map(|v| v.into_iter()).collect();

        // Initialize heap with first entry from each source
        for (idx, iter) in source_iters.iter_mut().enumerate() {
            if let Some(entry) = iter.next() {
                heap.push(HeapItem {
                    key: entry.key,
                    value: entry.value,
                    entry_type: entry.entry_type,
                    sequence: entry.sequence,
                    source_idx: idx,
                });
            }
        }

        let mut entries = Vec::new();
        let mut last_key: Option<Key> = None;

        while let Some(item) = heap.pop() {
            // Advance the source we just consumed
            if let Some(entry) = source_iters[item.source_idx].next() {
                heap.push(HeapItem {
                    key: entry.key,
                    value: entry.value,
                    entry_type: entry.entry_type,
                    sequence: entry.sequence,
                    source_idx: item.source_idx,
                });
            }

            // Skip duplicates (same key - we already have newer version)
            if Some(&item.key) == last_key.as_ref() {
                continue;
            }

            last_key = Some(item.key.clone());

            // Skip tombstones in final output
            if item.entry_type == EntryType::Delete {
                continue;
            }

            entries.push((item.key, item.value));
        }

        Self {
            entries,
            position: 0,
        }
    }

    /// Check if iterator is valid.
    pub fn valid(&self) -> bool {
        self.position < self.entries.len()
    }

    /// Get current key-value pair.
    pub fn current(&self) -> Option<(&Key, &Value)> {
        self.entries.get(self.position).map(|(k, v)| (k, v))
    }

    /// Move to next entry.
    pub fn next(&mut self) {
        if self.position < self.entries.len() {
            self.position += 1;
        }
    }

    /// Seek to the first key >= target.
    pub fn seek(&mut self, target: &Key) {
        self.position = self.entries
            .iter()
            .position(|(k, _)| k >= target)
            .unwrap_or(self.entries.len());
    }

    /// Seek to first entry.
    pub fn seek_to_first(&mut self) {
        self.position = 0;
    }
}

impl Iterator for DBIterator {
    type Item = (Key, Value);

    fn next(&mut self) -> Option<Self::Item> {
        if self.position < self.entries.len() {
            let item = self.entries[self.position].clone();
            self.position += 1;
            Some(item)
        } else {
            None
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn make_entry(key: &str, value: &str, seq: u64) -> Entry {
        Entry::put(Key::from(key), Value::from(value), seq)
    }

    fn make_delete(key: &str, seq: u64) -> Entry {
        Entry::delete(Key::from(key), seq)
    }

    #[test]
    fn test_empty_iterator() {
        let iter = DBIterator::new(vec![]);
        assert!(!iter.valid());
    }

    #[test]
    fn test_single_source() {
        let entries = vec![
            make_entry("a", "1", 1),
            make_entry("b", "2", 2),
            make_entry("c", "3", 3),
        ];
        
        let mut iter = DBIterator::new(vec![entries]);
        
        assert!(iter.valid());
        let (k, v) = iter.current().unwrap();
        assert_eq!(k.as_bytes(), b"a");
        assert_eq!(v.as_bytes(), b"1");
        
        iter.next();
        assert!(iter.valid());
        
        let collected: Vec<_> = DBIterator::new(vec![vec![
            make_entry("a", "1", 1),
            make_entry("b", "2", 2),
        ]]).collect();
        assert_eq!(collected.len(), 2);
    }

    #[test]
    fn test_merge_sources() {
        let source1 = vec![
            make_entry("a", "a1", 10),
            make_entry("c", "c1", 10),
        ];
        let source2 = vec![
            make_entry("b", "b2", 5),
            make_entry("d", "d2", 5),
        ];

        let collected: Vec<_> = DBIterator::new(vec![source1, source2]).collect();
        assert_eq!(collected.len(), 4);
        
        let keys: Vec<_> = collected.iter()
            .map(|(k, _)| std::str::from_utf8(k.as_bytes()).unwrap())
            .collect();
        assert_eq!(keys, vec!["a", "b", "c", "d"]);
    }

    #[test]
    fn test_deduplication() {
        let source1 = vec![
            make_entry("key", "new", 100),
        ];
        let source2 = vec![
            make_entry("key", "old", 50),
        ];

        let collected: Vec<_> = DBIterator::new(vec![source1, source2]).collect();
        assert_eq!(collected.len(), 1);
        assert_eq!(collected[0].1.as_bytes(), b"new");
    }

    #[test]
    fn test_skip_tombstones() {
        let entries = vec![
            make_entry("a", "1", 1),
            make_delete("b", 2),
            make_entry("c", "3", 3),
        ];

        let collected: Vec<_> = DBIterator::new(vec![entries]).collect();
        assert_eq!(collected.len(), 2);
        
        let keys: Vec<_> = collected.iter()
            .map(|(k, _)| std::str::from_utf8(k.as_bytes()).unwrap())
            .collect();
        assert_eq!(keys, vec!["a", "c"]);
    }

    #[test]
    fn test_seek() {
        let entries = vec![
            make_entry("a", "1", 1),
            make_entry("c", "3", 3),
            make_entry("e", "5", 5),
        ];

        let mut iter = DBIterator::new(vec![entries]);
        
        iter.seek(&Key::from("b"));
        assert!(iter.valid());
        assert_eq!(iter.current().unwrap().0.as_bytes(), b"c");
        
        iter.seek(&Key::from("z"));
        assert!(!iter.valid());
    }
}
