//! Skip list implementation for MemTable.

use std::cmp::Ordering;
use rand::Rng;

use crate::types::{Key, Value, Entry, EntryType, Sequence, InternalKey};

/// Maximum height of skip list.
const MAX_HEIGHT: usize = 12;

/// Probability for level increase.
const P: f64 = 0.25;

/// Skip list node.
struct Node {
    key: InternalKey,
    value: Value,
    /// Forward pointers for each level.
    forward: Vec<Option<Box<Node>>>,
}

impl Node {
    fn new(key: InternalKey, value: Value, height: usize) -> Self {
        let mut forward = Vec::with_capacity(height);
        for _ in 0..height {
            forward.push(None);
        }
        Self {
            key,
            value,
            forward,
        }
    }
}

/// Skip list for sorted key-value storage.
pub struct SkipList {
    /// Head node (sentinel).
    head: Box<Node>,
    /// Current maximum height.
    height: usize,
    /// Approximate memory size.
    size: usize,
    /// Number of entries.
    count: usize,
}

impl SkipList {
    /// Create a new skip list.
    pub fn new() -> Self {
        let mut forward = Vec::with_capacity(MAX_HEIGHT);
        for _ in 0..MAX_HEIGHT {
            forward.push(None);
        }
        let head = Box::new(Node {
            key: InternalKey::new(Key::new(vec![]), 0, EntryType::Put),
            value: Value::new(vec![]),
            forward,
        });

        Self {
            head,
            height: 1,
            size: 0,
            count: 0,
        }
    }

    /// Generate random height for new node.
    fn random_height() -> usize {
        let mut rng = rand::thread_rng();
        let mut height = 1;
        while height < MAX_HEIGHT && rng.gen::<f64>() < P {
            height += 1;
        }
        height
    }

    /// Insert an entry.
    pub fn insert(&mut self, key: Key, value: Value, sequence: Sequence, entry_type: EntryType) {
        let internal_key = InternalKey::new(key.clone(), sequence, entry_type);
        let height = Self::random_height();

        // Update max height
        if height > self.height {
            self.height = height;
        }

        // Track memory
        self.size += key.len() + value.len() + 32; // estimate
        self.count += 1;

        // Find insert position and update pointers
        // For simplicity, we'll use a non-optimal but correct implementation
        let new_node = Box::new(Node::new(internal_key.clone(), value, height));

        // Insert at correct position (simplified - linear scan)
        self.insert_node(new_node);
    }

    fn insert_node(&mut self, new_node: Box<Node>) {
        // Simple insertion at level 0 only for correctness
        // TODO: Full skip list with multi-level pointers for O(log n)
        let mut current = &mut self.head;

        // Find position: advance while next < new_node
        while current.forward[0].is_some() {
            let should_advance = {
                let next = current.forward[0].as_ref().unwrap();
                next.key < new_node.key
            };
            if should_advance {
                current = current.forward[0].as_mut().unwrap();
            } else {
                break;
            }
        }

        // Insert after current
        let mut new_node = new_node;
        new_node.forward[0] = current.forward[0].take();
        current.forward[0] = Some(new_node);
    }

    /// Get the latest value for a key.
    pub fn get(&self, key: &Key) -> Option<Entry> {
        let mut current = &self.head;

        for level in (0..self.height).rev() {
            while let Some(ref next) = current.forward[level] {
                match next.key.user_key.cmp(key) {
                    Ordering::Less => current = next,
                    Ordering::Equal => {
                        // Found - return latest (highest sequence for this key)
                        return Some(Entry {
                            key: next.key.user_key.clone(),
                            value: next.value.clone(),
                            sequence: next.key.sequence,
                            entry_type: next.key.entry_type,
                        });
                    }
                    Ordering::Greater => break,
                }
            }
        }

        // Check level 0
        if let Some(ref next) = current.forward[0] {
            if next.key.user_key == *key {
                return Some(Entry {
                    key: next.key.user_key.clone(),
                    value: next.value.clone(),
                    sequence: next.key.sequence,
                    entry_type: next.key.entry_type,
                });
            }
        }

        None
    }

    /// Get approximate memory usage.
    pub fn approximate_size(&self) -> usize {
        self.size
    }

    /// Get number of entries.
    pub fn len(&self) -> usize {
        self.count
    }

    /// Check if empty.
    pub fn is_empty(&self) -> bool {
        self.count == 0
    }

    /// Iterate over all entries in order.
    pub fn iter(&self) -> SkipListIterator<'_> {
        SkipListIterator {
            current: self.head.forward[0].as_ref(),
        }
    }
}

impl Default for SkipList {
    fn default() -> Self {
        Self::new()
    }
}

/// Iterator over skip list entries.
pub struct SkipListIterator<'a> {
    current: Option<&'a Box<Node>>,
}

impl<'a> Iterator for SkipListIterator<'a> {
    type Item = Entry;

    fn next(&mut self) -> Option<Self::Item> {
        let node = self.current?;
        let entry = Entry {
            key: node.key.user_key.clone(),
            value: node.value.clone(),
            sequence: node.key.sequence,
            entry_type: node.key.entry_type,
        };
        self.current = node.forward[0].as_ref();
        Some(entry)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_skiplist_insert_get() {
        let mut sl = SkipList::new();
        sl.insert(Key::from("foo"), Value::from("bar"), 1, EntryType::Put);

        let entry = sl.get(&Key::from("foo")).unwrap();
        assert_eq!(entry.value.as_bytes(), b"bar");
    }

    #[test]
    fn test_skiplist_not_found() {
        let sl = SkipList::new();
        assert!(sl.get(&Key::from("missing")).is_none());
    }

    #[test]
    fn test_skiplist_overwrite() {
        let mut sl = SkipList::new();
        sl.insert(Key::from("key"), Value::from("old"), 1, EntryType::Put);
        sl.insert(Key::from("key"), Value::from("new"), 2, EntryType::Put);

        // Should get the newer one (higher sequence)
        let entry = sl.get(&Key::from("key")).unwrap();
        assert_eq!(entry.sequence, 2);
    }

    #[test]
    fn test_skiplist_iterator() {
        let mut sl = SkipList::new();
        sl.insert(Key::from("b"), Value::from("2"), 1, EntryType::Put);
        sl.insert(Key::from("a"), Value::from("1"), 2, EntryType::Put);
        sl.insert(Key::from("c"), Value::from("3"), 3, EntryType::Put);

        let entries: Vec<_> = sl.iter().collect();
        assert_eq!(entries.len(), 3);
        // Should be sorted
        assert_eq!(entries[0].key.as_bytes(), b"a");
        assert_eq!(entries[1].key.as_bytes(), b"b");
        assert_eq!(entries[2].key.as_bytes(), b"c");
    }

    #[test]
    fn test_skiplist_size() {
        let mut sl = SkipList::new();
        assert!(sl.is_empty());

        sl.insert(Key::from("key"), Value::from("value"), 1, EntryType::Put);
        assert_eq!(sl.len(), 1);
        assert!(sl.approximate_size() > 0);
    }
}
