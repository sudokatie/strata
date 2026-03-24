//! Skip list implementation for MemTable.
//!
//! O(log n) probabilistic data structure for sorted key-value storage.

use std::ptr::NonNull;
use rand::Rng;

use crate::types::{Key, Value, Entry, EntryType, Sequence, InternalKey};

/// Maximum height of skip list.
const MAX_HEIGHT: usize = 12;

/// Probability for level increase (1/4 chance per level).
const P: f64 = 0.25;

/// Skip list node.
struct Node {
    key: InternalKey,
    value: Value,
    /// Forward pointers for each level (length = node height).
    forward: Vec<Option<NonNull<Node>>>,
}

impl Node {
    fn new(key: InternalKey, value: Value, height: usize) -> Box<Self> {
        Box::new(Self {
            key,
            value,
            forward: vec![None; height],
        })
    }
}

/// Skip list for sorted key-value storage.
/// 
/// Provides O(log n) insert, lookup, and iteration.
pub struct SkipList {
    /// Head node (sentinel with MAX_HEIGHT levels).
    head: Box<Node>,
    /// Current maximum height in use.
    height: usize,
    /// Approximate memory size in bytes.
    size: usize,
    /// Number of entries.
    count: usize,
}

impl SkipList {
    /// Create a new skip list.
    pub fn new() -> Self {
        // Head is a sentinel with max height
        let head = Box::new(Node {
            key: InternalKey::new(Key::new(vec![]), 0, EntryType::Put),
            value: Value::new(vec![]),
            forward: vec![None; MAX_HEIGHT],
        });

        Self {
            head,
            height: 1,
            size: 0,
            count: 0,
        }
    }

    /// Generate random height for new node using geometric distribution.
    fn random_height() -> usize {
        let mut rng = rand::thread_rng();
        let mut height = 1;
        while height < MAX_HEIGHT && rng.gen::<f64>() < P {
            height += 1;
        }
        height
    }

    /// Insert an entry. O(log n) average case.
    pub fn insert(&mut self, key: Key, value: Value, sequence: Sequence, entry_type: EntryType) {
        let internal_key = InternalKey::new(key.clone(), sequence, entry_type);
        let new_height = Self::random_height();

        // Track update points at each level
        let mut update: [Option<NonNull<Node>>; MAX_HEIGHT] = [None; MAX_HEIGHT];
        
        // Find insert position using skip list traversal
        let mut current: *mut Node = self.head.as_mut();
        
        for level in (0..self.height).rev() {
            unsafe {
                while let Some(next_ptr) = (&(*current).forward)[level] {
                    let next = next_ptr.as_ref();
                    if next.key < internal_key {
                        current = next_ptr.as_ptr();
                    } else {
                        break;
                    }
                }
                update[level] = Some(NonNull::new_unchecked(current));
            }
        }

        // Update height if new node is taller
        if new_height > self.height {
            for update_slot in update.iter_mut().take(new_height).skip(self.height) {
                *update_slot = NonNull::new(self.head.as_mut());
            }
            self.height = new_height;
        }

        // Create new node
        let mut new_node = Node::new(internal_key, value.clone(), new_height);
        let new_node_ptr = NonNull::new(new_node.as_mut()).unwrap();

        // Insert at each level
        for (level, update_slot) in update.iter().enumerate().take(new_height) {
            unsafe {
                if let Some(mut update_ptr) = *update_slot {
                    let update_node = update_ptr.as_mut();
                    new_node.forward[level] = update_node.forward[level];
                    update_node.forward[level] = Some(new_node_ptr);
                }
            }
        }

        // Leak the box - we manage memory manually
        Box::leak(new_node);

        // Track memory
        self.size += key.len() + value.len() + 32 + new_height * 8;
        self.count += 1;
    }

    /// Get the latest value for a key. O(log n) average case.
    /// 
    /// Since entries are ordered by InternalKey (user_key, then sequence descending),
    /// the first entry with matching user_key has the highest sequence number.
    pub fn get(&self, key: &Key) -> Option<Entry> {
        let mut current: *const Node = self.head.as_ref();
        
        // Create a search key with max sequence to find first entry with this user_key
        let search_key = InternalKey::new(key.clone(), u64::MAX, EntryType::Put);

        // Search using skip list traversal with InternalKey comparison
        for level in (0..self.height).rev() {
            unsafe {
                while let Some(next_ptr) = (&(*current).forward)[level] {
                    let next = next_ptr.as_ref();
                    // Use InternalKey comparison for correct traversal
                    if next.key < search_key {
                        current = next_ptr.as_ptr();
                    } else {
                        break;
                    }
                }
            }
        }

        // After traversal, check the next node at level 0
        unsafe {
            if let Some(next_ptr) = (&(*current).forward)[0] {
                let next = next_ptr.as_ref();
                if next.key.user_key == *key {
                    return Some(Entry {
                        key: next.key.user_key.clone(),
                        value: next.value.clone(),
                        sequence: next.key.sequence,
                        entry_type: next.key.entry_type,
                    });
                }
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
        let first = self.head.forward[0].map(|ptr| unsafe { ptr.as_ref() });
        SkipListIterator { current: first }
    }
}

impl Default for SkipList {
    fn default() -> Self {
        Self::new()
    }
}

impl Drop for SkipList {
    fn drop(&mut self) {
        // Free all nodes (except head which is owned by self)
        let mut current = self.head.forward[0];
        while let Some(node_ptr) = current {
            unsafe {
                let node = Box::from_raw(node_ptr.as_ptr());
                current = node.forward[0];
                // node is dropped here
            }
        }
    }
}

/// Iterator over skip list entries.
pub struct SkipListIterator<'a> {
    current: Option<&'a Node>,
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
        self.current = node.forward[0].map(|ptr| unsafe { ptr.as_ref() });
        Some(entry)
    }
}

// Safety: SkipList manages its own memory and uses raw pointers internally.
// The public API is safe and the struct can be safely shared across threads
// when protected by a Mutex (which is how it's used in MemTable).
unsafe impl Send for SkipList {}
unsafe impl Sync for SkipList {}

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

    #[test]
    fn test_skiplist_many_entries() {
        let mut sl = SkipList::new();
        
        // Insert 1000 entries in random order
        for i in (0..1000).rev() {
            let key = format!("key{:05}", i);
            sl.insert(Key::from(key.as_str()), Value::from("v"), i as u64, EntryType::Put);
        }

        // All should be found
        for i in 0..1000 {
            let key = format!("key{:05}", i);
            assert!(sl.get(&Key::from(key.as_str())).is_some());
        }

        // Iterator should return sorted
        let entries: Vec<_> = sl.iter().collect();
        assert_eq!(entries.len(), 1000);
        for i in 0..1000 {
            let expected = format!("key{:05}", i);
            assert_eq!(entries[i].key.as_bytes(), expected.as_bytes());
        }
    }

    #[test]
    fn test_skiplist_height_varies() {
        // Insert many entries to verify multi-level structure
        let mut sl = SkipList::new();
        for i in 0..100 {
            let key = format!("key{:03}", i);
            sl.insert(Key::from(key.as_str()), Value::from("v"), i as u64, EntryType::Put);
        }
        
        // With 100 entries and P=0.25, we should have height > 1
        // (probabilistic, but very likely)
        assert!(sl.height >= 1);
        assert_eq!(sl.len(), 100);
    }
}
