//! Core data types.

use std::cmp::Ordering;

/// Sequence number (monotonically increasing).
pub type Sequence = u64;

/// Key type (byte sequence).
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct Key(pub Vec<u8>);

impl Key {
    /// Create a new key.
    pub fn new(data: impl Into<Vec<u8>>) -> Self {
        Self(data.into())
    }

    /// Get key bytes.
    pub fn as_bytes(&self) -> &[u8] {
        &self.0
    }

    /// Get key length.
    pub fn len(&self) -> usize {
        self.0.len()
    }

    /// Check if empty.
    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }
}

impl From<&[u8]> for Key {
    fn from(data: &[u8]) -> Self {
        Self(data.to_vec())
    }
}

impl From<Vec<u8>> for Key {
    fn from(data: Vec<u8>) -> Self {
        Self(data)
    }
}

impl From<&str> for Key {
    fn from(s: &str) -> Self {
        Self(s.as_bytes().to_vec())
    }
}

impl Ord for Key {
    fn cmp(&self, other: &Self) -> Ordering {
        self.0.cmp(&other.0)
    }
}

impl PartialOrd for Key {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

/// Value type (byte sequence).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Value(pub Vec<u8>);

impl Value {
    /// Create a new value.
    pub fn new(data: impl Into<Vec<u8>>) -> Self {
        Self(data.into())
    }

    /// Get value bytes.
    pub fn as_bytes(&self) -> &[u8] {
        &self.0
    }

    /// Get value length.
    pub fn len(&self) -> usize {
        self.0.len()
    }

    /// Check if empty.
    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }
}

impl From<&[u8]> for Value {
    fn from(data: &[u8]) -> Self {
        Self(data.to_vec())
    }
}

impl From<Vec<u8>> for Value {
    fn from(data: Vec<u8>) -> Self {
        Self(data)
    }
}

impl From<&str> for Value {
    fn from(s: &str) -> Self {
        Self(s.as_bytes().to_vec())
    }
}

/// Entry type.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum EntryType {
    /// Key-value pair.
    Put,
    /// Deletion tombstone.
    Delete,
}

/// Log entry combining key, value, sequence, and type.
#[derive(Debug, Clone)]
pub struct Entry {
    /// Key.
    pub key: Key,
    /// Value (empty for Delete).
    pub value: Value,
    /// Sequence number.
    pub sequence: Sequence,
    /// Entry type.
    pub entry_type: EntryType,
}

impl Entry {
    /// Create a Put entry.
    pub fn put(key: Key, value: Value, sequence: Sequence) -> Self {
        Self {
            key,
            value,
            sequence,
            entry_type: EntryType::Put,
        }
    }

    /// Create a Delete entry.
    pub fn delete(key: Key, sequence: Sequence) -> Self {
        Self {
            key,
            value: Value::new(vec![]),
            sequence,
            entry_type: EntryType::Delete,
        }
    }

    /// Check if this is a tombstone.
    pub fn is_tombstone(&self) -> bool {
        self.entry_type == EntryType::Delete
    }

    /// Encode entry to bytes.
    pub fn encode(&self) -> Vec<u8> {
        let mut buf = Vec::new();

        // Type (1 byte)
        buf.push(match self.entry_type {
            EntryType::Put => 0,
            EntryType::Delete => 1,
        });

        // Sequence (8 bytes)
        buf.extend_from_slice(&self.sequence.to_le_bytes());

        // Key length (4 bytes) + key
        buf.extend_from_slice(&(self.key.len() as u32).to_le_bytes());
        buf.extend_from_slice(self.key.as_bytes());

        // Value length (4 bytes) + value
        buf.extend_from_slice(&(self.value.len() as u32).to_le_bytes());
        buf.extend_from_slice(self.value.as_bytes());

        buf
    }

    /// Decode entry from bytes.
    pub fn decode(data: &[u8]) -> Option<Self> {
        if data.len() < 17 {
            return None;
        }

        let entry_type = match data[0] {
            0 => EntryType::Put,
            1 => EntryType::Delete,
            _ => return None,
        };

        let sequence = u64::from_le_bytes(data[1..9].try_into().ok()?);

        let key_len = u32::from_le_bytes(data[9..13].try_into().ok()?) as usize;
        if data.len() < 13 + key_len + 4 {
            return None;
        }
        let key = Key::from(&data[13..13 + key_len]);

        let val_offset = 13 + key_len;
        let val_len = u32::from_le_bytes(data[val_offset..val_offset + 4].try_into().ok()?) as usize;
        if data.len() < val_offset + 4 + val_len {
            return None;
        }
        let value = Value::from(&data[val_offset + 4..val_offset + 4 + val_len]);

        Some(Self {
            key,
            value,
            sequence,
            entry_type,
        })
    }
}

/// Internal key combining user key and sequence number.
/// Used for comparing entries: same key sorted by decreasing sequence.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct InternalKey {
    pub user_key: Key,
    pub sequence: Sequence,
    pub entry_type: EntryType,
}

impl InternalKey {
    pub fn new(user_key: Key, sequence: Sequence, entry_type: EntryType) -> Self {
        Self {
            user_key,
            sequence,
            entry_type,
        }
    }
}

impl Ord for InternalKey {
    fn cmp(&self, other: &Self) -> Ordering {
        // First compare user keys
        match self.user_key.cmp(&other.user_key) {
            Ordering::Equal => {
                // Same key: newer sequence comes first (descending)
                other.sequence.cmp(&self.sequence)
            }
            ord => ord,
        }
    }
}

impl PartialOrd for InternalKey {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_key_ordering() {
        let k1 = Key::from("apple");
        let k2 = Key::from("banana");
        let k3 = Key::from("apple");

        assert!(k1 < k2);
        assert_eq!(k1, k3);
    }

    #[test]
    fn test_entry_encode_decode() {
        let entry = Entry::put(
            Key::from("foo"),
            Value::from("bar"),
            42,
        );

        let encoded = entry.encode();
        let decoded = Entry::decode(&encoded).unwrap();

        assert_eq!(decoded.key, entry.key);
        assert_eq!(decoded.value, entry.value);
        assert_eq!(decoded.sequence, 42);
        assert_eq!(decoded.entry_type, EntryType::Put);
    }

    #[test]
    fn test_delete_entry() {
        let entry = Entry::delete(Key::from("deleted"), 100);

        assert!(entry.is_tombstone());
        assert!(entry.value.is_empty());

        let encoded = entry.encode();
        let decoded = Entry::decode(&encoded).unwrap();
        assert!(decoded.is_tombstone());
    }

    #[test]
    fn test_internal_key_ordering() {
        let k1 = InternalKey::new(Key::from("a"), 10, EntryType::Put);
        let k2 = InternalKey::new(Key::from("a"), 5, EntryType::Put);
        let k3 = InternalKey::new(Key::from("b"), 10, EntryType::Put);

        // Same key: higher sequence comes first
        assert!(k1 < k2);
        // Different keys: lexicographic
        assert!(k1 < k3);
    }
}
