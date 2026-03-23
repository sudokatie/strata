//! Snapshot for point-in-time reads.

use std::sync::Arc;
use crate::types::Sequence;

/// A snapshot represents a point-in-time view of the database.
/// Entries with sequence > snapshot.sequence are not visible.
#[derive(Debug, Clone)]
pub struct Snapshot {
    /// Sequence number at snapshot creation.
    pub(crate) sequence: Sequence,
    /// Reference to keep version alive.
    #[allow(dead_code)]
    pub(crate) version_ref: Arc<()>,
}

impl Snapshot {
    /// Create a new snapshot.
    pub fn new(sequence: Sequence) -> Self {
        Self {
            sequence,
            version_ref: Arc::new(()),
        }
    }

    /// Get the sequence number of this snapshot.
    pub fn sequence(&self) -> Sequence {
        self.sequence
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_snapshot_sequence() {
        let snap = Snapshot::new(42);
        assert_eq!(snap.sequence(), 42);
    }

    #[test]
    fn test_snapshot_clone() {
        let snap1 = Snapshot::new(100);
        let snap2 = snap1.clone();
        assert_eq!(snap1.sequence(), snap2.sequence());
    }
}
