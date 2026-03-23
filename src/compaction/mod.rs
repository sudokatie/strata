//! Compaction implementation.

pub mod merge;

pub use merge::{MergeIterator, MergeEntry, entries_to_merge, kv_to_merge};
