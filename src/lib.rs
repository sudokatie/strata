//! Strata - An LSM-tree storage engine.
//!
//! A Log-Structured Merge-tree implementation optimized for write-heavy
//! workloads with good read performance through compaction.

pub mod bloom;
pub mod error;
pub mod types;
pub mod options;
pub mod wal;
pub mod memtable;
pub mod sstable;
pub mod compaction;
pub mod manifest;
pub mod db;

pub use bloom::{BloomFilter, BloomFilterBuilder};
pub use error::{Error, Result};
pub use types::{Key, Value, Entry, EntryType, Sequence};
pub use options::{Options, ReadOptions, WriteOptions, Compression};
pub use db::{DB, DBStats, LevelStats, Snapshot};
