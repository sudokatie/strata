//! MemTable implementation.

pub mod skiplist;
#[allow(clippy::module_inception)]
pub mod memtable;

pub use skiplist::SkipList;
pub use memtable::{MemTable, ImmutableMemTable};
