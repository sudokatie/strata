//! SSTable implementation.

pub mod block;

pub use block::{Block, BlockBuilder, BlockIterator, BLOCK_SIZE};
