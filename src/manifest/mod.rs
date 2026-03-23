//! Manifest and version management.

pub mod version;

pub use version::{
    FileMetaData, Version, VersionEdit, Manifest, MAX_LEVELS,
};
