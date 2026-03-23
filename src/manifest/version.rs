//! Version and manifest management.
//!
//! A Version represents a snapshot of the database file set at a point in time.
//! VersionEdit records changes (add/delete files) to create new versions.
//! The Manifest is a log of VersionEdits for recovery.

use std::collections::HashSet;
use std::fs::{self, File, OpenOptions};
use std::io::{BufRead, BufReader, Write};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};

use crate::types::Sequence;
use crate::Result;

/// Maximum number of levels in the LSM tree.
pub const MAX_LEVELS: usize = 7;

/// Metadata about an SSTable file.
#[derive(Debug, Clone)]
pub struct FileMetaData {
    /// Unique file number.
    pub number: u64,
    /// File size in bytes.
    pub file_size: u64,
    /// Smallest key in the file.
    pub smallest: Vec<u8>,
    /// Largest key in the file.
    pub largest: Vec<u8>,
}

impl FileMetaData {
    /// Create new file metadata.
    pub fn new(number: u64, file_size: u64, smallest: Vec<u8>, largest: Vec<u8>) -> Self {
        Self {
            number,
            file_size,
            smallest,
            largest,
        }
    }

    /// Encode to bytes for manifest storage.
    pub fn encode(&self) -> Vec<u8> {
        let mut buf = Vec::new();

        // number (8 bytes)
        buf.extend_from_slice(&self.number.to_le_bytes());
        // file_size (8 bytes)
        buf.extend_from_slice(&self.file_size.to_le_bytes());
        // smallest length + data
        buf.extend_from_slice(&(self.smallest.len() as u32).to_le_bytes());
        buf.extend_from_slice(&self.smallest);
        // largest length + data
        buf.extend_from_slice(&(self.largest.len() as u32).to_le_bytes());
        buf.extend_from_slice(&self.largest);

        buf
    }

    /// Decode from bytes.
    pub fn decode(data: &[u8]) -> Option<(Self, usize)> {
        if data.len() < 20 {
            return None;
        }

        let number = u64::from_le_bytes(data[0..8].try_into().ok()?);
        let file_size = u64::from_le_bytes(data[8..16].try_into().ok()?);

        let smallest_len = u32::from_le_bytes(data[16..20].try_into().ok()?) as usize;
        if data.len() < 20 + smallest_len + 4 {
            return None;
        }
        let smallest = data[20..20 + smallest_len].to_vec();

        let largest_offset = 20 + smallest_len;
        let largest_len = u32::from_le_bytes(
            data[largest_offset..largest_offset + 4].try_into().ok()?
        ) as usize;
        if data.len() < largest_offset + 4 + largest_len {
            return None;
        }
        let largest = data[largest_offset + 4..largest_offset + 4 + largest_len].to_vec();

        let total_len = largest_offset + 4 + largest_len;

        Some((
            Self {
                number,
                file_size,
                smallest,
                largest,
            },
            total_len,
        ))
    }
}

/// Edit to a version - records files added or deleted.
#[derive(Debug, Clone, Default)]
pub struct VersionEdit {
    /// Log number (for WAL).
    pub log_number: Option<u64>,
    /// Previous log number.
    pub prev_log_number: Option<u64>,
    /// Next file number.
    pub next_file_number: Option<u64>,
    /// Last sequence number.
    pub last_sequence: Option<Sequence>,
    /// Files added (level -> files).
    pub new_files: Vec<(usize, FileMetaData)>,
    /// Files deleted (level -> file numbers).
    pub deleted_files: Vec<(usize, u64)>,
}

impl VersionEdit {
    /// Create empty edit.
    pub fn new() -> Self {
        Self::default()
    }

    /// Set log number.
    pub fn set_log_number(&mut self, num: u64) {
        self.log_number = Some(num);
    }

    /// Set last sequence.
    pub fn set_last_sequence(&mut self, seq: Sequence) {
        self.last_sequence = Some(seq);
    }

    /// Set next file number.
    pub fn set_next_file_number(&mut self, num: u64) {
        self.next_file_number = Some(num);
    }

    /// Add a new file.
    pub fn add_file(&mut self, level: usize, meta: FileMetaData) {
        self.new_files.push((level, meta));
    }

    /// Delete a file.
    pub fn delete_file(&mut self, level: usize, number: u64) {
        self.deleted_files.push((level, number));
    }

    /// Encode to bytes.
    pub fn encode(&self) -> Vec<u8> {
        let mut buf = Vec::new();

        // Tags for each field
        const TAG_LOG_NUMBER: u8 = 1;
        const TAG_PREV_LOG_NUMBER: u8 = 2;
        const TAG_NEXT_FILE_NUMBER: u8 = 3;
        const TAG_LAST_SEQUENCE: u8 = 4;
        const TAG_NEW_FILE: u8 = 5;
        const TAG_DELETED_FILE: u8 = 6;

        if let Some(num) = self.log_number {
            buf.push(TAG_LOG_NUMBER);
            buf.extend_from_slice(&num.to_le_bytes());
        }

        if let Some(num) = self.prev_log_number {
            buf.push(TAG_PREV_LOG_NUMBER);
            buf.extend_from_slice(&num.to_le_bytes());
        }

        if let Some(num) = self.next_file_number {
            buf.push(TAG_NEXT_FILE_NUMBER);
            buf.extend_from_slice(&num.to_le_bytes());
        }

        if let Some(seq) = self.last_sequence {
            buf.push(TAG_LAST_SEQUENCE);
            buf.extend_from_slice(&seq.to_le_bytes());
        }

        for (level, meta) in &self.new_files {
            buf.push(TAG_NEW_FILE);
            buf.push(*level as u8);
            let meta_bytes = meta.encode();
            buf.extend_from_slice(&(meta_bytes.len() as u32).to_le_bytes());
            buf.extend_from_slice(&meta_bytes);
        }

        for (level, number) in &self.deleted_files {
            buf.push(TAG_DELETED_FILE);
            buf.push(*level as u8);
            buf.extend_from_slice(&number.to_le_bytes());
        }

        buf
    }

    /// Decode from bytes.
    pub fn decode(data: &[u8]) -> Option<Self> {
        const TAG_LOG_NUMBER: u8 = 1;
        const TAG_PREV_LOG_NUMBER: u8 = 2;
        const TAG_NEXT_FILE_NUMBER: u8 = 3;
        const TAG_LAST_SEQUENCE: u8 = 4;
        const TAG_NEW_FILE: u8 = 5;
        const TAG_DELETED_FILE: u8 = 6;

        let mut edit = VersionEdit::new();
        let mut pos = 0;

        while pos < data.len() {
            let tag = data[pos];
            pos += 1;

            match tag {
                TAG_LOG_NUMBER => {
                    if pos + 8 > data.len() {
                        return None;
                    }
                    edit.log_number = Some(u64::from_le_bytes(
                        data[pos..pos + 8].try_into().ok()?
                    ));
                    pos += 8;
                }
                TAG_PREV_LOG_NUMBER => {
                    if pos + 8 > data.len() {
                        return None;
                    }
                    edit.prev_log_number = Some(u64::from_le_bytes(
                        data[pos..pos + 8].try_into().ok()?
                    ));
                    pos += 8;
                }
                TAG_NEXT_FILE_NUMBER => {
                    if pos + 8 > data.len() {
                        return None;
                    }
                    edit.next_file_number = Some(u64::from_le_bytes(
                        data[pos..pos + 8].try_into().ok()?
                    ));
                    pos += 8;
                }
                TAG_LAST_SEQUENCE => {
                    if pos + 8 > data.len() {
                        return None;
                    }
                    edit.last_sequence = Some(u64::from_le_bytes(
                        data[pos..pos + 8].try_into().ok()?
                    ));
                    pos += 8;
                }
                TAG_NEW_FILE => {
                    if pos + 5 > data.len() {
                        return None;
                    }
                    let level = data[pos] as usize;
                    pos += 1;
                    let meta_len = u32::from_le_bytes(
                        data[pos..pos + 4].try_into().ok()?
                    ) as usize;
                    pos += 4;
                    if pos + meta_len > data.len() {
                        return None;
                    }
                    let (meta, _) = FileMetaData::decode(&data[pos..pos + meta_len])?;
                    edit.new_files.push((level, meta));
                    pos += meta_len;
                }
                TAG_DELETED_FILE => {
                    if pos + 9 > data.len() {
                        return None;
                    }
                    let level = data[pos] as usize;
                    pos += 1;
                    let number = u64::from_le_bytes(
                        data[pos..pos + 8].try_into().ok()?
                    );
                    edit.deleted_files.push((level, number));
                    pos += 8;
                }
                _ => return None, // Unknown tag
            }
        }

        Some(edit)
    }
}

/// A Version is a snapshot of the file set at a point in time.
#[derive(Debug, Clone)]
pub struct Version {
    /// Files at each level.
    files: Vec<Vec<FileMetaData>>,
}

impl Version {
    /// Create empty version.
    pub fn new() -> Self {
        Self {
            files: vec![Vec::new(); MAX_LEVELS],
        }
    }

    /// Get files at a level.
    pub fn files(&self, level: usize) -> &[FileMetaData] {
        &self.files[level]
    }

    /// Total number of files across all levels.
    pub fn num_files(&self) -> usize {
        self.files.iter().map(|f| f.len()).sum()
    }

    /// Apply an edit to create a new version.
    pub fn apply(&self, edit: &VersionEdit) -> Version {
        let mut new_files = self.files.clone();

        // Remove deleted files
        let deleted: HashSet<(usize, u64)> = edit.deleted_files.iter().copied().collect();
        for (level, files) in new_files.iter_mut().enumerate() {
            files.retain(|f| !deleted.contains(&(level, f.number)));
        }

        // Add new files
        for (level, meta) in &edit.new_files {
            new_files[*level].push(meta.clone());
        }

        // Sort files in each level by smallest key
        for files in &mut new_files {
            files.sort_by(|a, b| a.smallest.cmp(&b.smallest));
        }

        Version { files: new_files }
    }
}

impl Default for Version {
    fn default() -> Self {
        Self::new()
    }
}

/// Manifest manages version history and persistence.
pub struct Manifest {
    /// Database directory.
    db_path: PathBuf,
    /// Current manifest file number.
    manifest_number: u64,
    /// Manifest file writer.
    manifest_file: Option<File>,
    /// Next file number generator.
    next_file_number: AtomicU64,
    /// Current version.
    current: Version,
}

impl Manifest {
    /// Create or open manifest.
    pub fn open(db_path: &Path) -> Result<Self> {
        fs::create_dir_all(db_path)?;

        let current_path = db_path.join("CURRENT");
        let (manifest_number, current) = if current_path.exists() {
            // Recover from existing manifest
            let manifest_name = fs::read_to_string(&current_path)?;
            let manifest_name = manifest_name.trim();
            let manifest_number: u64 = manifest_name
                .strip_prefix("MANIFEST-")
                .and_then(|s| s.parse().ok())
                .ok_or_else(|| crate::Error::Corruption("invalid CURRENT file".into()))?;

            let manifest_path = db_path.join(manifest_name);
            let version = Self::recover_version(&manifest_path)?;
            (manifest_number, version)
        } else {
            // New database
            (1, Version::new())
        };

        // Open manifest for appending
        let manifest_path = db_path.join(format!("MANIFEST-{:06}", manifest_number));
        let manifest_file = OpenOptions::new()
            .create(true)
            .append(true)
            .open(&manifest_path)?;

        // Write CURRENT if new
        if !current_path.exists() {
            Self::write_current(db_path, manifest_number)?;
        }

        Ok(Self {
            db_path: db_path.to_path_buf(),
            manifest_number,
            manifest_file: Some(manifest_file),
            next_file_number: AtomicU64::new(manifest_number + 1),
            current,
        })
    }

    /// Recover version by replaying manifest.
    fn recover_version(manifest_path: &Path) -> Result<Version> {
        let file = File::open(manifest_path)?;
        let reader = BufReader::new(file);

        let mut version = Version::new();

        for line in reader.lines() {
            let line = line?;
            if line.is_empty() {
                continue;
            }

            // Decode hex-encoded edit
            let bytes = hex::decode(&line)
                .map_err(|_| crate::Error::Corruption("invalid manifest line".into()))?;

            let edit = VersionEdit::decode(&bytes)
                .ok_or_else(|| crate::Error::Corruption("invalid version edit".into()))?;

            version = version.apply(&edit);
        }

        Ok(version)
    }

    /// Write CURRENT file pointing to manifest.
    fn write_current(db_path: &Path, manifest_number: u64) -> Result<()> {
        let current_path = db_path.join("CURRENT");
        let tmp_path = db_path.join("CURRENT.tmp");

        let content = format!("MANIFEST-{:06}\n", manifest_number);
        fs::write(&tmp_path, &content)?;
        fs::rename(&tmp_path, &current_path)?;

        Ok(())
    }

    /// Log an edit and apply it.
    pub fn log_and_apply(&mut self, edit: &VersionEdit) -> Result<()> {
        // Write edit to manifest
        if let Some(ref mut file) = self.manifest_file {
            let bytes = edit.encode();
            let hex = hex::encode(&bytes);
            writeln!(file, "{}", hex)?;
            file.sync_all()?;
        }

        // Apply to current version
        self.current = self.current.apply(edit);

        // Update file number
        if let Some(num) = edit.next_file_number {
            self.next_file_number.store(num, Ordering::SeqCst);
        }

        Ok(())
    }

    /// Get current version.
    pub fn current(&self) -> &Version {
        &self.current
    }

    /// Allocate a new file number.
    pub fn new_file_number(&self) -> u64 {
        self.next_file_number.fetch_add(1, Ordering::SeqCst)
    }

    /// Get database path.
    pub fn db_path(&self) -> &Path {
        &self.db_path
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    #[test]
    fn test_file_metadata_encode_decode() {
        let meta = FileMetaData::new(
            42,
            1024,
            b"aaa".to_vec(),
            b"zzz".to_vec(),
        );

        let encoded = meta.encode();
        let (decoded, len) = FileMetaData::decode(&encoded).unwrap();

        assert_eq!(len, encoded.len());
        assert_eq!(decoded.number, 42);
        assert_eq!(decoded.file_size, 1024);
        assert_eq!(decoded.smallest, b"aaa");
        assert_eq!(decoded.largest, b"zzz");
    }

    #[test]
    fn test_version_edit_encode_decode() {
        let mut edit = VersionEdit::new();
        edit.set_log_number(5);
        edit.set_last_sequence(100);
        edit.add_file(0, FileMetaData::new(1, 500, b"a".to_vec(), b"m".to_vec()));
        edit.add_file(1, FileMetaData::new(2, 600, b"n".to_vec(), b"z".to_vec()));
        edit.delete_file(0, 99);

        let encoded = edit.encode();
        let decoded = VersionEdit::decode(&encoded).unwrap();

        assert_eq!(decoded.log_number, Some(5));
        assert_eq!(decoded.last_sequence, Some(100));
        assert_eq!(decoded.new_files.len(), 2);
        assert_eq!(decoded.deleted_files.len(), 1);
        assert_eq!(decoded.deleted_files[0], (0, 99));
    }

    #[test]
    fn test_version_apply() {
        let version = Version::new();

        // Add files
        let mut edit = VersionEdit::new();
        edit.add_file(0, FileMetaData::new(1, 100, b"a".to_vec(), b"m".to_vec()));
        edit.add_file(0, FileMetaData::new(2, 100, b"n".to_vec(), b"z".to_vec()));

        let version = version.apply(&edit);
        assert_eq!(version.files(0).len(), 2);
        assert_eq!(version.num_files(), 2);

        // Delete one file
        let mut edit = VersionEdit::new();
        edit.delete_file(0, 1);

        let version = version.apply(&edit);
        assert_eq!(version.files(0).len(), 1);
        assert_eq!(version.files(0)[0].number, 2);
    }

    #[test]
    fn test_manifest_new_db() {
        let tmp = TempDir::new().unwrap();
        let manifest = Manifest::open(tmp.path()).unwrap();

        assert_eq!(manifest.current().num_files(), 0);
        assert!(tmp.path().join("CURRENT").exists());
        assert!(tmp.path().join("MANIFEST-000001").exists());
    }

    #[test]
    fn test_manifest_log_and_apply() {
        let tmp = TempDir::new().unwrap();
        let mut manifest = Manifest::open(tmp.path()).unwrap();

        let mut edit = VersionEdit::new();
        edit.add_file(0, FileMetaData::new(10, 1000, b"foo".to_vec(), b"zoo".to_vec()));

        manifest.log_and_apply(&edit).unwrap();

        assert_eq!(manifest.current().num_files(), 1);
        assert_eq!(manifest.current().files(0)[0].number, 10);
    }

    #[test]
    fn test_manifest_recovery() {
        let tmp = TempDir::new().unwrap();

        // Create and populate
        {
            let mut manifest = Manifest::open(tmp.path()).unwrap();

            let mut edit = VersionEdit::new();
            edit.add_file(0, FileMetaData::new(1, 100, b"a".to_vec(), b"m".to_vec()));
            manifest.log_and_apply(&edit).unwrap();

            let mut edit = VersionEdit::new();
            edit.add_file(1, FileMetaData::new(2, 200, b"n".to_vec(), b"z".to_vec()));
            manifest.log_and_apply(&edit).unwrap();
        }

        // Recover
        let manifest = Manifest::open(tmp.path()).unwrap();
        assert_eq!(manifest.current().num_files(), 2);
        assert_eq!(manifest.current().files(0).len(), 1);
        assert_eq!(manifest.current().files(1).len(), 1);
    }

    #[test]
    fn test_file_number_allocation() {
        let tmp = TempDir::new().unwrap();
        let manifest = Manifest::open(tmp.path()).unwrap();

        let n1 = manifest.new_file_number();
        let n2 = manifest.new_file_number();
        let n3 = manifest.new_file_number();

        assert!(n2 > n1);
        assert!(n3 > n2);
    }
}
