//! Leveled compaction execution.

use std::path::Path;

use crate::manifest::{Manifest, VersionEdit, FileMetaData};
use crate::sstable::{SSTableBuilder, SSTableReader};
use crate::types::Key;
use crate::Result;
use super::merge::MergeEntry;
use super::picker::CompactionInput;

/// Maximum size for output SSTables (64MB).
pub const MAX_OUTPUT_FILE_SIZE: u64 = 64 * 1024 * 1024;

/// Leveled compaction executor.
pub struct LeveledCompaction<'a> {
    manifest: &'a mut Manifest,
}

impl<'a> LeveledCompaction<'a> {
    /// Create new compaction executor.
    pub fn new(manifest: &'a mut Manifest) -> Self {
        Self { manifest }
    }

    /// Execute compaction.
    pub fn compact(&mut self, input: &CompactionInput) -> Result<CompactionResult> {
        let db_path = self.manifest.db_path().to_path_buf();
        
        // Collect all entries from input files
        let mut all_entries: Vec<MergeEntry> = Vec::new();
        
        // Read from source level
        for file in &input.input_files {
            let path = sst_path(&db_path, file.number);
            let entries = read_sst_entries(&path)?;
            all_entries.extend(entries);
        }
        
        // Read from target level
        for file in &input.output_level_files {
            let path = sst_path(&db_path, file.number);
            let entries = read_sst_entries(&path)?;
            all_entries.extend(entries);
        }

        // Sort all entries by key, then by sequence (descending)
        all_entries.sort_by(|a, b| {
            match a.key.cmp(&b.key) {
                std::cmp::Ordering::Equal => b.sequence.cmp(&a.sequence),
                ord => ord,
            }
        });

        // Deduplicate - keep only newest for each key
        let mut deduped: Vec<MergeEntry> = Vec::new();
        let mut last_key: Option<Key> = None;
        
        for entry in all_entries {
            if Some(&entry.key) != last_key.as_ref() {
                last_key = Some(entry.key.clone());
                deduped.push(entry);
            }
        }

        // Write output files
        let target_level = input.level + 1;
        let output_files = self.write_output_files(&db_path, &deduped, target_level)?;

        // Build version edit
        let mut edit = VersionEdit::new();
        
        // Mark input files as deleted
        for file in &input.input_files {
            edit.delete_file(input.level, file.number);
        }
        for file in &input.output_level_files {
            edit.delete_file(target_level, file.number);
        }
        
        // Add output files
        for file in &output_files {
            edit.add_file(target_level, file.clone());
        }

        // Apply edit to manifest
        self.manifest.log_and_apply(&edit)?;

        // Delete old files (safe now that manifest is updated)
        for file in &input.input_files {
            let path = sst_path(&db_path, file.number);
            let _ = std::fs::remove_file(&path); // Ignore errors
        }
        for file in &input.output_level_files {
            let path = sst_path(&db_path, file.number);
            let _ = std::fs::remove_file(&path);
        }

        Ok(CompactionResult {
            input_files: input.input_files.len() + input.output_level_files.len(),
            output_files: output_files.len(),
            input_bytes: input.total_bytes(),
            output_bytes: output_files.iter().map(|f| f.file_size).sum(),
        })
    }

    /// Write output SSTable files.
    fn write_output_files(
        &mut self,
        db_path: &Path,
        entries: &[MergeEntry],
        _level: usize,
    ) -> Result<Vec<FileMetaData>> {
        if entries.is_empty() {
            return Ok(vec![]);
        }

        let mut output_files = Vec::new();
        let mut current_builder: Option<(SSTableBuilder, u64, Vec<u8>)> = None;
        let mut current_size = 0u64;

        for entry in entries {
            // Skip tombstones at bottom level (they can be dropped)
            // For now, keep all entries
            
            // Start new file if needed
            if current_builder.is_none() || current_size >= MAX_OUTPUT_FILE_SIZE {
                // Finish current builder
                if let Some((builder, file_num, smallest)) = current_builder.take() {
                    let meta = builder.finish()?;
                    let largest = entries.iter()
                        .take_while(|e| e.key.as_bytes() <= entry.key.as_bytes())
                        .last()
                        .map(|e| e.key.0.clone())
                        .unwrap_or_default();
                    
                    output_files.push(FileMetaData::new(
                        file_num,
                        meta.num_entries as u64, // Approximate
                        smallest,
                        largest,
                    ));
                }

                // Start new builder
                let file_num = self.manifest.new_file_number();
                let path = sst_path(db_path, file_num);
                let builder = SSTableBuilder::new(&path)?;
                current_builder = Some((builder, file_num, entry.key.0.clone()));
                current_size = 0;
            }

            // Add entry
            if let Some((ref mut builder, _, _)) = current_builder {
                builder.add(&entry.key, &entry.value)?;
                current_size += entry.key.len() as u64 + entry.value.len() as u64 + 16;
            }
        }

        // Finish last builder
        if let Some((builder, file_num, smallest)) = current_builder {
            let meta = builder.finish()?;
            let largest = entries.last()
                .map(|e| e.key.0.clone())
                .unwrap_or_default();
            
            output_files.push(FileMetaData::new(
                file_num,
                meta.num_entries as u64,
                smallest,
                largest,
            ));
        }

        Ok(output_files)
    }
}

/// Result of a compaction operation.
#[derive(Debug)]
pub struct CompactionResult {
    pub input_files: usize,
    pub output_files: usize,
    pub input_bytes: u64,
    pub output_bytes: u64,
}

/// Get SSTable path for a file number.
fn sst_path(db_path: &Path, file_number: u64) -> std::path::PathBuf {
    db_path.join(format!("{:06}.sst", file_number))
}

/// Read entries from an SSTable.
fn read_sst_entries(path: &Path) -> Result<Vec<MergeEntry>> {
    if !path.exists() {
        return Ok(vec![]);
    }

    let mut reader = SSTableReader::open(path)?;
    let mut entries = Vec::new();

    for result in reader.iter()? {
        let (key, value) = result?;
        entries.push(MergeEntry::from_kv(key, value, 0));
    }

    Ok(entries)
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;
    use crate::manifest::Manifest;
    use crate::types::Value;

    fn create_test_sst(
        db_path: &Path,
        file_num: u64,
        entries: &[(&str, &str)],
    ) -> Result<FileMetaData> {
        let path = sst_path(db_path, file_num);
        let mut builder = SSTableBuilder::new(&path)?;
        
        let mut smallest = String::new();
        let mut largest = String::new();
        
        for (i, (k, v)) in entries.iter().enumerate() {
            builder.add(&Key::from(*k), &Value::from(*v))?;
            if i == 0 {
                smallest = k.to_string();
            }
            largest = k.to_string();
        }
        
        let meta = builder.finish()?;
        
        Ok(FileMetaData::new(
            file_num,
            meta.num_entries as u64,
            smallest.into_bytes(),
            largest.into_bytes(),
        ))
    }

    #[test]
    fn test_compact_l0_to_l1() {
        let tmp = TempDir::new().unwrap();
        let mut manifest = Manifest::open(tmp.path()).unwrap();

        // Create L0 files
        let f1 = create_test_sst(tmp.path(), 1, &[
            ("a", "1"), ("c", "3"), ("e", "5"),
        ]).unwrap();
        let f2 = create_test_sst(tmp.path(), 2, &[
            ("b", "2"), ("d", "4"), ("f", "6"),
        ]).unwrap();

        // Add to manifest
        let mut edit = VersionEdit::new();
        edit.add_file(0, f1.clone());
        edit.add_file(0, f2.clone());
        manifest.log_and_apply(&edit).unwrap();

        // Compact
        let input = CompactionInput {
            level: 0,
            input_files: vec![f1, f2],
            output_level_files: vec![],
        };

        let mut compaction = LeveledCompaction::new(&mut manifest);
        let result = compaction.compact(&input).unwrap();

        assert_eq!(result.input_files, 2);
        assert!(result.output_files >= 1);

        // Verify L0 is empty, L1 has files
        assert_eq!(manifest.current().files(0).len(), 0);
        assert!(manifest.current().files(1).len() >= 1);
    }

    #[test]
    fn test_compact_with_overlapping_l1() {
        let tmp = TempDir::new().unwrap();
        let mut manifest = Manifest::open(tmp.path()).unwrap();

        // Create L0 file
        let f1 = create_test_sst(tmp.path(), 1, &[
            ("b", "new_b"), ("d", "new_d"),
        ]).unwrap();

        // Create L1 file that overlaps
        let f2 = create_test_sst(tmp.path(), 2, &[
            ("a", "old_a"), ("c", "old_c"), ("e", "old_e"),
        ]).unwrap();

        let mut edit = VersionEdit::new();
        edit.add_file(0, f1.clone());
        edit.add_file(1, f2.clone());
        manifest.log_and_apply(&edit).unwrap();

        let input = CompactionInput {
            level: 0,
            input_files: vec![f1],
            output_level_files: vec![f2],
        };

        let mut compaction = LeveledCompaction::new(&mut manifest);
        let result = compaction.compact(&input).unwrap();

        assert_eq!(result.input_files, 2);
        
        // Both L0 and old L1 should be gone
        assert_eq!(manifest.current().files(0).len(), 0);
        // New L1 should have merged data
        assert!(manifest.current().files(1).len() >= 1);
    }

    #[test]
    fn test_empty_compaction() {
        let tmp = TempDir::new().unwrap();
        let mut manifest = Manifest::open(tmp.path()).unwrap();

        let input = CompactionInput {
            level: 0,
            input_files: vec![],
            output_level_files: vec![],
        };

        let mut compaction = LeveledCompaction::new(&mut manifest);
        let result = compaction.compact(&input).unwrap();

        assert_eq!(result.input_files, 0);
        assert_eq!(result.output_files, 0);
    }
}
