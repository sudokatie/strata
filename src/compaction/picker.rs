//! Compaction picker - selects files for compaction.

use crate::manifest::{Version, FileMetaData, MAX_LEVELS};

/// Default L0 compaction trigger.
pub const L0_COMPACTION_TRIGGER: usize = 4;

/// Default level size multiplier (level N+1 is 10x level N).
pub const LEVEL_SIZE_MULTIPLIER: u64 = 10;

/// Base level size in bytes (10MB).
pub const BASE_LEVEL_SIZE: u64 = 10 * 1024 * 1024;

/// Compaction input - files to compact.
#[derive(Debug, Clone)]
pub struct CompactionInput {
    /// Level being compacted from.
    pub level: usize,
    /// Files from the source level.
    pub input_files: Vec<FileMetaData>,
    /// Files from the target level (level + 1) that overlap.
    pub output_level_files: Vec<FileMetaData>,
}

impl CompactionInput {
    /// Total bytes to read.
    pub fn total_bytes(&self) -> u64 {
        self.input_files.iter().map(|f| f.file_size).sum::<u64>()
            + self.output_level_files.iter().map(|f| f.file_size).sum::<u64>()
    }
}

/// Compaction picker.
pub struct CompactionPicker {
    /// L0 compaction trigger (number of files).
    l0_trigger: usize,
    /// Base level size.
    base_level_size: u64,
    /// Level size multiplier.
    level_multiplier: u64,
}

impl CompactionPicker {
    /// Create with default settings.
    pub fn new() -> Self {
        Self {
            l0_trigger: L0_COMPACTION_TRIGGER,
            base_level_size: BASE_LEVEL_SIZE,
            level_multiplier: LEVEL_SIZE_MULTIPLIER,
        }
    }

    /// Create with custom settings.
    pub fn with_config(l0_trigger: usize, base_level_size: u64, level_multiplier: u64) -> Self {
        Self {
            l0_trigger,
            base_level_size,
            level_multiplier,
        }
    }

    /// Pick files for compaction. Returns None if no compaction needed.
    pub fn pick_compaction(&self, version: &Version) -> Option<CompactionInput> {
        // First check L0 - it has highest priority
        if let Some(input) = self.pick_l0_compaction(version) {
            return Some(input);
        }

        // Then check each level
        for level in 1..MAX_LEVELS - 1 {
            if let Some(input) = self.pick_level_compaction(version, level) {
                return Some(input);
            }
        }

        None
    }

    /// Pick L0 compaction when file count exceeds trigger.
    fn pick_l0_compaction(&self, version: &Version) -> Option<CompactionInput> {
        let l0_files = version.files(0);
        
        if l0_files.len() < self.l0_trigger {
            return None;
        }

        // L0 files may overlap, so we need to compact all of them
        let input_files: Vec<_> = l0_files.to_vec();
        
        // Find overlapping files in L1
        let output_level_files = self.find_overlapping_files(version, 1, &input_files);

        Some(CompactionInput {
            level: 0,
            input_files,
            output_level_files,
        })
    }

    /// Pick level compaction when level size exceeds limit.
    fn pick_level_compaction(&self, version: &Version, level: usize) -> Option<CompactionInput> {
        let files = version.files(level);
        let level_size: u64 = files.iter().map(|f| f.file_size).sum();
        let max_size = self.max_bytes_for_level(level);

        if level_size <= max_size {
            return None;
        }

        // Pick the file with the largest size (simple strategy)
        let file = files.iter()
            .max_by_key(|f| f.file_size)?
            .clone();

        let input_files = vec![file];
        
        // Find overlapping files in next level
        let output_level_files = if level + 1 < MAX_LEVELS {
            self.find_overlapping_files(version, level + 1, &input_files)
        } else {
            vec![]
        };

        Some(CompactionInput {
            level,
            input_files,
            output_level_files,
        })
    }

    /// Calculate max bytes for a level.
    fn max_bytes_for_level(&self, level: usize) -> u64 {
        if level == 0 {
            // L0 is special - trigger based on file count
            u64::MAX
        } else {
            self.base_level_size * self.level_multiplier.pow(level as u32 - 1)
        }
    }

    /// Find files in target level that overlap with input files.
    fn find_overlapping_files(
        &self,
        version: &Version,
        target_level: usize,
        input_files: &[FileMetaData],
    ) -> Vec<FileMetaData> {
        if input_files.is_empty() {
            return vec![];
        }

        // Get the key range of input files
        let smallest: &[u8] = input_files.iter()
            .map(|f| f.smallest.as_slice())
            .min()
            .unwrap();
        let largest: &[u8] = input_files.iter()
            .map(|f| f.largest.as_slice())
            .max()
            .unwrap();

        // Find overlapping files in target level
        version.files(target_level)
            .iter()
            .filter(|f| {
                // File overlaps if its range intersects [smallest, largest]
                f.largest.as_slice() >= smallest && f.smallest.as_slice() <= largest
            })
            .cloned()
            .collect()
    }

    /// Calculate compaction score for a level.
    /// Score > 1.0 means compaction is needed.
    pub fn level_score(&self, version: &Version, level: usize) -> f64 {
        if level == 0 {
            version.files(0).len() as f64 / self.l0_trigger as f64
        } else {
            let size: u64 = version.files(level).iter().map(|f| f.file_size).sum();
            let max_size = self.max_bytes_for_level(level);
            size as f64 / max_size as f64
        }
    }

    /// Get scores for all levels.
    pub fn all_scores(&self, version: &Version) -> Vec<(usize, f64)> {
        (0..MAX_LEVELS)
            .map(|level| (level, self.level_score(version, level)))
            .filter(|(_, score)| *score > 0.0)
            .collect()
    }
}

impl Default for CompactionPicker {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::manifest::VersionEdit;

    fn make_file(number: u64, size: u64, smallest: &str, largest: &str) -> FileMetaData {
        FileMetaData::new(
            number,
            size,
            smallest.as_bytes().to_vec(),
            largest.as_bytes().to_vec(),
        )
    }

    fn version_with_files(level: usize, files: Vec<FileMetaData>) -> Version {
        let mut edit = VersionEdit::new();
        for file in files {
            edit.add_file(level, file);
        }
        Version::new().apply(&edit)
    }

    #[test]
    fn test_no_compaction_needed() {
        let picker = CompactionPicker::new();
        let version = Version::new();

        assert!(picker.pick_compaction(&version).is_none());
    }

    #[test]
    fn test_l0_compaction_trigger() {
        let picker = CompactionPicker::new();
        
        // Less than trigger - no compaction
        let files = vec![
            make_file(1, 1000, "a", "m"),
            make_file(2, 1000, "n", "z"),
        ];
        let version = version_with_files(0, files);
        assert!(picker.pick_compaction(&version).is_none());

        // At trigger - compaction needed
        let files = vec![
            make_file(1, 1000, "a", "f"),
            make_file(2, 1000, "g", "l"),
            make_file(3, 1000, "m", "r"),
            make_file(4, 1000, "s", "z"),
        ];
        let version = version_with_files(0, files);
        let input = picker.pick_compaction(&version).unwrap();
        
        assert_eq!(input.level, 0);
        assert_eq!(input.input_files.len(), 4);
    }

    #[test]
    fn test_level_compaction() {
        let picker = CompactionPicker::with_config(4, 1000, 10); // Small sizes for testing
        
        // L1 with files exceeding limit
        let files = vec![
            make_file(1, 600, "a", "m"),
            make_file(2, 600, "n", "z"),
        ];
        let version = version_with_files(1, files);
        
        let input = picker.pick_compaction(&version).unwrap();
        assert_eq!(input.level, 1);
        assert_eq!(input.input_files.len(), 1); // Picks largest file
    }

    #[test]
    fn test_find_overlapping_files() {
        let picker = CompactionPicker::new();
        
        // L0 files
        let l0_files = vec![
            make_file(1, 1000, "d", "h"),
        ];
        
        // L1 files - some overlap, some don't
        let l1_files = vec![
            make_file(10, 500, "a", "c"),   // No overlap
            make_file(11, 500, "c", "e"),   // Overlaps
            make_file(12, 500, "f", "j"),   // Overlaps
            make_file(13, 500, "k", "z"),   // No overlap
        ];
        
        let mut edit = VersionEdit::new();
        for f in &l0_files {
            edit.add_file(0, f.clone());
        }
        for f in &l1_files {
            edit.add_file(1, f.clone());
        }
        let version = Version::new().apply(&edit);

        let overlapping = picker.find_overlapping_files(&version, 1, &l0_files);
        
        assert_eq!(overlapping.len(), 2);
        assert!(overlapping.iter().any(|f| f.number == 11));
        assert!(overlapping.iter().any(|f| f.number == 12));
    }

    #[test]
    fn test_level_score() {
        let picker = CompactionPicker::with_config(4, 1000, 10);
        
        // L0 score based on file count
        let files = vec![
            make_file(1, 100, "a", "m"),
            make_file(2, 100, "n", "z"),
        ];
        let version = version_with_files(0, files);
        assert!((picker.level_score(&version, 0) - 0.5).abs() < 0.01);

        // L1 score based on size
        let files = vec![
            make_file(1, 500, "a", "z"),
        ];
        let version = version_with_files(1, files);
        assert!((picker.level_score(&version, 1) - 0.5).abs() < 0.01);
    }

    #[test]
    fn test_all_scores() {
        let picker = CompactionPicker::with_config(4, 1000, 10);
        
        let mut edit = VersionEdit::new();
        edit.add_file(0, make_file(1, 100, "a", "m"));
        edit.add_file(1, make_file(2, 500, "a", "z"));
        let version = Version::new().apply(&edit);

        let scores = picker.all_scores(&version);
        assert!(scores.len() >= 2);
    }
}
