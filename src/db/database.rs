//! Main database struct.

use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex, Condvar};
use std::thread::{self, JoinHandle};
use std::sync::atomic::{AtomicBool, Ordering};

use crate::manifest::{Manifest, VersionEdit, FileMetaData};
use crate::memtable::{MemTable, ImmutableMemTable};
use crate::sstable::{SSTableBuilder, SSTableReader};
use crate::wal::WalWriter;
use crate::compaction::{CompactionPicker, LeveledCompaction};
use crate::types::{Key, Value, Entry, EntryType};
use crate::{Result, Options};
use super::snapshot::Snapshot;

/// Default memtable size before flush (4MB).
const DEFAULT_MEMTABLE_SIZE: usize = 4 * 1024 * 1024;

/// Database instance.
pub struct DB {
    inner: Arc<DBInner>,
    /// Background flush thread handle.
    flush_handle: Option<JoinHandle<()>>,
    /// Background compaction thread handle.
    compact_handle: Option<JoinHandle<()>>,
}

/// Inner database state protected by mutex.
struct DBInner {
    /// Database path.
    db_path: PathBuf,
    /// Configuration options.
    #[allow(dead_code)]
    options: Options,
    /// Manifest for version management.
    manifest: Mutex<Manifest>,
    /// Current mutable memtable.
    mem: Mutex<MemTable>,
    /// Immutable memtable waiting to be flushed.
    imm: Mutex<Option<ImmutableMemTable>>,
    /// Write-ahead log.
    wal: Mutex<Option<WalWriter>>,
    /// Shutdown flag.
    shutdown: AtomicBool,
    /// Condition variable for flush notification.
    flush_cv: Condvar,
    /// Condition variable for compaction notification.
    compact_cv: Condvar,
}

impl DB {
    /// Open a database.
    pub fn open(path: &Path, options: Options) -> Result<Self> {
        std::fs::create_dir_all(path)?;
        
        // Open manifest
        let manifest = Manifest::open(path)?;
        
        // Create WAL
        let wal_num = manifest.new_file_number();
        let wal_path = path.join(format!("{:06}.log", wal_num));
        let wal = WalWriter::new(&wal_path)?;
        
        // Record WAL number in manifest
        let mut edit = VersionEdit::new();
        edit.set_log_number(wal_num);
        // Note: We'd apply this but Manifest is already owned

        let inner = Arc::new(DBInner {
            db_path: path.to_path_buf(),
            options,
            manifest: Mutex::new(manifest),
            mem: Mutex::new(MemTable::new()),
            imm: Mutex::new(None),
            wal: Mutex::new(Some(wal)),
            shutdown: AtomicBool::new(false),
            flush_cv: Condvar::new(),
            compact_cv: Condvar::new(),
        });

        // Start background threads
        let flush_inner = Arc::clone(&inner);
        let flush_handle = thread::spawn(move || {
            flush_thread(flush_inner);
        });

        let compact_inner = Arc::clone(&inner);
        let compact_handle = thread::spawn(move || {
            compact_thread(compact_inner);
        });

        Ok(Self {
            inner,
            flush_handle: Some(flush_handle),
            compact_handle: Some(compact_handle),
        })
    }

    /// Put a key-value pair.
    pub fn put(&self, key: &Key, value: &Value) -> Result<()> {
        self.write_entry(key, value, EntryType::Put)
    }

    /// Delete a key.
    pub fn delete(&self, key: &Key) -> Result<()> {
        self.write_entry(key, &Value::new(vec![]), EntryType::Delete)
    }

    fn write_entry(&self, key: &Key, value: &Value, entry_type: EntryType) -> Result<()> {
        // Write to WAL first
        {
            let mut wal_guard = self.inner.wal.lock().unwrap();
            if let Some(ref mut wal) = *wal_guard {
                let entry = match entry_type {
                    EntryType::Put => Entry::put(key.clone(), value.clone(), 0),
                    EntryType::Delete => Entry::delete(key.clone(), 0),
                };
                wal.append(&entry.encode())?;
            }
        }

        // Write to memtable
        {
            let mut mem = self.inner.mem.lock().unwrap();
            match entry_type {
                EntryType::Put => mem.put(key.clone(), value.clone()),
                EntryType::Delete => mem.delete(key.clone()),
            }

            // Check if memtable needs flushing
            if mem.approximate_size() >= DEFAULT_MEMTABLE_SIZE {
                self.maybe_schedule_flush(&mut mem)?;
            }
        }

        Ok(())
    }

    fn maybe_schedule_flush(&self, mem: &mut MemTable) -> Result<()> {
        // Check if there's already an immutable memtable
        let mut imm_guard = self.inner.imm.lock().unwrap();
        if imm_guard.is_some() {
            // Wait for previous flush to complete
            return Ok(());
        }

        // Freeze current memtable
        let old_mem = std::mem::take(mem);
        *imm_guard = Some(old_mem.freeze());

        // Create new WAL
        {
            let manifest = self.inner.manifest.lock().unwrap();
            let wal_num = manifest.new_file_number();
            let wal_path = self.inner.db_path.join(format!("{:06}.log", wal_num));
            let new_wal = WalWriter::new(&wal_path)?;
            
            let mut wal_guard = self.inner.wal.lock().unwrap();
            *wal_guard = Some(new_wal);
        }

        // Signal flush thread
        self.inner.flush_cv.notify_one();

        Ok(())
    }

    /// Get a value by key.
    pub fn get(&self, key: &Key) -> Result<Option<Value>> {
        // Check memtable first
        {
            let mem = self.inner.mem.lock().unwrap();
            if let Some(entry) = mem.get_entry(key) {
                return Ok(match entry.entry_type {
                    EntryType::Put => Some(entry.value),
                    EntryType::Delete => None,
                });
            }
        }

        // Check immutable memtable
        {
            let imm = self.inner.imm.lock().unwrap();
            if let Some(ref imm_table) = *imm {
                if let Some(entry) = imm_table.get_entry(key) {
                    return Ok(match entry.entry_type {
                        EntryType::Put => Some(entry.value),
                        EntryType::Delete => None,
                    });
                }
            }
        }

        // Check SSTables level by level
        let manifest = self.inner.manifest.lock().unwrap();
        let version = manifest.current();

        for level in 0..7 {
            let files = version.files(level);
            
            // For L0, check all files (they may overlap)
            // For L1+, files are sorted and non-overlapping
            for file in files {
                // Quick key range check
                if key.as_bytes() < file.smallest.as_slice()
                    || key.as_bytes() > file.largest.as_slice()
                {
                    continue;
                }

                let path = self.inner.db_path.join(format!("{:06}.sst", file.number));
                if let Ok(mut reader) = SSTableReader::open(&path) {
                    if let Ok(Some(value)) = reader.get(key) {
                        return Ok(Some(value));
                    }
                }
            }
        }

        Ok(None)
    }

    /// Force a flush of the memtable.
    pub fn flush(&self) -> Result<()> {
        // Move current memtable to immutable
        {
            let mut mem = self.inner.mem.lock().unwrap();
            if mem.is_empty() {
                return Ok(());
            }

            let mut imm = self.inner.imm.lock().unwrap();
            
            // Wait if there's already an immutable memtable
            while imm.is_some() {
                imm = self.inner.flush_cv.wait(imm).unwrap();
            }

            let old_mem = std::mem::take(&mut *mem);
            *imm = Some(old_mem.freeze());
        }

        // Signal and wait for flush
        self.inner.flush_cv.notify_one();

        // Wait for flush to complete
        loop {
            let imm = self.inner.imm.lock().unwrap();
            if imm.is_none() {
                break;
            }
            std::thread::sleep(std::time::Duration::from_millis(10));
        }

        Ok(())
    }

    /// Trigger compaction.
    pub fn compact(&self) -> Result<()> {
        self.inner.compact_cv.notify_one();
        Ok(())
    }

    /// Get database statistics.
    pub fn stats(&self) -> DBStats {
        let manifest = self.inner.manifest.lock().unwrap();
        let version = manifest.current();
        let mem = self.inner.mem.lock().unwrap();
        let imm = self.inner.imm.lock().unwrap();

        let mut level_stats = Vec::new();
        for level in 0..7 {
            let files = version.files(level);
            if !files.is_empty() {
                let total_size: u64 = files.iter().map(|f| f.file_size).sum();
                level_stats.push(LevelStats {
                    level,
                    num_files: files.len(),
                    total_bytes: total_size,
                });
            }
        }

        DBStats {
            memtable_size: mem.approximate_size(),
            memtable_entries: mem.len(),
            immutable_memtable: imm.is_some(),
            levels: level_stats,
            total_files: version.num_files(),
        }
    }

    /// Create a snapshot for point-in-time reads.
    /// The snapshot captures the current state - entries written after 
    /// this point will not be visible through get_at().
    pub fn snapshot(&self) -> Snapshot {
        let mem = self.inner.mem.lock().unwrap();
        // sequence() returns the NEXT sequence to be assigned
        // So we use sequence() - 1 to capture the current state
        let seq = mem.sequence();
        Snapshot::new(seq.saturating_sub(1))
    }

    /// Get a value by key at a specific snapshot.
    pub fn get_at(&self, key: &Key, snapshot: &Snapshot) -> Result<Option<Value>> {
        let max_seq = snapshot.sequence();

        // Check memtable - need to scan for entries matching key with seq <= max_seq
        {
            let mem = self.inner.mem.lock().unwrap();
            // Find the newest entry for this key with sequence <= max_seq
            let mut best_entry: Option<Entry> = None;
            for entry in mem.iter() {
                if entry.key == *key && entry.sequence <= max_seq {
                    match &best_entry {
                        None => best_entry = Some(entry),
                        Some(best) if entry.sequence > best.sequence => {
                            best_entry = Some(entry);
                        }
                        _ => {}
                    }
                }
            }
            if let Some(entry) = best_entry {
                return Ok(match entry.entry_type {
                    EntryType::Put => Some(entry.value),
                    EntryType::Delete => None,
                });
            }
        }

        // Check immutable memtable
        {
            let imm = self.inner.imm.lock().unwrap();
            if let Some(ref imm_table) = *imm {
                let mut best_entry: Option<Entry> = None;
                for entry in imm_table.iter() {
                    if entry.key == *key && entry.sequence <= max_seq {
                        match &best_entry {
                            None => best_entry = Some(entry),
                            Some(best) if entry.sequence > best.sequence => {
                                best_entry = Some(entry);
                            }
                            _ => {}
                        }
                    }
                }
                if let Some(entry) = best_entry {
                    return Ok(match entry.entry_type {
                        EntryType::Put => Some(entry.value),
                        EntryType::Delete => None,
                    });
                }
            }
        }

        // Check SSTables (they store older data, so sequence check less critical)
        // For full snapshot isolation, SSTable entries would also need sequence filtering
        let manifest = self.inner.manifest.lock().unwrap();
        let version = manifest.current();

        for level in 0..7 {
            let files = version.files(level);
            for file in files {
                if key.as_bytes() < file.smallest.as_slice()
                    || key.as_bytes() > file.largest.as_slice()
                {
                    continue;
                }

                let path = self.inner.db_path.join(format!("{:06}.sst", file.number));
                if let Ok(mut reader) = SSTableReader::open(&path) {
                    if let Ok(Some(value)) = reader.get(key) {
                        return Ok(Some(value));
                    }
                }
            }
        }

        Ok(None)
    }
}

/// Statistics for a single level.
#[derive(Debug, Clone)]
pub struct LevelStats {
    /// Level number.
    pub level: usize,
    /// Number of files.
    pub num_files: usize,
    /// Total bytes in level.
    pub total_bytes: u64,
}

/// Database statistics.
#[derive(Debug, Clone)]
pub struct DBStats {
    /// Current memtable size in bytes.
    pub memtable_size: usize,
    /// Number of entries in memtable.
    pub memtable_entries: usize,
    /// Whether there's an immutable memtable pending flush.
    pub immutable_memtable: bool,
    /// Statistics per level.
    pub levels: Vec<LevelStats>,
    /// Total number of SSTable files.
    pub total_files: usize,
}

impl Drop for DB {
    fn drop(&mut self) {
        // Signal shutdown
        self.inner.shutdown.store(true, Ordering::SeqCst);
        self.inner.flush_cv.notify_all();
        self.inner.compact_cv.notify_all();

        // Wait for threads
        if let Some(handle) = self.flush_handle.take() {
            let _ = handle.join();
        }
        if let Some(handle) = self.compact_handle.take() {
            let _ = handle.join();
        }
    }
}

/// Background flush thread.
fn flush_thread(inner: Arc<DBInner>) {
    loop {
        // Wait for work or shutdown
        {
            let imm = inner.imm.lock().unwrap();
            let _guard = inner.flush_cv.wait_while(imm, |imm| {
                imm.is_none() && !inner.shutdown.load(Ordering::SeqCst)
            }).unwrap();
        }

        if inner.shutdown.load(Ordering::SeqCst) {
            break;
        }

        // Do the flush
        if let Err(e) = do_flush(&inner) {
            eprintln!("Flush error: {}", e);
        }

        // Notify waiters and compaction
        inner.flush_cv.notify_all();
        inner.compact_cv.notify_one();
    }
}

fn do_flush(inner: &DBInner) -> Result<()> {
    // Collect entries from immutable memtable
    let entries: Vec<Entry> = {
        let imm = inner.imm.lock().unwrap();
        match &*imm {
            Some(t) => t.iter().collect(),
            None => return Ok(()),
        }
    };

    if entries.is_empty() {
        let mut imm = inner.imm.lock().unwrap();
        *imm = None;
        return Ok(());
    }

    // Get file number
    let file_num = {
        let manifest = inner.manifest.lock().unwrap();
        manifest.new_file_number()
    };

    // Build SSTable
    let path = inner.db_path.join(format!("{:06}.sst", file_num));
    let mut builder = SSTableBuilder::new(&path)?;

    let mut smallest: Option<Vec<u8>> = None;
    let mut largest: Option<Vec<u8>> = None;

    for entry in &entries {
        builder.add(&entry.key, &entry.value)?;
        
        if smallest.is_none() {
            smallest = Some(entry.key.0.clone());
        }
        largest = Some(entry.key.0.clone());
    }

    let meta = builder.finish()?;

    // Update manifest
    {
        let mut manifest = inner.manifest.lock().unwrap();
        let mut edit = VersionEdit::new();
        edit.add_file(0, FileMetaData::new(
            file_num,
            meta.num_entries as u64,
            smallest.unwrap_or_default(),
            largest.unwrap_or_default(),
        ));
        manifest.log_and_apply(&edit)?;
    }

    // Clear immutable memtable
    {
        let mut imm = inner.imm.lock().unwrap();
        *imm = None;
    }

    Ok(())
}

/// Background compaction thread.
fn compact_thread(inner: Arc<DBInner>) {
    loop {
        // Wait for signal or shutdown
        {
            let manifest = inner.manifest.lock().unwrap();
            let _guard = inner.compact_cv.wait_timeout(
                manifest,
                std::time::Duration::from_secs(10),
            ).unwrap();
        }

        if inner.shutdown.load(Ordering::SeqCst) {
            break;
        }

        // Check if compaction needed
        if let Err(e) = do_compact(&inner) {
            eprintln!("Compaction error: {}", e);
        }
    }
}

fn do_compact(inner: &DBInner) -> Result<()> {
    let picker = CompactionPicker::new();
    
    let input = {
        let manifest = inner.manifest.lock().unwrap();
        picker.pick_compaction(manifest.current())
    };

    if let Some(input) = input {
        let mut manifest = inner.manifest.lock().unwrap();
        let mut compaction = LeveledCompaction::new(&mut manifest);
        compaction.compact(&input)?;
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    #[test]
    fn test_open_db() {
        let tmp = TempDir::new().unwrap();
        let db = DB::open(tmp.path(), Options::default()).unwrap();
        drop(db);
    }

    #[test]
    fn test_put_get() {
        let tmp = TempDir::new().unwrap();
        let db = DB::open(tmp.path(), Options::default()).unwrap();

        db.put(&Key::from("foo"), &Value::from("bar")).unwrap();
        
        let value = db.get(&Key::from("foo")).unwrap();
        assert_eq!(value.unwrap().as_bytes(), b"bar");
    }

    #[test]
    fn test_delete() {
        let tmp = TempDir::new().unwrap();
        let db = DB::open(tmp.path(), Options::default()).unwrap();

        db.put(&Key::from("key"), &Value::from("value")).unwrap();
        assert!(db.get(&Key::from("key")).unwrap().is_some());

        db.delete(&Key::from("key")).unwrap();
        assert!(db.get(&Key::from("key")).unwrap().is_none());
    }

    #[test]
    fn test_overwrite() {
        let tmp = TempDir::new().unwrap();
        let db = DB::open(tmp.path(), Options::default()).unwrap();

        db.put(&Key::from("key"), &Value::from("v1")).unwrap();
        db.put(&Key::from("key"), &Value::from("v2")).unwrap();
        
        let value = db.get(&Key::from("key")).unwrap();
        assert_eq!(value.unwrap().as_bytes(), b"v2");
    }

    #[test]
    fn test_persistence() {
        let tmp = TempDir::new().unwrap();
        let path = tmp.path().to_path_buf();

        // Write data
        {
            let db = DB::open(&path, Options::default()).unwrap();
            db.put(&Key::from("persist"), &Value::from("data")).unwrap();
            db.flush().unwrap();
        }

        // Reopen and verify
        {
            let db = DB::open(&path, Options::default()).unwrap();
            let value = db.get(&Key::from("persist")).unwrap();
            assert_eq!(value.unwrap().as_bytes(), b"data");
        }
    }

    #[test]
    fn test_many_writes() {
        let tmp = TempDir::new().unwrap();
        let db = DB::open(tmp.path(), Options::default()).unwrap();

        for i in 0..100 {
            let key = format!("key{:03}", i);
            let value = format!("value{}", i);
            db.put(&Key::from(key.as_str()), &Value::from(value.as_str())).unwrap();
        }

        for i in 0..100 {
            let key = format!("key{:03}", i);
            let expected = format!("value{}", i);
            let value = db.get(&Key::from(key.as_str())).unwrap().unwrap();
            assert_eq!(value.as_bytes(), expected.as_bytes());
        }
    }

    #[test]
    fn test_stats() {
        let tmp = TempDir::new().unwrap();
        let db = DB::open(tmp.path(), Options::default()).unwrap();

        // Empty database
        let stats = db.stats();
        assert_eq!(stats.memtable_entries, 0);
        assert_eq!(stats.total_files, 0);

        // Add some entries
        for i in 0..10 {
            let key = format!("key{}", i);
            db.put(&Key::from(key.as_str()), &Value::from("value")).unwrap();
        }

        let stats = db.stats();
        assert_eq!(stats.memtable_entries, 10);
        assert!(stats.memtable_size > 0);
    }

    #[test]
    fn test_stats_after_flush() {
        let tmp = TempDir::new().unwrap();
        let db = DB::open(tmp.path(), Options::default()).unwrap();

        for i in 0..10 {
            let key = format!("key{}", i);
            db.put(&Key::from(key.as_str()), &Value::from("value")).unwrap();
        }

        db.flush().unwrap();

        let stats = db.stats();
        // After flush, memtable should be empty
        assert_eq!(stats.memtable_entries, 0);
        // Should have SSTable files
        assert!(stats.total_files > 0);
    }

    #[test]
    fn test_snapshot() {
        let tmp = TempDir::new().unwrap();
        let db = DB::open(tmp.path(), Options::default()).unwrap();

        // Write initial value
        db.put(&Key::from("key"), &Value::from("v1")).unwrap();

        // Take snapshot
        let snap = db.snapshot();

        // Write new value
        db.put(&Key::from("key"), &Value::from("v2")).unwrap();

        // Regular get sees new value
        let value = db.get(&Key::from("key")).unwrap().unwrap();
        assert_eq!(value.as_bytes(), b"v2");

        // Snapshot read sees old value
        let value = db.get_at(&Key::from("key"), &snap).unwrap().unwrap();
        assert_eq!(value.as_bytes(), b"v1");
    }

    #[test]
    fn test_snapshot_isolation() {
        let tmp = TempDir::new().unwrap();
        let db = DB::open(tmp.path(), Options::default()).unwrap();

        // Initial writes
        db.put(&Key::from("a"), &Value::from("1")).unwrap();
        db.put(&Key::from("b"), &Value::from("2")).unwrap();

        let snap1 = db.snapshot();

        // More writes
        db.put(&Key::from("a"), &Value::from("10")).unwrap();
        db.put(&Key::from("c"), &Value::from("3")).unwrap();

        let snap2 = db.snapshot();

        // Delete
        db.delete(&Key::from("b")).unwrap();

        // snap1 sees original values
        assert_eq!(db.get_at(&Key::from("a"), &snap1).unwrap().unwrap().as_bytes(), b"1");
        assert_eq!(db.get_at(&Key::from("b"), &snap1).unwrap().unwrap().as_bytes(), b"2");
        assert!(db.get_at(&Key::from("c"), &snap1).unwrap().is_none());

        // snap2 sees intermediate state
        assert_eq!(db.get_at(&Key::from("a"), &snap2).unwrap().unwrap().as_bytes(), b"10");
        assert_eq!(db.get_at(&Key::from("b"), &snap2).unwrap().unwrap().as_bytes(), b"2");
        assert_eq!(db.get_at(&Key::from("c"), &snap2).unwrap().unwrap().as_bytes(), b"3");

        // Current state
        assert_eq!(db.get(&Key::from("a")).unwrap().unwrap().as_bytes(), b"10");
        assert!(db.get(&Key::from("b")).unwrap().is_none()); // deleted
        assert_eq!(db.get(&Key::from("c")).unwrap().unwrap().as_bytes(), b"3");
    }
}
