//! Write-ahead log implementation.

use std::fs::{File, OpenOptions};
use std::io::{BufReader, BufWriter, Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};

use crate::Result;
use super::record::{LogRecord, RecordType, BLOCK_SIZE, HEADER_SIZE};

/// WAL writer.
pub struct WalWriter {
    path: PathBuf,
    file: BufWriter<File>,
    block_offset: usize,
}

impl WalWriter {
    /// Create a new WAL writer.
    pub fn new(path: &Path) -> Result<Self> {
        let file = OpenOptions::new()
            .create(true)
            .write(true)
            .append(true)
            .open(path)?;

        let block_offset = file.metadata()?.len() as usize % BLOCK_SIZE;

        Ok(Self {
            path: path.to_path_buf(),
            file: BufWriter::new(file),
            block_offset,
        })
    }

    /// Append data to the log.
    pub fn append(&mut self, data: &[u8]) -> Result<()> {
        let mut remaining = data;
        let mut first = true;

        while !remaining.is_empty() {
            let space_in_block = BLOCK_SIZE - self.block_offset;

            if space_in_block < HEADER_SIZE {
                // Pad rest of block with zeros
                self.file.write_all(&vec![0u8; space_in_block])?;
                self.block_offset = 0;
                continue;
            }

            let available = space_in_block - HEADER_SIZE;
            let fragment_len = std::cmp::min(available, remaining.len());
            let last = fragment_len == remaining.len();

            let record_type = match (first, last) {
                (true, true) => RecordType::Full,
                (true, false) => RecordType::First,
                (false, true) => RecordType::Last,
                (false, false) => RecordType::Middle,
            };

            let record = LogRecord::new(remaining[..fragment_len].to_vec(), record_type);
            let encoded = record.encode();

            self.file.write_all(&encoded)?;
            self.block_offset = (self.block_offset + encoded.len()) % BLOCK_SIZE;

            remaining = &remaining[fragment_len..];
            first = false;
        }

        Ok(())
    }

    /// Sync to disk.
    pub fn sync(&mut self) -> Result<()> {
        self.file.flush()?;
        self.file.get_ref().sync_all()?;
        Ok(())
    }
}

/// WAL reader.
pub struct WalReader {
    file: BufReader<File>,
    block: Vec<u8>,
    block_offset: usize,
    eof: bool,
}

impl WalReader {
    /// Create a new WAL reader.
    pub fn new(path: &Path) -> Result<Self> {
        let file = File::open(path)?;
        Ok(Self {
            file: BufReader::new(file),
            block: vec![0u8; BLOCK_SIZE],
            block_offset: BLOCK_SIZE, // Force read on first access
            eof: false,
        })
    }

    /// Read the next record.
    pub fn read_record(&mut self) -> Result<Option<Vec<u8>>> {
        if self.eof {
            return Ok(None);
        }

        let mut result = Vec::new();
        let mut in_fragmented = false;

        loop {
            // Read new block if needed
            if self.block_offset >= BLOCK_SIZE {
                match self.file.read_exact(&mut self.block) {
                    Ok(()) => {}
                    Err(e) if e.kind() == std::io::ErrorKind::UnexpectedEof => {
                        self.eof = true;
                        if in_fragmented {
                            return Err(crate::Error::Corruption("truncated record".into()));
                        }
                        return Ok(None);
                    }
                    Err(e) => return Err(e.into()),
                }
                self.block_offset = 0;
            }

            // Skip zero padding
            while self.block_offset < BLOCK_SIZE && self.block[self.block_offset] == 0 {
                self.block_offset += 1;
            }

            if self.block_offset >= BLOCK_SIZE {
                continue;
            }

            // Decode record
            let record = LogRecord::decode(&self.block[self.block_offset..])?;
            self.block_offset += HEADER_SIZE + record.data.len();

            result.extend_from_slice(&record.data);

            match record.record_type {
                RecordType::Full => return Ok(Some(result)),
                RecordType::First => in_fragmented = true,
                RecordType::Middle => {
                    if !in_fragmented {
                        return Err(crate::Error::Corruption("unexpected middle record".into()));
                    }
                }
                RecordType::Last => {
                    if !in_fragmented {
                        return Err(crate::Error::Corruption("unexpected last record".into()));
                    }
                    return Ok(Some(result));
                }
            }
        }
    }

    /// Read all records.
    pub fn read_all(&mut self) -> Result<Vec<Vec<u8>>> {
        let mut records = Vec::new();
        while let Some(record) = self.read_record()? {
            records.push(record);
        }
        Ok(records)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::NamedTempFile;

    #[test]
    fn test_write_read_single() {
        let tmp = NamedTempFile::new().unwrap();
        let path = tmp.path();

        {
            let mut writer = WalWriter::new(path).unwrap();
            writer.append(b"hello world").unwrap();
            writer.sync().unwrap();
        }

        {
            let mut reader = WalReader::new(path).unwrap();
            let record = reader.read_record().unwrap().unwrap();
            assert_eq!(record, b"hello world");
            assert!(reader.read_record().unwrap().is_none());
        }
    }

    #[test]
    fn test_write_read_multiple() {
        let tmp = NamedTempFile::new().unwrap();
        let path = tmp.path();

        {
            let mut writer = WalWriter::new(path).unwrap();
            writer.append(b"first").unwrap();
            writer.append(b"second").unwrap();
            writer.append(b"third").unwrap();
            writer.sync().unwrap();
        }

        {
            let mut reader = WalReader::new(path).unwrap();
            let records = reader.read_all().unwrap();
            assert_eq!(records.len(), 3);
            assert_eq!(records[0], b"first");
            assert_eq!(records[1], b"second");
            assert_eq!(records[2], b"third");
        }
    }

    #[test]
    fn test_large_record_spans_blocks() {
        let tmp = NamedTempFile::new().unwrap();
        let path = tmp.path();

        // Create data larger than one block
        let large_data = vec![42u8; BLOCK_SIZE * 2 + 1000];

        {
            let mut writer = WalWriter::new(path).unwrap();
            writer.append(&large_data).unwrap();
            writer.sync().unwrap();
        }

        {
            let mut reader = WalReader::new(path).unwrap();
            let record = reader.read_record().unwrap().unwrap();
            assert_eq!(record, large_data);
        }
    }
}
