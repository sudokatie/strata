//! WAL record format.

use crate::Result;

/// Block size for WAL (32KB).
pub const BLOCK_SIZE: usize = 32 * 1024;

/// Header size: length(4) + type(1) + crc(4).
pub const HEADER_SIZE: usize = 9;

/// Record type for multi-block records.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RecordType {
    /// Complete record in one block.
    Full = 0,
    /// First fragment of a record.
    First = 1,
    /// Middle fragment.
    Middle = 2,
    /// Last fragment.
    Last = 3,
}

impl RecordType {
    /// Convert from byte.
    pub fn from_byte(b: u8) -> Option<Self> {
        match b {
            0 => Some(RecordType::Full),
            1 => Some(RecordType::First),
            2 => Some(RecordType::Middle),
            3 => Some(RecordType::Last),
            _ => None,
        }
    }
}

/// Log record.
#[derive(Debug, Clone)]
pub struct LogRecord {
    /// Record data.
    pub data: Vec<u8>,
    /// Record type.
    pub record_type: RecordType,
}

impl LogRecord {
    /// Create a new record.
    pub fn new(data: Vec<u8>, record_type: RecordType) -> Self {
        Self { data, record_type }
    }

    /// Encode record with header.
    pub fn encode(&self) -> Vec<u8> {
        let len = self.data.len() as u32;
        let crc = crc32fast::hash(&self.data);

        let mut buf = Vec::with_capacity(HEADER_SIZE + self.data.len());
        buf.extend_from_slice(&len.to_le_bytes());
        buf.push(self.record_type as u8);
        buf.extend_from_slice(&crc.to_le_bytes());
        buf.extend_from_slice(&self.data);

        buf
    }

    /// Decode record from bytes.
    pub fn decode(data: &[u8]) -> Result<Self> {
        if data.len() < HEADER_SIZE {
            return Err(crate::Error::InvalidRecord);
        }

        let len = u32::from_le_bytes(data[0..4].try_into().unwrap()) as usize;
        let record_type = RecordType::from_byte(data[4])
            .ok_or(crate::Error::InvalidRecord)?;
        let stored_crc = u32::from_le_bytes(data[5..9].try_into().unwrap());

        if data.len() < HEADER_SIZE + len {
            return Err(crate::Error::InvalidRecord);
        }

        let record_data = data[HEADER_SIZE..HEADER_SIZE + len].to_vec();
        let computed_crc = crc32fast::hash(&record_data);

        if stored_crc != computed_crc {
            return Err(crate::Error::ChecksumMismatch);
        }

        Ok(Self {
            data: record_data,
            record_type,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_record_encode_decode() {
        let record = LogRecord::new(vec![1, 2, 3, 4, 5], RecordType::Full);
        let encoded = record.encode();
        let decoded = LogRecord::decode(&encoded).unwrap();

        assert_eq!(decoded.data, vec![1, 2, 3, 4, 5]);
        assert_eq!(decoded.record_type, RecordType::Full);
    }

    #[test]
    fn test_record_types() {
        for (byte, expected) in [(0, RecordType::Full), (1, RecordType::First), (2, RecordType::Middle), (3, RecordType::Last)] {
            assert_eq!(RecordType::from_byte(byte), Some(expected));
        }
        assert_eq!(RecordType::from_byte(99), None);
    }

    #[test]
    fn test_checksum_mismatch() {
        let record = LogRecord::new(vec![1, 2, 3], RecordType::Full);
        let mut encoded = record.encode();
        // Corrupt the data
        if let Some(last) = encoded.last_mut() {
            *last ^= 0xFF;
        }
        assert!(LogRecord::decode(&encoded).is_err());
    }
}
