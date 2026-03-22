//! Main database struct.

use std::path::Path;

use crate::{Result, Options, Key, Value};

/// Database instance.
pub struct DB {
    // Placeholder
    _options: Options,
}

impl DB {
    /// Open a database.
    pub fn open(_path: &Path, options: Options) -> Result<Self> {
        Ok(Self { _options: options })
    }

    /// Put a key-value pair.
    pub fn put(&self, _key: &Key, _value: &Value) -> Result<()> {
        // Placeholder
        Ok(())
    }

    /// Get a value by key.
    pub fn get(&self, _key: &Key) -> Result<Option<Value>> {
        // Placeholder
        Ok(None)
    }

    /// Delete a key.
    pub fn delete(&self, _key: &Key) -> Result<()> {
        // Placeholder
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    #[test]
    fn test_open_db() {
        let tmp = TempDir::new().unwrap();
        let db = DB::open(tmp.path(), Options::default()).unwrap();
        // Just verify it opens
        drop(db);
    }
}
