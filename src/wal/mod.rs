//! Write-ahead log.

pub mod log;
pub mod record;

pub use log::{WalWriter, WalReader};
pub use record::{LogRecord, RecordType};
