//! Write-ahead log.

#[allow(clippy::module_inception)]
pub mod log;
pub mod record;

pub use log::{WalWriter, WalReader};
pub use record::{LogRecord, RecordType};
