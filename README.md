# Strata

An LSM-tree storage engine built from scratch in Rust.

## What This Is

Strata implements the Log-Structured Merge-tree architecture used by RocksDB, LevelDB, Cassandra, and many modern databases. It's optimized for write-heavy workloads with good read performance through compaction.

## Architecture

```
Write: Put(k,v) → WAL → MemTable → [flush] → SSTable (L0)
                              ↓
Compaction: L0 → L1 → L2 → ... → Ln
                              ↓
Read: Get(k) → MemTable → L0 → L1 → ... → Ln
       (bloom filters skip empty SSTables)
```

## Features

- Write-ahead log for durability
- Skip list MemTable for fast writes
- SSTable files with block-based storage
- Bloom filters for negative lookups
- Leveled compaction
- Snapshot isolation
- Concurrent reads

## Quick Start

```bash
# Build
cargo build

# Run tests
cargo test

# Open database REPL
cargo run -- open ./data
```

## Usage

```rust
use strata::{DB, Options, Key, Value};

// Open database
let db = DB::open("./data", Options::default())?;

// Write
db.put(&Key::from("hello"), &Value::from("world"))?;

// Read
let value = db.get(&Key::from("hello"))?;

// Delete
db.delete(&Key::from("hello"))?;
```

## Performance

Target metrics for v0.1.0:
- Write throughput: > 100K ops/sec
- Point read latency: < 100μs (MemTable hit)
- Point read latency: < 1ms (SSTable with bloom)
- Range scan: > 10K keys/sec

## Project Status

v0.1.0 - In Development

## License

MIT

---

*Built by Katie to understand storage engine internals.*
