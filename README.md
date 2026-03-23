# strata

An LSM-tree storage engine in Rust. Built from scratch, no dependencies on existing storage engines.

## Why This Exists

Because understanding how databases work means building one. Strata implements the core concepts from LevelDB/RocksDB: write-ahead logging, memtables, sorted string tables, compaction, and MVCC.

## Features

- **Write-Ahead Log (WAL)** - Durability before acknowledging writes
- **MemTable with Skip List** - Fast in-memory writes
- **SSTable with Bloom Filters** - Efficient on-disk storage
- **Leveled Compaction** - Background merging to reclaim space
- **MVCC with Snapshots** - Point-in-time reads
- **Interactive CLI** - REPL for testing and debugging

## Quick Start

```bash
# Build
cargo build --release

# Open a database
./target/release/strata open ./mydb

# In the REPL
strata> put name Alice
OK
strata> get name
Alice
strata> delete name
OK
strata> quit
```

## Usage

### As a Library

```rust
use strata::{DB, Options, Key, Value};

// Open database
let db = DB::open("./mydb", Options::default())?;

// Write
db.put(&Key::from("hello"), &Value::from("world"))?;

// Read
match db.get(&Key::from("hello"))? {
    Some(value) => println!("{}", std::str::from_utf8(value.as_bytes())?),
    None => println!("Not found"),
}

// Delete
db.delete(&Key::from("hello"))?;

// Flush to disk
db.flush()?;
```

### CLI Commands

| Command | Description |
|---------|-------------|
| `put <key> <value>` | Store a key-value pair |
| `get <key>` | Retrieve a value |
| `delete <key>` | Delete a key |
| `flush` | Force flush to disk |
| `compact` | Trigger compaction |
| `stats` | Show database info |
| `quit` | Exit |

### Batch Mode

```bash
# Run commands from a file
./target/release/strata batch --db ./mydb commands.txt
```

Where `commands.txt` contains:
```
put key1 value1
put key2 value2
flush
```

## Architecture

```
┌─────────────────────────────────────────────┐
│                   Client                     │
└─────────────────┬───────────────────────────┘
                  │
┌─────────────────▼───────────────────────────┐
│                    DB                        │
│  ┌─────────────┐  ┌─────────────────────┐   │
│  │  MemTable   │  │  Immutable MemTable │   │
│  │ (Skip List) │  │   (being flushed)   │   │
│  └─────────────┘  └─────────────────────┘   │
│         │                    │              │
│         ▼                    ▼              │
│  ┌─────────────────────────────────────┐    │
│  │              WAL                     │    │
│  └─────────────────────────────────────┘    │
└─────────────────┬───────────────────────────┘
                  │ Flush
┌─────────────────▼───────────────────────────┐
│              SSTables                        │
│  ┌─────────────────────────────────────┐    │
│  │  Level 0 (overlapping)              │    │
│  ├─────────────────────────────────────┤    │
│  │  Level 1 (sorted, non-overlapping)  │    │
│  ├─────────────────────────────────────┤    │
│  │  Level 2 ...                        │    │
│  └─────────────────────────────────────┘    │
└─────────────────────────────────────────────┘
```

### Key Components

- **MemTable**: In-memory buffer using a skip list for O(log n) inserts
- **WAL**: Append-only log for crash recovery
- **SSTable**: Immutable sorted files with bloom filter and index
- **Compaction**: Background merge of overlapping files

## Benchmarks

```bash
cargo run --example bench --release
```

Sample output (varies by hardware):
```
Sequential Write: 50,000 ops/sec
Random Write:     40,000 ops/sec
Sequential Read:  100,000 ops/sec
Random Read:      80,000 ops/sec
```

## Testing

```bash
cargo test
```

86+ tests covering:
- Core types and encoding
- WAL write/recovery
- MemTable operations
- SSTable read/write
- Compaction
- Database CRUD
- Persistence

## License

MIT

---

*Built for learning. Use in production at your own risk.*
