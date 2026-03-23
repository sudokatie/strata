//! Database benchmarks.

use std::time::Instant;
use tempfile::TempDir;
use strata::{DB, Options, Key, Value};

const NUM_ENTRIES: usize = 10_000;
const VALUE_SIZE: usize = 100;

fn main() {
    println!("Strata Benchmarks");
    println!("=================\n");

    bench_sequential_write();
    bench_random_write();
    bench_sequential_read();
    bench_random_read();
}

fn bench_sequential_write() {
    let tmp = TempDir::new().unwrap();
    let db = DB::open(tmp.path(), Options::default()).unwrap();

    let value = "x".repeat(VALUE_SIZE);
    
    let start = Instant::now();
    for i in 0..NUM_ENTRIES {
        let key = format!("key{:08}", i);
        db.put(&Key::from(key.as_str()), &Value::from(value.as_str())).unwrap();
    }
    db.flush().unwrap();
    let elapsed = start.elapsed();

    let ops_per_sec = NUM_ENTRIES as f64 / elapsed.as_secs_f64();
    println!("Sequential Write:");
    println!("  {} ops in {:?}", NUM_ENTRIES, elapsed);
    println!("  {:.0} ops/sec\n", ops_per_sec);
}

fn bench_random_write() {
    let tmp = TempDir::new().unwrap();
    let db = DB::open(tmp.path(), Options::default()).unwrap();

    let value = "x".repeat(VALUE_SIZE);
    let mut keys: Vec<usize> = (0..NUM_ENTRIES).collect();
    
    // Shuffle
    use std::collections::hash_map::DefaultHasher;
    use std::hash::{Hash, Hasher};
    for i in 0..keys.len() {
        let mut hasher = DefaultHasher::new();
        i.hash(&mut hasher);
        let j = hasher.finish() as usize % keys.len();
        keys.swap(i, j);
    }

    let start = Instant::now();
    for i in &keys {
        let key = format!("key{:08}", i);
        db.put(&Key::from(key.as_str()), &Value::from(value.as_str())).unwrap();
    }
    db.flush().unwrap();
    let elapsed = start.elapsed();

    let ops_per_sec = NUM_ENTRIES as f64 / elapsed.as_secs_f64();
    println!("Random Write:");
    println!("  {} ops in {:?}", NUM_ENTRIES, elapsed);
    println!("  {:.0} ops/sec\n", ops_per_sec);
}

fn bench_sequential_read() {
    let tmp = TempDir::new().unwrap();
    let db = DB::open(tmp.path(), Options::default()).unwrap();

    // Populate
    let value = "x".repeat(VALUE_SIZE);
    for i in 0..NUM_ENTRIES {
        let key = format!("key{:08}", i);
        db.put(&Key::from(key.as_str()), &Value::from(value.as_str())).unwrap();
    }
    db.flush().unwrap();

    // Benchmark reads
    let start = Instant::now();
    for i in 0..NUM_ENTRIES {
        let key = format!("key{:08}", i);
        let _ = db.get(&Key::from(key.as_str())).unwrap();
    }
    let elapsed = start.elapsed();

    let ops_per_sec = NUM_ENTRIES as f64 / elapsed.as_secs_f64();
    println!("Sequential Read:");
    println!("  {} ops in {:?}", NUM_ENTRIES, elapsed);
    println!("  {:.0} ops/sec\n", ops_per_sec);
}

fn bench_random_read() {
    let tmp = TempDir::new().unwrap();
    let db = DB::open(tmp.path(), Options::default()).unwrap();

    // Populate
    let value = "x".repeat(VALUE_SIZE);
    for i in 0..NUM_ENTRIES {
        let key = format!("key{:08}", i);
        db.put(&Key::from(key.as_str()), &Value::from(value.as_str())).unwrap();
    }
    db.flush().unwrap();

    // Generate random order
    let mut keys: Vec<usize> = (0..NUM_ENTRIES).collect();
    use std::collections::hash_map::DefaultHasher;
    use std::hash::{Hash, Hasher};
    for i in 0..keys.len() {
        let mut hasher = DefaultHasher::new();
        (i + 42).hash(&mut hasher);
        let j = hasher.finish() as usize % keys.len();
        keys.swap(i, j);
    }

    // Benchmark reads
    let start = Instant::now();
    for i in &keys {
        let key = format!("key{:08}", i);
        let _ = db.get(&Key::from(key.as_str())).unwrap();
    }
    let elapsed = start.elapsed();

    let ops_per_sec = NUM_ENTRIES as f64 / elapsed.as_secs_f64();
    println!("Random Read:");
    println!("  {} ops in {:?}", NUM_ENTRIES, elapsed);
    println!("  {:.0} ops/sec\n", ops_per_sec);
}
