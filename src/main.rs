//! Strata CLI.

use std::io::{self, Write, BufRead};
use std::path::Path;

use clap::{Parser, Subcommand};
use strata::{DB, Options, Key, Value};

#[derive(Parser)]
#[command(name = "strata")]
#[command(about = "An LSM-tree storage engine")]
#[command(version)]
struct Cli {
    #[command(subcommand)]
    command: Commands,
}

#[derive(Subcommand)]
enum Commands {
    /// Open a database and start REPL
    Open {
        /// Path to database directory
        path: String,
    },
    /// Run commands from a file
    Batch {
        /// Path to database directory
        #[arg(short, long)]
        db: String,
        /// Path to command file
        file: String,
    },
}

fn main() {
    let cli = Cli::parse();

    match cli.command {
        Commands::Open { path } => {
            if let Err(e) = run_repl(&path) {
                eprintln!("Error: {}", e);
                std::process::exit(1);
            }
        }
        Commands::Batch { db, file } => {
            if let Err(e) = run_batch(&db, &file) {
                eprintln!("Error: {}", e);
                std::process::exit(1);
            }
        }
    }
}

fn run_repl(path: &str) -> Result<(), Box<dyn std::error::Error>> {
    println!("Opening database at: {}", path);
    let db = DB::open(Path::new(path), Options::default())?;
    println!("Database opened. Type 'help' for commands.");

    let stdin = io::stdin();
    let mut stdout = io::stdout();

    loop {
        print!("strata> ");
        stdout.flush()?;

        let mut line = String::new();
        if stdin.lock().read_line(&mut line)? == 0 {
            // EOF
            break;
        }

        let line = line.trim();
        if line.is_empty() {
            continue;
        }

        let parts: Vec<&str> = line.splitn(3, ' ').collect();
        let cmd = parts[0].to_lowercase();

        match cmd.as_str() {
            "help" | "?" => {
                print_help();
            }
            "put" => {
                if parts.len() < 3 {
                    println!("Usage: put <key> <value>");
                    continue;
                }
                let key = Key::from(parts[1]);
                let value = Value::from(parts[2]);
                match db.put(&key, &value) {
                    Ok(()) => println!("OK"),
                    Err(e) => println!("Error: {}", e),
                }
            }
            "get" => {
                if parts.len() < 2 {
                    println!("Usage: get <key>");
                    continue;
                }
                let key = Key::from(parts[1]);
                match db.get(&key) {
                    Ok(Some(value)) => {
                        match std::str::from_utf8(value.as_bytes()) {
                            Ok(s) => println!("{}", s),
                            Err(_) => println!("{:?}", value.as_bytes()),
                        }
                    }
                    Ok(None) => println!("(not found)"),
                    Err(e) => println!("Error: {}", e),
                }
            }
            "delete" | "del" => {
                if parts.len() < 2 {
                    println!("Usage: delete <key>");
                    continue;
                }
                let key = Key::from(parts[1]);
                match db.delete(&key) {
                    Ok(()) => println!("OK"),
                    Err(e) => println!("Error: {}", e),
                }
            }
            "flush" => {
                match db.flush() {
                    Ok(()) => println!("Flushed to disk"),
                    Err(e) => println!("Error: {}", e),
                }
            }
            "compact" => {
                match db.compact() {
                    Ok(()) => println!("Compaction triggered"),
                    Err(e) => println!("Error: {}", e),
                }
            }
            "stats" => {
                let stats = db.stats();
                println!("Database: {}", path);
                println!("Status: Open");
                println!();
                println!("MemTable:");
                println!("  Entries: {}", stats.memtable_entries);
                println!("  Size: {} bytes", stats.memtable_size);
                if stats.immutable_memtable {
                    println!("  Immutable: yes (pending flush)");
                }
                println!();
                println!("Levels:");
                if stats.levels.is_empty() {
                    println!("  (no SSTables yet)");
                } else {
                    for level in &stats.levels {
                        println!("  L{}: {} files, {} bytes", 
                            level.level, level.num_files, level.total_bytes);
                    }
                }
                println!();
                println!("Total SSTable files: {}", stats.total_files);
            }
            "quit" | "exit" | "q" => {
                println!("Goodbye!");
                break;
            }
            _ => {
                println!("Unknown command: {}. Type 'help' for usage.", cmd);
            }
        }
    }

    Ok(())
}

fn run_batch(db_path: &str, file_path: &str) -> Result<(), Box<dyn std::error::Error>> {
    let db = DB::open(Path::new(db_path), Options::default())?;
    let file = std::fs::File::open(file_path)?;
    let reader = io::BufReader::new(file);

    for line in reader.lines() {
        let line = line?;
        let line = line.trim();
        if line.is_empty() || line.starts_with('#') {
            continue;
        }

        let parts: Vec<&str> = line.splitn(3, ' ').collect();
        let cmd = parts[0].to_lowercase();

        match cmd.as_str() {
            "put" => {
                if parts.len() >= 3 {
                    let key = Key::from(parts[1]);
                    let value = Value::from(parts[2]);
                    db.put(&key, &value)?;
                }
            }
            "delete" | "del" => {
                if parts.len() >= 2 {
                    let key = Key::from(parts[1]);
                    db.delete(&key)?;
                }
            }
            "flush" => {
                db.flush()?;
            }
            "compact" => {
                db.compact()?;
            }
            _ => {
                eprintln!("Unknown command: {}", cmd);
            }
        }
    }

    println!("Batch complete");
    Ok(())
}

fn print_help() {
    println!("Commands:");
    println!("  put <key> <value>  - Store a key-value pair");
    println!("  get <key>          - Retrieve a value by key");
    println!("  delete <key>       - Delete a key");
    println!("  flush              - Flush memtable to disk");
    println!("  compact            - Trigger compaction");
    println!("  stats              - Show database statistics");
    println!("  help               - Show this help");
    println!("  quit               - Exit the REPL");
}
