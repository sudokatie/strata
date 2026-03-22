//! Strata CLI.

use clap::{Parser, Subcommand};

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
    /// Open a database
    Open {
        /// Path to database directory
        path: String,
    },
}

fn main() {
    let cli = Cli::parse();

    match cli.command {
        Commands::Open { path } => {
            println!("Opening database at: {}", path);
            // TODO: Implement REPL
        }
    }
}
