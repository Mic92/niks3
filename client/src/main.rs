use anyhow::{Context, Result};
use clap::{Parser, Subcommand};
use std::fs::File;
use std::io::Read;
use std::path::{Path, PathBuf};

#[derive(Parser)]
#[command(name = "niks3")]
#[command(about = "S3-compatible Nix binary cache uploader", long_about = None)]
#[command(version)]
struct Cli {
    #[command(subcommand)]
    command: Commands,
}

#[derive(Subcommand)]
enum Commands {
    /// Upload paths to S3-compatible binary cache
    #[command(arg_required_else_help = true)]
    Push {
        /// S3 bucket name
        bucket: String,

        /// Store paths to push to binary cache
        #[arg(required = true)]
        paths: Vec<PathBuf>,

        /// AWS access key ID
        #[arg(long, env = "NIKS3_S3_ACCESS_KEY")]
        access_key: Option<String>,

        /// Path to AWS access key ID
        #[arg(long, env = "NIKS3_S3_ACCESS_KEY_PATH")]
        access_key_path: Option<String>,

        /// AWS secret access key
        #[arg(long, env = "NIKS3_S3_SECRET_KEY")]
        secret_key: Option<String>,

        /// Path to AWS secret access key. Conflicts with --secret-key option
        #[arg(long, env = "NIKS3_S3_SECRET_KEY_PATH")]
        secret_key_path: Option<String>,

        /// S3 endpoint URL (for non-AWS S3-compatible services)
        #[arg(long, env = "NIKS3_S3_ENDPOINT")]
        endpoint: Option<String>,
    },
}

fn read_key(path: &Path) -> Result<String> {
    let mut file =
        File::open(path).with_context(|| format!("Failed to open key file: {}", path.display()))?;
    let mut key = String::new();
    file.read_to_string(&mut key)
        .with_context(|| format!("Failed to read key file: {}", path.display()))?;
    Ok(key.trim().to_string())
}

fn main() {
    let cli = Cli::parse();

    match cli.command {
        Commands::Push {
            bucket,
            paths,
            access_key,
            access_key_path,
            secret_key,
            secret_key_path,
            endpoint,
        } => {}
    }
}
