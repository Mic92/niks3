use anyhow::{Context, Result};
use clap::{Parser, Subcommand};
use niks3::nar;
use niks3::nix_store::{get_path_info_recursive, get_store_path_hash};
use niks3::upload::UploadClient;
use std::path::{Path, PathBuf};
use tempfile::tempdir;
use tokio::fs;
use tracing::{debug, info};
use tracing_subscriber::EnvFilter;
use url::Url;

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
        /// Niks3 server URL
        #[arg(long, env = "NIKS3_SERVER_URL")]
        server_url: String,

        /// Store paths to push to binary cache
        #[arg(required = true)]
        paths: Vec<PathBuf>,

        /// Authentication token for the server
        #[arg(long, env = "NIKS3_AUTH_TOKEN")]
        auth_token: String,
    },
}

#[tokio::main]
async fn main() -> Result<()> {
    // Initialize tracing
    tracing_subscriber::fmt()
        .with_env_filter(
            EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info")),
        )
        .init();

    let cli = Cli::parse();

    match cli.command {
        Commands::Push {
            server_url,
            paths,
            auth_token,
        } => {
            let base_url = Url::parse(&server_url).context("Invalid server URL")?;

            let client = UploadClient::new(base_url, auth_token)?;

            // Get path info for all paths and their closures
            let path_infos = get_path_info_recursive(&paths)?;
            info!("Found {} paths in closure", path_infos.len());

            // Create a temporary directory for NAR files
            let temp_dir = tempdir()?;

            // Process each path in the closure
            for (store_path, path_info) in &path_infos {
                let hash = get_store_path_hash(store_path)?;
                info!("Uploading {}", store_path);

                // Generate NAR file
                let nar_filename = format!("{}.nar", hash);
                let nar_path = temp_dir.path().join(&nar_filename);

                debug!("Creating NAR at {}", nar_path.display());
                let mut nar_file = fs::File::create(&nar_path).await?;
                nar::dump_path(&mut nar_file, Path::new(store_path)).await?;
                nar_file.sync_all().await?;

                // Create narinfo content
                let narinfo_content = create_narinfo(path_info, &nar_filename).await?;

                // Extract references as store path hashes (not full paths)
                let references: Vec<String> = path_info
                    .references
                    .iter()
                    .map(|r| get_store_path_hash(r))
                    .collect::<Result<Vec<_>>>()?;

                // Upload this closure with references
                client
                    .upload_closure(
                        &hash,
                        narinfo_content.into_bytes(),
                        vec![(format!("nar/{}", nar_filename), nar_path)],
                        references,
                    )
                    .await
                    .context("Failed to upload closure")?;
            }

            info!("Upload completed successfully");
        }
    }

    Ok(())
}

async fn create_narinfo(
    path_info: &niks3::nix_store::NixPathInfo,
    nar_filename: &str,
) -> Result<String> {
    use std::fmt::Write;

    let mut narinfo = String::new();

    // StorePath
    writeln!(&mut narinfo, "StorePath: {}", path_info.path)?;

    // URL to the NAR file
    writeln!(&mut narinfo, "URL: nar/{}", nar_filename)?;

    // Compression (none for now)
    writeln!(&mut narinfo, "Compression: none")?;

    // NAR hash and size
    writeln!(&mut narinfo, "NarHash: {}", path_info.nar_hash)?;
    writeln!(&mut narinfo, "NarSize: {}", path_info.nar_size)?;

    // For uncompressed files, FileHash and FileSize equal NarHash and NarSize
    // TODO: once we support compression, this will need to change
    writeln!(&mut narinfo, "FileHash: {}", path_info.nar_hash)?;
    writeln!(&mut narinfo, "FileSize: {}", path_info.nar_size)?;

    // References
    write!(&mut narinfo, "References:")?;
    for reference in &path_info.references {
        // References should be full store paths, not just hashes
        // Remove the /nix/store/ prefix
        let store_prefix = "/nix/store/";
        let ref_path = reference.strip_prefix(store_prefix).unwrap_or(reference);
        write!(&mut narinfo, " {}", ref_path)?;
    }
    writeln!(&mut narinfo)?;

    // Deriver (optional)
    if let Some(deriver) = &path_info.deriver {
        // Remove the /nix/store/ prefix from deriver too
        let store_prefix = "/nix/store/";
        let deriver_path = deriver.strip_prefix(store_prefix).unwrap_or(deriver);
        writeln!(&mut narinfo, "Deriver: {}", deriver_path)?;
    }

    // Signatures (optional)
    if let Some(signatures) = &path_info.signatures {
        for sig in signatures {
            writeln!(&mut narinfo, "Sig: {}", sig)?;
        }
    }

    // CA (content-addressed, optional)
    if let Some(ca) = &path_info.ca {
        writeln!(&mut narinfo, "CA: {}", ca)?;
    }

    Ok(narinfo)
}
