use anyhow::{Context, Result};
use clap::{Parser, Subcommand};
use futures::stream::{self, StreamExt};
use niks3::nar;
use niks3::nix_store::{get_path_info_recursive, get_store_path_hash, NixPathInfo};
use niks3::upload::{ObjectWithRefs, UploadClient};
use std::collections::HashMap;
use std::path::{Path, PathBuf};
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

        /// Maximum number of concurrent uploads
        #[arg(long, default_value = "30")]
        max_concurrent_uploads: usize,
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
            max_concurrent_uploads,
        } => {
            let base_url = Url::parse(&server_url).context("Invalid server URL")?;

            let client = UploadClient::new(base_url, auth_token)?;

            // Get path info for all paths and their closures
            let path_infos = get_path_info_recursive(&paths)?;
            info!("Found {} paths in closure", path_infos.len());

            // First, collect all objects we want to upload
            let mut all_closures = Vec::new();
            let mut path_info_by_hash = HashMap::new();

            for (store_path, path_info) in &path_infos {
                let hash = get_store_path_hash(store_path)?;
                path_info_by_hash.insert(hash.clone(), (store_path.clone(), path_info.clone()));

                // Extract references as store path hashes
                let references: Vec<String> = path_info
                    .references
                    .iter()
                    .map(|r| get_store_path_hash(r))
                    .collect::<Result<Vec<_>>>()?;

                // Prepare objects for this closure
                let mut objects = vec![ObjectWithRefs {
                    key: format!("{}.narinfo", hash),
                    refs: references,
                }];

                // Add NAR file object
                let nar_filename = format!("{}.nar", hash);
                objects.push(ObjectWithRefs {
                    key: format!("nar/{}", nar_filename),
                    refs: vec![],
                });

                all_closures.push((hash.clone(), objects));
            }

            // Create all pending closures and get back what needs uploading
            info!("Checking which objects need uploading...");
            let mut pending_objects = HashMap::new();
            let mut pending_ids = Vec::new();

            for (closure_hash, objects) in all_closures {
                let response = client
                    .create_pending_closure(closure_hash.clone(), objects)
                    .await?;

                pending_ids.push(response.id);

                // Collect pending objects
                for (key, obj) in response.pending_objects {
                    pending_objects.insert(key, obj);
                }
            }

            info!(
                "Need to upload {} objects out of {} total",
                pending_objects.len(),
                path_infos.len() * 2 // Each path has a narinfo and a nar file
            );

            // Create upload tasks for all pending objects
            let mut upload_tasks = Vec::new();

            for (object_key, pending_object) in pending_objects {
                let client = client.clone();
                let path_info_by_hash = path_info_by_hash.clone();
                
                let task = async move {
                    if let Some(hash) = object_key.strip_suffix(".narinfo") {
                        // This is a narinfo file
                        if let Some((store_path, path_info)) = path_info_by_hash.get(hash) {
                            debug!("Uploading narinfo for {}", store_path);
                            let narinfo_content =
                                create_narinfo(path_info, &format!("{}.nar", hash)).await?;
                            client
                                .upload_bytes_to_presigned_url(
                                    &pending_object.presigned_url,
                                    narinfo_content.into_bytes(),
                                    &object_key,
                                )
                                .await?;
                        }
                    } else if let Some(nar_name) = object_key.strip_prefix("nar/") {
                        // This is a NAR file
                        if let Some(hash) = nar_name.strip_suffix(".nar") {
                            if let Some((store_path, path_info)) = path_info_by_hash.get(hash) {
                                debug!("Uploading NAR for {}", store_path);

                                // Serialize NAR to memory
                                let mut nar_data = Vec::with_capacity(path_info.nar_size as usize);
                                nar::dump_path(&mut nar_data, Path::new(store_path)).await?;

                                // Upload the NAR data
                                client
                                    .upload_bytes_to_presigned_url(
                                        &pending_object.presigned_url,
                                        nar_data,
                                        &object_key,
                                    )
                                    .await?;
                            }
                        }
                    }
                    Ok::<(), anyhow::Error>(())
                };
                
                upload_tasks.push(task);
            }

            // Execute uploads with concurrency limit
            info!("Uploading with max {} concurrent uploads", max_concurrent_uploads);
            
            let upload_stream = stream::iter(upload_tasks)
                .buffer_unordered(max_concurrent_uploads);
            
            let results: Vec<Result<()>> = upload_stream.collect().await;
            
            // Check for any upload failures
            for result in results {
                result?;
            }

            // Complete all pending closures
            info!("Completing pending closures...");
            for pending_id in pending_ids {
                client.complete_pending_closure(&pending_id).await?;
            }

            info!("Upload completed successfully");
        }
    }

    Ok(())
}

async fn create_narinfo(path_info: &NixPathInfo, nar_filename: &str) -> Result<String> {
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
