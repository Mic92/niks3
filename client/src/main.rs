use anyhow::{Context, Result};
use clap::{Parser, Subcommand};
use futures::stream::{self, StreamExt};
use niks3::nix_store::{get_path_info_recursive, get_store_path_hash, NixPathInfo};
use niks3::upload::{ObjectWithRefs, PendingObject, UploadClient, UploadTempDir};
use std::collections::HashMap;
use std::path::{Path, PathBuf};
use tracing::{debug, info};
use tracing_subscriber::EnvFilter;
use url::Url;

struct PreparedClosures {
    closures: Vec<(String, Vec<ObjectWithRefs>)>,
    path_info_by_hash: HashMap<String, (String, NixPathInfo)>,
}

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
        } => push_command(server_url, paths, auth_token, max_concurrent_uploads).await?,
    }

    Ok(())
}

async fn push_command(
    server_url: String,
    paths: Vec<PathBuf>,
    auth_token: String,
    max_concurrent_uploads: usize,
) -> Result<()> {
    let base_url = Url::parse(&server_url).context("Invalid server URL")?;
    let client = UploadClient::new(base_url, auth_token)?;

    // Get path info for all paths and their closures
    let path_infos = get_path_info_recursive(&paths)?;
    info!("Found {} paths in closure", path_infos.len());

    // Prepare closures and collect path info
    let prepared = prepare_closures(&path_infos)?;

    // Create pending closures and collect what needs uploading
    let (pending_objects, pending_ids) =
        create_pending_closures(&client, prepared.closures).await?;

    info!(
        "Need to upload {} objects out of {} total",
        pending_objects.len(),
        path_infos.len() * 2 // Each path has a narinfo and a nar file
    );

    // Upload all pending objects (NARs first, then narinfos)
    upload_pending_objects(
        &client,
        pending_objects,
        prepared.path_info_by_hash,
        max_concurrent_uploads,
    )
    .await?;

    // Complete all pending closures
    complete_closures(&client, pending_ids).await?;

    info!("Upload completed successfully");
    Ok(())
}

fn prepare_closures(path_infos: &HashMap<String, NixPathInfo>) -> Result<PreparedClosures> {
    let mut closures = Vec::new();
    let mut path_info_by_hash = HashMap::new();

    for (store_path, path_info) in path_infos {
        let hash = get_store_path_hash(store_path)?;
        path_info_by_hash.insert(hash.clone(), (store_path.clone(), path_info.clone()));

        // Extract references as store path hashes
        let references: Vec<String> = path_info
            .references
            .iter()
            .map(|r| get_store_path_hash(r))
            .collect::<Result<Vec<_>>>()?;

        // Add NAR file object (with .zst extension for compressed)
        let nar_filename = format!("{}.nar.zst", hash);
        let nar_key = format!("nar/{}", nar_filename);

        // Create narinfo object that references both dependencies and its own NAR file
        let mut narinfo_refs = references;
        narinfo_refs.push(nar_key.clone());

        let narinfo_key = format!("{}.narinfo", hash);

        // Prepare objects for this closure
        let mut objects = vec![ObjectWithRefs {
            key: narinfo_key.clone(),
            refs: narinfo_refs,
        }];

        objects.push(ObjectWithRefs {
            key: nar_key,
            refs: vec![],
        });

        closures.push((narinfo_key, objects));
    }

    Ok(PreparedClosures {
        closures,
        path_info_by_hash,
    })
}

async fn create_pending_closures(
    client: &UploadClient,
    all_closures: Vec<(String, Vec<ObjectWithRefs>)>,
) -> Result<(HashMap<String, PendingObject>, Vec<String>)> {
    debug!("Checking which objects need uploading...");
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

    Ok((pending_objects, pending_ids))
}

async fn upload_pending_objects(
    client: &UploadClient,
    pending_objects: HashMap<String, PendingObject>,
    path_info_by_hash: HashMap<String, (String, NixPathInfo)>,
    max_concurrent_uploads: usize,
) -> Result<()> {
    // Separate NAR and narinfo uploads
    let mut nar_uploads = Vec::new();
    let mut narinfo_uploads = Vec::new();

    for (object_key, pending_object) in pending_objects {
        if object_key.ends_with(".narinfo") {
            narinfo_uploads.push((object_key, pending_object));
        } else if object_key.starts_with("nar/") {
            nar_uploads.push((object_key, pending_object));
        }
    }

    // First, upload all NAR files and collect their compressed sizes and hashes
    info!("Uploading {} NAR files...", nar_uploads.len());
    let compressed_info = upload_nars(
        client,
        nar_uploads,
        &path_info_by_hash,
        max_concurrent_uploads,
    )
    .await?;

    // Now upload narinfo files with correct compressed sizes and hashes
    info!("Uploading {} narinfo files...", narinfo_uploads.len());
    upload_narinfos(
        client,
        narinfo_uploads,
        &path_info_by_hash,
        &compressed_info,
        max_concurrent_uploads,
    )
    .await?;

    Ok(())
}

async fn upload_nars(
    client: &UploadClient,
    nar_uploads: Vec<(String, PendingObject)>,
    path_info_by_hash: &HashMap<String, (String, NixPathInfo)>,
    max_concurrent_uploads: usize,
) -> Result<HashMap<String, (usize, String)>> {
    let mut compressed_info = HashMap::new();

    // Create upload temp directory for all compressions
    let temp_dir = UploadTempDir::new().context("Failed to create upload temp directory")?;

    // Create a vector to store all compression tasks with owned data
    let mut compression_data = Vec::new();

    // First, prepare all data for compression
    for (object_key, pending_object) in nar_uploads {
        if let Some(nar_name) = object_key.strip_prefix("nar/") {
            if let Some(hash) = nar_name.strip_suffix(".nar.zst") {
                if let Some((store_path, _path_info)) = path_info_by_hash.get(hash) {
                    debug!("Preparing compression for {}", store_path);
                    compression_data.push((
                        object_key.clone(),
                        pending_object,
                        store_path.clone(),
                        hash.to_string(),
                    ));
                }
            }
        }
    }

    // Execute compressions with concurrency limit
    let compression_futures: Vec<_> = compression_data
        .into_iter()
        .map(|(object_key, pending_object, store_path, hash)| {
            let temp_dir_ref = &temp_dir;
            async move {
                debug!("Compressing NAR for {}", store_path);
                let compressed_file = UploadClient::compress_nar_to_file(
                    temp_dir_ref,
                    Path::new(&store_path),
                    &object_key,
                )
                .await
                .with_context(|| format!("Failed to compress NAR for {}", object_key))?;

                Ok::<_, anyhow::Error>((compressed_file, object_key, pending_object, hash))
            }
        })
        .collect();

    info!("Compressing {} NAR files", compression_futures.len());
    let compression_stream =
        stream::iter(compression_futures).buffer_unordered(max_concurrent_uploads);
    let compression_results: Vec<_> = compression_stream.collect().await;

    // Then upload all compressed files
    let upload_tasks: Vec<_> = compression_results
        .into_iter()
        .map(|result| {
            let client = client.clone();
            async move {
                let (compressed_file, object_key, pending_object, hash) = result?;

                debug!("Uploading compressed file for {}", object_key);
                client
                    .upload_compressed_file(
                        &pending_object.presigned_url,
                        &compressed_file,
                        &object_key,
                    )
                    .await
                    .with_context(|| {
                        format!("Failed to upload compressed file for {}", object_key)
                    })?;

                let size = compressed_file.size;
                let file_hash = compressed_file.hash.clone();
                Ok::<_, anyhow::Error>((hash, (size, file_hash)))
            }
        })
        .collect();

    // Execute uploads with concurrency limit
    info!(
        "Uploading {} compressed files with max {} concurrent uploads",
        upload_tasks.len(),
        max_concurrent_uploads
    );
    let upload_stream = stream::iter(upload_tasks).buffer_unordered(max_concurrent_uploads);
    let upload_results: Vec<Result<(String, (usize, String))>> = upload_stream.collect().await;

    // Collect compressed sizes and hashes
    for result in upload_results {
        let (hash, info) = result?;
        if !hash.is_empty() {
            compressed_info.insert(hash, info);
        }
    }

    Ok(compressed_info)
}

async fn upload_narinfos(
    client: &UploadClient,
    narinfo_uploads: Vec<(String, PendingObject)>,
    path_info_by_hash: &HashMap<String, (String, NixPathInfo)>,
    compressed_info: &HashMap<String, (usize, String)>,
    max_concurrent_uploads: usize,
) -> Result<()> {
    let narinfo_tasks: Vec<_> = narinfo_uploads
        .into_iter()
        .map(|(object_key, pending_object)| {
            let client = client.clone();
            let path_info_by_hash = path_info_by_hash.clone();
            let compressed_info = compressed_info.clone();

            async move {
                if let Some(hash) = object_key.strip_suffix(".narinfo") {
                    if let Some((store_path, path_info)) = path_info_by_hash.get(hash) {
                        debug!("Uploading narinfo for {}", store_path);

                        // Get compressed size and hash for this NAR
                        let (compressed_size, file_hash) = compressed_info
                            .get(hash)
                            .cloned()
                            .unwrap_or((0, String::new()));

                        let narinfo_content = create_narinfo(
                            path_info,
                            &format!("{}.nar.zst", hash),
                            compressed_size,
                            &file_hash,
                        )
                        .await?;

                        client
                            .upload_bytes_to_presigned_url(
                                &pending_object.presigned_url,
                                narinfo_content.into_bytes(),
                                &object_key,
                            )
                            .await?;
                    }
                }
                Ok::<(), anyhow::Error>(())
            }
        })
        .collect();

    // Execute narinfo uploads with concurrency limit
    let narinfo_stream = stream::iter(narinfo_tasks).buffer_unordered(max_concurrent_uploads);
    let narinfo_results: Vec<Result<()>> = narinfo_stream.collect().await;

    // Check for any upload failures
    for result in narinfo_results {
        result?;
    }

    Ok(())
}

async fn complete_closures(client: &UploadClient, pending_ids: Vec<String>) -> Result<()> {
    debug!("Completing pending closures...");
    for pending_id in pending_ids {
        client.complete_pending_closure(&pending_id).await?;
    }
    Ok(())
}

async fn create_narinfo(
    path_info: &NixPathInfo,
    nar_filename: &str,
    compressed_size: usize,
    file_hash: &str,
) -> Result<String> {
    use std::fmt::Write;

    let mut narinfo = String::new();

    // StorePath
    writeln!(&mut narinfo, "StorePath: {}", path_info.path)?;

    // URL to the NAR file
    writeln!(&mut narinfo, "URL: nar/{}", nar_filename)?;

    // Compression
    writeln!(&mut narinfo, "Compression: zstd")?;

    // NAR hash and size (uncompressed)
    writeln!(&mut narinfo, "NarHash: {}", path_info.nar_hash)?;
    writeln!(&mut narinfo, "NarSize: {}", path_info.nar_size)?;

    // FileHash and FileSize for compressed file
    writeln!(&mut narinfo, "FileHash: {}", file_hash)?;
    writeln!(&mut narinfo, "FileSize: {}", compressed_size)?;

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
