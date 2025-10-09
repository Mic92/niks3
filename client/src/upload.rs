use crate::nar;
use anyhow::{Context, Result};
use async_compression::tokio::bufread::ZstdEncoder;
use base64::Engine;
use bytes::Bytes;
use futures::stream::Stream;
use futures::TryStreamExt;
use reqwest::{header, Body, Client};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use tempfile::TempDir;
use tokio::fs::File;
use tokio::io::{AsyncRead, BufReader};
use tokio_util::io::ReaderStream;
use tokio_util::io::StreamReader;
use tracing::{debug, info};
use url::Url;

/// Return type for create_hashing_stream function  
type HashingStreamResult = (
    std::pin::Pin<Box<dyn Stream<Item = Result<Bytes>> + Send>>,
    Arc<AtomicU64>,
    Arc<Mutex<Sha256>>,
);

/// Upload temp directory for staging compressed files
#[derive(Debug)]
pub struct UploadTempDir {
    temp_dir: TempDir,
    file_counter: AtomicU64,
}

impl UploadTempDir {
    /// Create a new upload temp directory
    pub fn new() -> Result<Self> {
        let temp_dir = TempDir::new().context("Failed to create temp directory")?;
        Ok(Self {
            temp_dir,
            file_counter: AtomicU64::new(0),
        })
    }

    /// Get the path to the temp directory
    pub fn path(&self) -> &Path {
        self.temp_dir.path()
    }

    /// Allocate a new file path within this directory without creating the file
    pub fn allocate_temp_path(&self) -> PathBuf {
        let file_id = self.file_counter.fetch_add(1, Ordering::SeqCst);
        let filename = format!("{}", file_id);
        self.temp_dir.path().join(filename)
    }
}

/// Compressed file information
#[derive(Debug)]
pub struct CompressedFile {
    pub path: PathBuf,
    pub size: u64,
    pub hash: String,
}

impl Drop for CompressedFile {
    fn drop(&mut self) {
        // Clean up temporary file when dropped
        if self.path.exists() {
            let _ = std::fs::remove_file(&self.path);
        }
    }
}

#[derive(Debug, Clone)]
pub struct UploadClient {
    client: Client,
    base_url: Url,
    auth_token: String,
}

#[derive(Debug, Serialize)]
pub struct ObjectWithRefs {
    pub key: String,
    pub refs: Vec<String>,
}

#[derive(Debug, Serialize)]
pub struct CreatePendingClosureRequest {
    pub closure: String,
    pub objects: Vec<ObjectWithRefs>,
}

#[derive(Debug, Deserialize)]
pub struct CreatePendingClosureResponse {
    pub id: String,
    pub started_at: String,
    pub pending_objects: std::collections::HashMap<String, PendingObject>,
}

#[derive(Debug, Deserialize)]
pub struct PendingObject {
    pub presigned_url: String,
}

impl UploadClient {
    /// Create a new upload client
    pub fn new(base_url: Url, auth_token: String) -> Result<Self> {
        let client = Client::builder()
            .timeout(std::time::Duration::from_secs(300))
            .build()
            .context("Failed to build HTTP client")?;

        Ok(Self {
            client,
            base_url,
            auth_token,
        })
    }

    /// Create a pending closure and get upload URLs
    pub async fn create_pending_closure(
        &self,
        closure: String,
        objects: Vec<ObjectWithRefs>,
    ) -> Result<CreatePendingClosureResponse> {
        let url = self
            .base_url
            .join("api/pending_closures")
            .context("Failed to build URL")?;

        let request = CreatePendingClosureRequest { closure, objects };

        debug!("Creating pending closure for {}", request.closure);

        let response = self
            .client
            .post(url)
            .header("Authorization", format!("Bearer {}", self.auth_token))
            .json(&request)
            .send()
            .await
            .context("Failed to create pending closure")?;

        if !response.status().is_success() {
            let status = response.status();
            let body = response
                .text()
                .await
                .unwrap_or_else(|_| "Failed to read response body".to_string());
            anyhow::bail!("Failed to create pending closure: {} - {}", status, body);
        }

        let result = response
            .json::<CreatePendingClosureResponse>()
            .await
            .context("Failed to parse response")?;

        info!(
            "Created pending closure {} with {} objects",
            result.id,
            result.pending_objects.len()
        );

        Ok(result)
    }

    /// Upload content from bytes to a presigned URL
    pub async fn upload_bytes_to_presigned_url(
        &self,
        upload_url: &str,
        content: Vec<u8>,
        object_key: &str,
    ) -> Result<()> {
        info!(
            "Uploading {} ({} bytes) to presigned URL",
            object_key,
            content.len()
        );

        let response = self
            .client
            .put(upload_url)
            .header(reqwest::header::CONTENT_LENGTH, content.len().to_string())
            .body(content)
            .send()
            .await
            .context("Failed to upload content")?;

        if !response.status().is_success() {
            let status = response.status();
            let body = response
                .text()
                .await
                .unwrap_or_else(|_| "Failed to read response body".to_string());
            anyhow::bail!("Failed to upload {}: {} - {}", object_key, status, body);
        }

        debug!("Successfully uploaded {}", object_key);
        Ok(())
    }

    /// Compress NAR data to a temporary file and return file info
    /// This separates compression from upload for S3 size requirements
    pub async fn compress_nar_to_file(
        temp_dir: &UploadTempDir,
        store_path: &Path,
        object_key: &str,
    ) -> Result<CompressedFile> {
        info!("Compressing NAR data for {} to temporary file", object_key);

        // Allocate temp file path
        let temp_path = temp_dir.allocate_temp_path();

        // Create pipe to connect NAR serialization to compression
        let (nar_reader, nar_writer) = tokio::io::duplex(65536);

        // Spawn NAR serialization task
        let store_path = store_path.to_path_buf();
        let nar_task = tokio::spawn(async move {
            let mut writer = nar_writer;
            nar::dump_path(&mut writer, &store_path).await
        });

        // Create compressed stream using async-compression with tokio support
        let buf_reader = BufReader::new(nar_reader);
        let compressed_reader = ZstdEncoder::new(buf_reader);

        // Create a hashing stream wrapper
        let (stream, size_tracker, hasher) = create_hashing_stream(compressed_reader);

        // Convert anyhow::Error to std::io::Error for StreamReader compatibility
        let stream = stream.map_err(std::io::Error::other);

        // Convert stream to AsyncRead and copy to file
        let mut stream_reader = StreamReader::new(stream);
        let mut temp_file = File::create(&temp_path)
            .await
            .with_context(|| format!("Failed to create temp file at {}", temp_path.display()))?;

        tokio::io::copy(&mut stream_reader, &mut temp_file)
            .await
            .context("Failed to copy compressed stream to temp file")?;

        // Wait for NAR task to complete
        nar_task.await??;

        // Get final size and hash
        let total_size = size_tracker.load(Ordering::SeqCst);
        let hash = hasher.lock().unwrap().clone().finalize();
        let hash_str = format!(
            "sha256:{}",
            base64::engine::general_purpose::STANDARD.encode(hash)
        );

        debug!(
            "Compressed {} to {} (size: {} bytes, hash: {})",
            object_key,
            temp_path.display(),
            total_size,
            hash_str
        );

        Ok(CompressedFile {
            path: temp_path,
            size: total_size,
            hash: hash_str,
        })
    }

    /// Upload a compressed file to a presigned URL
    /// This provides the file size upfront as required by S3
    pub async fn upload_compressed_file(
        &self,
        upload_url: &str,
        compressed_file: &CompressedFile,
        object_key: &str,
    ) -> Result<()> {
        debug!(
            "Uploading compressed file {} ({} bytes) to presigned URL",
            object_key, compressed_file.size
        );

        let file = File::open(&compressed_file.path).await.with_context(|| {
            format!(
                "Failed to open compressed file at {}",
                compressed_file.path.display()
            )
        })?;

        // Create a stream from the file
        let stream = ReaderStream::new(file);
        let body = Body::wrap_stream(stream);

        let response = self
            .client
            .put(upload_url)
            .header(header::CONTENT_LENGTH, compressed_file.size.to_string())
            .body(body)
            .send()
            .await
            .context("Failed to upload compressed file")?;

        if !response.status().is_success() {
            let status = response.status();
            let body = response
                .text()
                .await
                .unwrap_or_else(|_| "Failed to read response body".to_string());
            anyhow::bail!("Failed to upload {}: {} - {}", object_key, status, body);
        }

        debug!("Successfully uploaded compressed file {}", object_key);
        Ok(())
    }

    /// Complete a pending closure
    pub async fn complete_pending_closure(&self, closure_id: &str) -> Result<()> {
        let url = self
            .base_url
            .join(&format!("api/pending_closures/{}/complete", closure_id))
            .context("Failed to build URL")?;

        info!("Completing pending closure {}", closure_id);

        let response = self
            .client
            .post(url)
            .header("Authorization", format!("Bearer {}", self.auth_token))
            .send()
            .await
            .context("Failed to complete pending closure")?;

        if !response.status().is_success() {
            let status = response.status();
            let body = response
                .text()
                .await
                .unwrap_or_else(|_| "Failed to read response body".to_string());
            anyhow::bail!("Failed to complete pending closure: {} - {}", status, body);
        }

        info!("Successfully completed pending closure {}", closure_id);
        Ok(())
    }
}

/// Creates a hashing stream that computes size and sha256 hash of data
fn create_hashing_stream(reader: impl AsyncRead + Send + 'static) -> HashingStreamResult {
    let size_tracker = Arc::new(AtomicU64::new(0));
    let hasher = Arc::new(Mutex::new(Sha256::new()));

    let size_tracker_clone = size_tracker.clone();
    let hasher_clone = hasher.clone();

    let stream = ReaderStream::new(reader)
        .map_err(|e| anyhow::anyhow!("IO error: {}", e))
        .map_ok(move |chunk| {
            // Update hash and size
            {
                let mut hasher = hasher_clone.lock().unwrap();
                hasher.update(&chunk);
            }

            let len = chunk.len() as u64;
            size_tracker_clone.fetch_add(len, Ordering::SeqCst);

            chunk
        });

    (Box::pin(stream), size_tracker, hasher)
}
