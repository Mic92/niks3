use anyhow::{Context, Result};
use reqwest::{header, Body, Client};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::path::Path;
use tokio::fs::File;
use tokio_util::io::ReaderStream;
use tracing::{debug, info};
use url::Url;

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

    /// Upload a file to a presigned URL
    pub async fn upload_to_presigned_url(&self, upload_url: &str, file_path: &Path) -> Result<()> {
        let file = File::open(file_path).await.context("Failed to open file")?;

        let metadata = file
            .metadata()
            .await
            .context("Failed to get file metadata")?;
        let file_size = metadata.len();

        info!(
            "Uploading {} ({} bytes) to presigned URL",
            file_path.display(),
            file_size
        );

        // Create a stream from the file
        let stream = ReaderStream::new(file);
        let body = Body::wrap_stream(stream);

        let response = self
            .client
            .put(upload_url)
            .header(header::CONTENT_LENGTH, file_size.to_string())
            .body(body)
            .send()
            .await
            .context("Failed to upload file")?;

        if !response.status().is_success() {
            let status = response.status();
            let body = response
                .text()
                .await
                .unwrap_or_else(|_| "Failed to read response body".to_string());
            anyhow::bail!("Failed to upload file: {} - {}", status, body);
        }

        debug!("Successfully uploaded {}", file_path.display());
        Ok(())
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

    /// Upload multiple closures in a single batch
    /// Returns a map of object keys that need to be uploaded
    pub async fn create_batch_pending_closure(
        &self,
        closures: Vec<(String, Vec<ObjectWithRefs>)>,
    ) -> Result<HashMap<String, PendingObject>> {
        // For now, we'll handle one closure at a time but collect all pending objects
        // In the future, the server could support batching multiple closures
        let mut all_pending_objects = HashMap::new();
        
        for (closure_hash, objects) in closures {
            let pending = self
                .create_pending_closure(closure_hash, objects)
                .await?;
            
            // Collect all pending objects
            for (key, obj) in pending.pending_objects {
                all_pending_objects.insert(key, obj);
            }
            
            // TODO: Store pending closure IDs for later completion
        }
        
        Ok(all_pending_objects)
    }
}
