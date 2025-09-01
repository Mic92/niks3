use anyhow::{Context, Result};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::process::Command;
use tracing::info;

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct NixPathInfo {
    #[serde(skip)]
    pub path: String,
    pub nar_hash: String,
    pub nar_size: u64,
    pub references: Vec<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub deriver: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub signatures: Option<Vec<String>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub ca: Option<String>,
}

/// Get path info for a store path and all its dependencies
pub fn get_path_info_recursive(store_paths: &[PathBuf]) -> Result<HashMap<String, NixPathInfo>> {
    info!("Getting path info for {} paths", store_paths.len());

    let mut args = vec!["path-info", "--recursive", "--json"];
    for path in store_paths {
        args.push(path.to_str().unwrap());
    }

    let output = Command::new("nix")
        .arg("--extra-experimental-features")
        .arg("nix-command")
        .args(&args)
        .output()
        .context("Failed to run nix path-info")?;

    if !output.status.success() {
        anyhow::bail!(
            "nix path-info failed: {}",
            String::from_utf8_lossy(&output.stderr)
        );
    }

    let mut path_infos: HashMap<String, NixPathInfo> =
        serde_json::from_slice(&output.stdout).context("Failed to parse nix path-info output")?;

    // Populate the path field from the map keys
    for (path, info) in path_infos.iter_mut() {
        info.path = path.clone();
    }

    info!("Retrieved info for {} paths", path_infos.len());
    Ok(path_infos)
}

/// Get the hash part of a store path (e.g., "abc123..." from "/nix/store/abc123...-name")
pub fn get_store_path_hash(store_path: &str) -> Result<String> {
    let path = Path::new(store_path);
    let file_name = path
        .file_name()
        .and_then(|n| n.to_str())
        .context("Invalid store path")?;

    // Nix store paths have format: <hash>-<name>
    let hash = file_name
        .split('-')
        .next()
        .context("Invalid store path format")?;

    Ok(hash.to_string())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Write;
    use tempfile::NamedTempFile;

    #[test]
    fn test_store_path_hash() {
        let hash = get_store_path_hash("/nix/store/abc123def456-hello-world-1.0").unwrap();
        assert_eq!(hash, "abc123def456");
    }

    #[test]
    fn test_path_info_with_real_path() {
        // Create a temporary file and add it to the Nix store
        let mut temp_file = NamedTempFile::new().expect("Failed to create temp file");
        write!(temp_file, "test content for niks3").expect("Failed to write to temp file");

        // Add the file to the Nix store
        let output = Command::new("nix-store")
            .args(["--add", temp_file.path().to_str().unwrap()])
            .output()
            .expect("Failed to run nix-store --add");

        if output.status.success() {
            let store_path = String::from_utf8(output.stdout)
                .expect("Invalid UTF-8")
                .trim()
                .to_string();

            let store_path_buf = PathBuf::from(&store_path);

            // Get path info
            let result = get_path_info_recursive(&[store_path_buf.clone()]);
            if let Err(e) = &result {
                eprintln!("get_path_info_recursive failed: {:?}", e);
            }
            assert!(result.is_ok(), "Should be able to get path info");

            let infos = result.unwrap();
            assert_eq!(infos.len(), 1, "Should have exactly one path info");

            let info = infos.get(&store_path).expect("Path info should exist");
            assert_eq!(info.path, store_path);
            assert!(info.nar_size > 0);
            assert!(
                info.references.is_empty(),
                "Simple file should have no references"
            );
        } else {
            eprintln!(
                "nix-store --add failed: {}",
                String::from_utf8_lossy(&output.stderr)
            );
        }
    }
}
