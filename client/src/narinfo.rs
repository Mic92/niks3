use anyhow::Result;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::collections::BTreeSet;
use std::fmt::Write as FmtWrite;

use crate::nix_base32;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NarInfo {
    pub store_path: String,
    pub url: String,
    pub compression: String,
    pub file_hash: String,
    pub file_size: u64,
    pub nar_hash: String,
    pub nar_size: u64,
    pub references: BTreeSet<String>,
    pub deriver: Option<String>,
    pub system: Option<String>,
    pub sig: Option<String>,
    pub ca: Option<String>,
}

impl NarInfo {
    pub fn new(
        store_path: String,
        nar_data: &[u8],
        compressed_data: &[u8],
        compression: String,
    ) -> Self {
        let mut nar_hasher = Sha256::new();
        nar_hasher.update(nar_data);
        let nar_hash = nar_hasher.finalize();

        let mut file_hasher = Sha256::new();
        file_hasher.update(compressed_data);
        let file_hash = file_hasher.finalize();

        let basename = store_path.split('/').next_back().unwrap_or(&store_path);

        Self {
            store_path: store_path.clone(),
            url: format!(
                "nar/{}.nar{}",
                basename,
                if compression == "none" { "" } else { ".xz" }
            ),
            compression,
            file_hash: nix_base32::hash_to_nix_string("sha256", &file_hash),
            file_size: compressed_data.len() as u64,
            nar_hash: nix_base32::hash_to_nix_string("sha256", &nar_hash),
            nar_size: nar_data.len() as u64,
            references: BTreeSet::new(),
            deriver: None,
            system: None,
            sig: None,
            ca: None,
        }
    }

    pub fn to_string(&self) -> Result<String> {
        let mut result = String::new();

        writeln!(&mut result, "StorePath: {}", self.store_path)?;
        writeln!(&mut result, "URL: {}", self.url)?;
        writeln!(&mut result, "Compression: {}", self.compression)?;
        writeln!(&mut result, "FileHash: {}", self.file_hash)?;
        writeln!(&mut result, "FileSize: {}", self.file_size)?;
        writeln!(&mut result, "NarHash: {}", self.nar_hash)?;
        writeln!(&mut result, "NarSize: {}", self.nar_size)?;

        if !self.references.is_empty() {
            write!(&mut result, "References:")?;
            for reference in &self.references {
                write!(
                    &mut result,
                    " {}",
                    reference.split('/').next_back().unwrap_or(reference)
                )?;
            }
            writeln!(&mut result)?;
        }

        if let Some(ref deriver) = self.deriver {
            writeln!(
                &mut result,
                "Deriver: {}",
                deriver.split('/').next_back().unwrap_or(deriver)
            )?;
        }

        if let Some(ref system) = self.system {
            writeln!(&mut result, "System: {}", system)?;
        }

        if let Some(ref sig) = self.sig {
            writeln!(&mut result, "Sig: {}", sig)?;
        }

        if let Some(ref ca) = self.ca {
            writeln!(&mut result, "CA: {}", ca)?;
        }

        Ok(result)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_narinfo_creation() {
        let nar_data = b"test nar data";
        let compressed_data = b"compressed";

        let info = NarInfo::new(
            "/nix/store/abc123-test".to_string(),
            nar_data,
            compressed_data,
            "xz".to_string(),
        );

        assert_eq!(info.store_path, "/nix/store/abc123-test");
        assert_eq!(info.url, "nar/abc123-test.nar.xz");
        assert_eq!(info.compression, "xz");
        assert_eq!(info.nar_size, 13);
        assert_eq!(info.file_size, 10);
        assert!(info.nar_hash.starts_with("sha256:"));
        assert!(info.file_hash.starts_with("sha256:"));
    }

    #[test]
    fn test_narinfo_to_string() {
        let mut info = NarInfo::new(
            "/nix/store/abc123-test".to_string(),
            b"nar",
            b"compressed",
            "none".to_string(),
        );

        info.references.insert("/nix/store/def456-dep".to_string());
        info.system = Some("x86_64-linux".to_string());

        let result = info.to_string().unwrap();

        assert!(result.contains("StorePath: /nix/store/abc123-test"));
        assert!(result.contains("URL: nar/abc123-test.nar"));
        assert!(result.contains("Compression: none"));
        assert!(result.contains("References: def456-dep"));
        assert!(result.contains("System: x86_64-linux"));
    }
}
