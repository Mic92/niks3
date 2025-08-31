use anyhow::{Context, Result};
use std::ffi::{OsStr, OsString};
use std::os::unix::ffi::{OsStrExt, OsStringExt};
use std::os::unix::fs::MetadataExt;
use std::path::Path;
use tokio::fs;
use tokio::io::{self, AsyncWrite, AsyncWriteExt, BufReader};

const NAR_VERSION_MAGIC_1: &str = "nix-archive-1";

// Case hack configuration - enabled by default on macOS
#[cfg(target_os = "macos")]
const USE_CASE_HACK: bool = true;
#[cfg(not(target_os = "macos"))]
const USE_CASE_HACK: bool = false;

const CASE_HACK_SUFFIX: &[u8] = b"~nix~case~hack~";

fn strip_case_hack_suffix(name: &OsStr) -> OsString {
    if !USE_CASE_HACK {
        return name.to_owned();
    }

    let bytes = name.as_bytes();

    // Find the position of the case hack suffix
    if let Some(pos) = bytes
        .windows(CASE_HACK_SUFFIX.len())
        .position(|window| window == CASE_HACK_SUFFIX)
    {
        // Return the name without the suffix and any trailing number
        OsString::from_vec(bytes[0..pos].to_vec())
    } else {
        name.to_owned()
    }
}

async fn write_string<W: AsyncWrite + Unpin>(writer: &mut W, s: &[u8]) -> io::Result<()> {
    let len = s.len() as u64;
    writer.write_all(&len.to_le_bytes()).await?;
    writer.write_all(s).await?;

    // Pad to 8-byte boundary
    let padding = (8 - (s.len() % 8)) % 8;
    if padding > 0 {
        writer.write_all(&vec![0; padding]).await?;
    }

    Ok(())
}

async fn write_str<W: AsyncWrite + Unpin>(writer: &mut W, s: &str) -> io::Result<()> {
    write_string(writer, s.as_bytes()).await
}

pub async fn dump_path<W: AsyncWrite + Unpin>(writer: &mut W, path: &Path) -> Result<()> {
    write_str(writer, NAR_VERSION_MAGIC_1).await?;
    write_str(writer, "(").await?;
    dump_path_inner(writer, path).await?;
    write_str(writer, ")").await?;
    Ok(())
}

async fn dump_path_inner<W: AsyncWrite + Unpin>(writer: &mut W, path: &Path) -> Result<()> {
    let metadata = fs::symlink_metadata(path)
        .await
        .with_context(|| format!("Failed to get metadata for {}", path.display()))?;

    write_str(writer, "type").await?;

    if metadata.is_file() {
        write_str(writer, "regular").await?;

        if metadata.mode() & 0o111 != 0 {
            write_str(writer, "executable").await?;
            write_str(writer, "").await?;
        }

        write_str(writer, "contents").await?;

        // Get file size and write the length first
        let file_size = metadata.len();
        writer.write_all(&file_size.to_le_bytes()).await?;

        // Stream the file contents
        let file = tokio::fs::File::open(path)
            .await
            .with_context(|| format!("Failed to open file {}", path.display()))?;
        let mut reader = BufReader::new(file);

        // Copy the file contents to the writer
        let bytes_copied = tokio::io::copy(&mut reader, writer)
            .await
            .with_context(|| format!("Failed to stream file {}", path.display()))?;

        // Ensure we copied the expected number of bytes
        if bytes_copied != file_size {
            anyhow::bail!(
                "File size mismatch for {}: expected {}, copied {}",
                path.display(),
                file_size,
                bytes_copied
            );
        }

        // Pad to 8-byte boundary
        let padding = (8 - (file_size % 8)) % 8;
        if padding > 0 {
            writer.write_all(&vec![0; padding as usize]).await?;
        }
    } else if metadata.is_dir() {
        write_str(writer, "directory").await?;

        let mut dir = fs::read_dir(path)
            .await
            .with_context(|| format!("Failed to read directory {}", path.display()))?;
        let mut entries = Vec::new();
        while let Some(entry) = dir.next_entry().await? {
            entries.push(entry.file_name());
        }

        // NAR format requires entries to be sorted
        entries.sort();

        for entry in entries {
            // Strip case hack suffix for NAR serialization
            let nar_name = strip_case_hack_suffix(&entry);

            write_str(writer, "entry").await?;
            write_str(writer, "(").await?;

            write_str(writer, "name").await?;
            write_string(writer, nar_name.as_bytes()).await?;

            write_str(writer, "node").await?;
            write_str(writer, "(").await?;
            Box::pin(dump_path_inner(writer, &path.join(&entry))).await?;
            write_str(writer, ")").await?;

            write_str(writer, ")").await?;
        }
    } else if metadata.is_symlink() {
        write_str(writer, "symlink").await?;
        write_str(writer, "target").await?;

        let target = fs::read_link(path)
            .await
            .with_context(|| format!("Failed to read symlink {}", path.display()))?;
        write_string(writer, target.as_os_str().as_bytes()).await?;
    } else {
        anyhow::bail!("Unsupported file type for {}", path.display());
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::process::Command;
    use tempfile::tempdir;

    #[tokio::test]
    async fn test_nar_simple_file() {
        let dir = tempdir().unwrap();
        let file_path = dir.path().join("test.txt");
        fs::write(&file_path, b"hello world").await.unwrap();

        let mut buf = Vec::new();
        dump_path(&mut buf, &file_path).await.unwrap();

        // Verify the NAR starts with the magic header
        assert!(buf.len() > 30);
        // Check for the length-prefixed "nix-archive-1" string
        assert_eq!(&buf[0..8], &13u64.to_le_bytes());
        assert_eq!(&buf[8..21], b"nix-archive-1");

        // Check that it contains "type" and "regular"
        let nar_str = String::from_utf8_lossy(&buf);
        assert!(nar_str.contains("type"));
        assert!(nar_str.contains("regular"));
        assert!(nar_str.contains("contents"));
        assert!(nar_str.contains("hello world"));
    }

    #[tokio::test]
    async fn test_nar_executable_file() {
        let dir = tempdir().unwrap();
        let file_path = dir.path().join("test.sh");
        fs::write(&file_path, b"#!/bin/sh\necho hello")
            .await
            .unwrap();

        // Make executable
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            let mut perms = fs::metadata(&file_path).await.unwrap().permissions();
            perms.set_mode(0o755);
            fs::set_permissions(&file_path, perms).await.unwrap();
        }

        let mut buf = Vec::new();
        dump_path(&mut buf, &file_path).await.unwrap();

        let nar_str = String::from_utf8_lossy(&buf);
        assert!(nar_str.contains("executable"));
    }

    #[tokio::test]
    async fn test_nar_directory() {
        let dir = tempdir().unwrap();
        let subdir = dir.path().join("subdir");
        fs::create_dir(&subdir).await.unwrap();
        fs::write(subdir.join("file1.txt"), b"content1")
            .await
            .unwrap();
        fs::write(subdir.join("file2.txt"), b"content2")
            .await
            .unwrap();

        let mut buf = Vec::new();
        dump_path(&mut buf, &subdir).await.unwrap();

        let nar_str = String::from_utf8_lossy(&buf);
        assert!(nar_str.contains("directory"));
        assert!(nar_str.contains("entry"));
        assert!(nar_str.contains("file1.txt"));
        assert!(nar_str.contains("file2.txt"));
    }

    #[tokio::test]
    async fn test_nar_symlink() {
        let dir = tempdir().unwrap();
        let target = dir.path().join("target.txt");
        let link = dir.path().join("link");

        fs::write(&target, b"target content").await.unwrap();
        tokio::fs::symlink("target.txt", &link).await.unwrap();

        let mut buf = Vec::new();
        dump_path(&mut buf, &link).await.unwrap();

        let nar_str = String::from_utf8_lossy(&buf);
        assert!(nar_str.contains("symlink"));
        assert!(nar_str.contains("target"));
        assert!(nar_str.contains("target.txt"));
    }

    #[tokio::test]
    async fn test_nar_compare_with_nix_comprehensive() {
        // This test requires nix to be installed
        let nix_check = Command::new("nix-store").arg("--version").output();
        if nix_check.is_err() {
            eprintln!("Skipping test: nix-store not found");
            return;
        }

        let dir = tempdir().unwrap();
        let test_dir = dir.path().join("test");
        fs::create_dir(&test_dir).await.unwrap();

        // Create a comprehensive directory structure with all NAR features

        // 1. Regular file
        fs::write(test_dir.join("regular.txt"), b"Hello, NAR!")
            .await
            .unwrap();

        // 2. Executable file
        let exec_path = test_dir.join("script.sh");
        fs::write(&exec_path, b"#!/bin/sh\necho 'Hello from NAR'")
            .await
            .unwrap();
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            let mut perms = fs::metadata(&exec_path).await.unwrap().permissions();
            perms.set_mode(0o755);
            fs::set_permissions(&exec_path, perms).await.unwrap();
        }

        // 3. Empty file
        fs::write(test_dir.join("empty"), b"").await.unwrap();

        // 4. Subdirectory with files
        let subdir = test_dir.join("subdir");
        fs::create_dir(&subdir).await.unwrap();
        fs::write(subdir.join("nested.txt"), b"Nested content")
            .await
            .unwrap();

        // 5. Another subdirectory to test sorting
        let another_dir = test_dir.join("another");
        fs::create_dir(&another_dir).await.unwrap();
        fs::write(another_dir.join("file.txt"), b"Another file")
            .await
            .unwrap();

        // 6. Symlink to a file
        fs::symlink("regular.txt", test_dir.join("link-to-file"))
            .await
            .unwrap();

        // 7. Symlink to a directory
        fs::symlink("subdir", test_dir.join("link-to-dir"))
            .await
            .unwrap();

        // 8. Broken symlink
        fs::symlink("nonexistent", test_dir.join("broken-link"))
            .await
            .unwrap();

        // 9. File with special characters in name
        fs::write(test_dir.join("special-#@$.txt"), b"Special name")
            .await
            .unwrap();

        // Also test emoji in filename
        fs::write(test_dir.join("emoji-🦀-file.txt"), b"Rust emoji")
            .await
            .unwrap();

        // Test case hack suffixes (if on macOS)
        #[cfg(target_os = "macos")]
        {
            fs::write(test_dir.join("CaseTest"), b"original")
                .await
                .unwrap();
            fs::write(
                test_dir.join("casetest~nix~case~hack~1"),
                b"lowercase variant",
            )
            .await
            .unwrap();
            fs::write(
                test_dir.join("CASETEST~nix~case~hack~2"),
                b"uppercase variant",
            )
            .await
            .unwrap();
        }

        // 10. Deeply nested directory
        let deep = test_dir.join("deep");
        fs::create_dir(&deep).await.unwrap();
        let level1 = deep.join("level1");
        fs::create_dir(&level1).await.unwrap();
        let level2 = level1.join("level2");
        fs::create_dir(&level2).await.unwrap();
        fs::write(level2.join("deep.txt"), b"Deep file")
            .await
            .unwrap();

        // 11. Empty directory
        fs::create_dir(test_dir.join("empty-dir")).await.unwrap();

        // 12. Large file content (to test padding)
        let large_content = vec![b'X'; 1000];
        fs::write(test_dir.join("large.bin"), &large_content)
            .await
            .unwrap();

        // Generate NAR with our implementation
        let mut our_nar = Vec::new();
        dump_path(&mut our_nar, &test_dir).await.unwrap();

        // Generate NAR with nix-store
        let output = Command::new("nix-store")
            .arg("--dump")
            .arg(test_dir.canonicalize().unwrap())
            .output()
            .unwrap();

        if !output.status.success() {
            eprintln!(
                "nix-store stderr: {}",
                String::from_utf8_lossy(&output.stderr)
            );
            panic!(
                "nix-store failed with exit code: {:?}",
                output.status.code()
            );
        }

        let nix_nar = output.stdout;

        // Compare the outputs
        if our_nar != nix_nar {
            // Write both outputs to files for debugging
            use std::io::Write;
            let mut our_file = std::fs::File::create("/tmp/our.nar").unwrap();
            our_file.write_all(&our_nar).unwrap();
            let mut nix_file = std::fs::File::create("/tmp/nix.nar").unwrap();
            nix_file.write_all(&nix_nar).unwrap();

            panic!(
                "NAR output differs from nix-store. Written to /tmp/our.nar and /tmp/nix.nar for comparison"
            );
        }
    }

    #[test]
    fn test_strip_case_hack_suffix() {
        use std::ffi::OsStr;

        // Test stripping the suffix
        let name = OsStr::new("FOO~nix~case~hack~1");
        let stripped = strip_case_hack_suffix(name);
        if USE_CASE_HACK {
            assert_eq!(stripped, OsStr::new("FOO"));
        } else {
            assert_eq!(stripped, name);
        }

        // Test name without suffix
        let name = OsStr::new("normal_file.txt");
        let stripped = strip_case_hack_suffix(name);
        assert_eq!(stripped, name);

        // Test name with suffix in the middle (should strip from first occurrence)
        let name = OsStr::new("foo~nix~case~hack~bar~nix~case~hack~2");
        let stripped = strip_case_hack_suffix(name);
        if USE_CASE_HACK {
            assert_eq!(stripped, OsStr::new("foo"));
        } else {
            assert_eq!(stripped, name);
        }
    }

    #[tokio::test]
    async fn test_nar_case_hack() {
        if !USE_CASE_HACK {
            eprintln!("Skipping case hack test on non-macOS platform");
            return;
        }

        let dir = tempdir().unwrap();
        let test_dir = dir.path().join("test");
        fs::create_dir(&test_dir).await.unwrap();

        // Create files that would have case hack suffixes
        fs::write(test_dir.join("foo"), b"original").await.unwrap();
        fs::write(test_dir.join("FOO~nix~case~hack~1"), b"case variant")
            .await
            .unwrap();

        let mut buf = Vec::new();
        dump_path(&mut buf, &test_dir).await.unwrap();

        let nar_str = String::from_utf8_lossy(&buf);

        // The NAR should contain both entries as "foo" and "FOO"
        // Count occurrences of each name
        let foo_count = nar_str.matches("foo").count();
        let foo_upper_count = nar_str.matches("FOO").count();

        // Should have at least one occurrence of each
        assert!(foo_count >= 1, "Should contain 'foo'");
        assert!(foo_upper_count >= 1, "Should contain 'FOO'");

        // Should NOT contain the case hack suffix in the NAR
        assert!(
            !nar_str.contains("~nix~case~hack~"),
            "NAR should not contain case hack suffix"
        );
    }

    #[tokio::test]
    #[cfg(target_os = "macos")]
    async fn test_nar_case_hack_nix_compatibility() {
        // This test verifies our case hack implementation matches nix-store
        let nix_check = Command::new("nix-store").arg("--version").output();
        if nix_check.is_err() {
            eprintln!("Skipping test: nix-store not found");
            return;
        }

        let dir = tempdir().unwrap();
        let test_dir = dir.path().join("test");
        fs::create_dir(&test_dir).await.unwrap();

        // Create a directory with case-conflicting files using case hack suffixes
        fs::write(test_dir.join("README"), b"Original README")
            .await
            .unwrap();
        fs::write(test_dir.join("readme~nix~case~hack~1"), b"Lowercase readme")
            .await
            .unwrap();
        fs::write(
            test_dir.join("Readme~nix~case~hack~2"),
            b"Capitalized Readme",
        )
        .await
        .unwrap();

        // Generate NAR with our implementation
        let mut our_nar = Vec::new();
        dump_path(&mut our_nar, &test_dir).await.unwrap();

        // Generate NAR with nix-store
        let output = Command::new("nix-store")
            .arg("--dump")
            .arg(test_dir.canonicalize().unwrap())
            .output()
            .unwrap();

        if !output.status.success() {
            eprintln!(
                "nix-store stderr: {}",
                String::from_utf8_lossy(&output.stderr)
            );
            panic!(
                "nix-store failed with exit code: {:?}",
                output.status.code()
            );
        }

        let nix_nar = output.stdout;

        // Compare the outputs
        if our_nar != nix_nar {
            // For debugging, let's see what's in both NARs
            let our_str = String::from_utf8_lossy(&our_nar);
            let nix_str = String::from_utf8_lossy(&nix_nar);

            eprintln!("Our NAR contains: {:?}", our_str.matches("readme").count());
            eprintln!("Nix NAR contains: {:?}", nix_str.matches("readme").count());

            // Write both outputs to files for debugging
            use std::io::Write;
            let mut our_file = std::fs::File::create("/tmp/our_case_hack.nar").unwrap();
            our_file.write_all(&our_nar).unwrap();
            let mut nix_file = std::fs::File::create("/tmp/nix_case_hack.nar").unwrap();
            nix_file.write_all(&nix_nar).unwrap();

            panic!(
                "NAR output differs from nix-store. Written to /tmp/our_case_hack.nar and /tmp/nix_case_hack.nar for comparison"
            );
        }
    }
}
