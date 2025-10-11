package client

import (
	"compress/bzip2"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"strings"

	"github.com/klauspost/compress/zstd"
)

// GetBuildLogPath finds the build log file for a derivation path.
// It checks for both plain and .bz2 compressed logs.
// Returns the path to the log file if found, or an empty string if not found.
func GetBuildLogPath(drvPath string) (string, error) {
	// Extract the derivation name from the full path
	// e.g., "/nix/store/abcd1234-hello.drv" -> "abcd1234-hello.drv"
	drvName := filepath.Base(drvPath)
	if drvName == "" || drvName == "." || drvName == "/" {
		return "", fmt.Errorf("invalid derivation path: %s", drvPath)
	}

	// Get the store directory (e.g., /nix/store)
	storeDir := filepath.Dir(drvPath)

	// Get the parent directory of the store (e.g., /nix)
	storeParent := filepath.Dir(storeDir)

	// Build log path: {store_parent}/var/log/nix/drvs/{first_2_chars}/{rest_of_name}
	if len(drvName) < 2 {
		return "", fmt.Errorf("derivation name too short: %s", drvName)
	}

	logPath := filepath.Join(
		storeParent,
		"var", "log", "nix", "drvs",
		drvName[:2],
		drvName[2:],
	)

	// Check if plain log exists
	if _, err := os.Stat(logPath); err == nil {
		return logPath, nil
	}

	// Check if compressed log exists
	compressedPath := logPath + ".bz2"
	if _, err := os.Stat(compressedPath); err == nil {
		return compressedPath, nil
	}

	// Log not found
	return "", nil
}

// CompressedBuildLogInfo contains information about the compressed build log.
type CompressedBuildLogInfo struct {
	TempFile string // Path to temporary file containing compressed log
	Size     int64  // Size of compressed log
}

// Cleanup removes the temporary compressed log file.
// It's safe to call multiple times.
func (info *CompressedBuildLogInfo) Cleanup() error {
	if info.TempFile == "" {
		return nil
	}

	if err := os.Remove(info.TempFile); err != nil {
		return fmt.Errorf("removing temp file %s: %w", info.TempFile, err)
	}

	return nil
}

// CompressBuildLog reads and compresses a build log file to a temporary file.
// It automatically decompresses .bz2 source files and recompresses with zstd.
// Returns info about the compressed temp file. The caller must call Cleanup() when done.
func CompressBuildLog(logPath string) (*CompressedBuildLogInfo, error) {
	// Open source log file
	srcFile, err := os.Open(logPath)
	if err != nil {
		return nil, fmt.Errorf("opening build log: %w", err)
	}

	defer func() {
		if err := srcFile.Close(); err != nil {
			slog.Error("Failed to close source file", "error", err)
		}
	}()

	var reader io.Reader = srcFile

	// If the file is .bz2 compressed, wrap it in a decompressor
	// Use LimitReader to protect against decompression bombs (limit to 1GB decompressed)
	if strings.HasSuffix(logPath, ".bz2") {
		reader = io.LimitReader(bzip2.NewReader(srcFile), 1<<30) // 1GB limit
	}

	// Create temporary file for compressed output
	tempFile, err := os.CreateTemp("", "buildlog-*.zst")
	if err != nil {
		return nil, fmt.Errorf("creating temp file: %w", err)
	}

	defer func() {
		if err := tempFile.Close(); err != nil {
			slog.Error("Failed to close temp file", "error", err)
		}
	}()

	// Get encoder from pool and reset it to write to temp file
	encoder, ok := zstdEncoderPool.Get().(*zstd.Encoder)
	if !ok {
		if err := os.Remove(tempFile.Name()); err != nil {
			slog.Error("Failed to remove temp file", "path", tempFile.Name(), "error", err)
		}

		return nil, errors.New("failed to get zstd encoder from pool")
	}
	defer zstdEncoderPool.Put(encoder)

	encoder.Reset(tempFile)

	// Stream compress the log
	if _, err := io.Copy(encoder, reader); err != nil {
		_ = encoder.Close()

		if removeErr := os.Remove(tempFile.Name()); removeErr != nil {
			slog.Error("Failed to remove temp file", "path", tempFile.Name(), "error", removeErr)
		}

		return nil, fmt.Errorf("compressing build log: %w", err)
	}

	if err := encoder.Close(); err != nil {
		if removeErr := os.Remove(tempFile.Name()); removeErr != nil {
			slog.Error("Failed to remove temp file", "path", tempFile.Name(), "error", removeErr)
		}

		return nil, fmt.Errorf("closing zstd encoder: %w", err)
	}

	// Get compressed file size
	stat, err := tempFile.Stat()
	if err != nil {
		if removeErr := os.Remove(tempFile.Name()); removeErr != nil {
			slog.Error("Failed to remove temp file", "path", tempFile.Name(), "error", removeErr)
		}

		return nil, fmt.Errorf("getting temp file size: %w", err)
	}

	return &CompressedBuildLogInfo{
		TempFile: tempFile.Name(),
		Size:     stat.Size(),
	}, nil
}
