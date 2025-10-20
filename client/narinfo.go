package client

import (
	"bytes"
	"errors"
	"fmt"
	"sort"
	"strings"

	"github.com/klauspost/compress/zstd"
)

// generateNarinfoContent creates narinfo file content from metadata and signatures.
// This is used after receiving signatures from the server to generate the final narinfo.
func generateNarinfoContent(meta *NarinfoMetadata, signatures []string) string {
	var sb strings.Builder

	// StorePath
	fmt.Fprintf(&sb, "StorePath: %s\n", meta.StorePath)

	// URL to the NAR file
	fmt.Fprintf(&sb, "URL: %s\n", meta.URL)

	// Compression
	fmt.Fprintf(&sb, "Compression: %s\n", meta.Compression)

	// NAR hash and size (uncompressed)
	fmt.Fprintf(&sb, "NarHash: %s\n", meta.NarHash)
	fmt.Fprintf(&sb, "NarSize: %d\n", meta.NarSize)

	// FileHash and FileSize for compressed file
	fmt.Fprintf(&sb, "FileHash: %s\n", meta.FileHash)
	fmt.Fprintf(&sb, "FileSize: %d\n", meta.FileSize)

	// References (must have space after colon, even if empty)
	fmt.Fprint(&sb, "References:")

	// Sort references for deterministic output
	sortedRefs := make([]string, len(meta.References))
	copy(sortedRefs, meta.References)
	sort.Strings(sortedRefs)

	for _, ref := range sortedRefs {
		// Remove /nix/store/ prefix
		refName := strings.TrimPrefix(ref, "/nix/store/")
		fmt.Fprintf(&sb, " %s", refName)
	}

	// Always add a space after "References:" even if empty
	if len(meta.References) == 0 {
		fmt.Fprint(&sb, " ")
	}

	fmt.Fprint(&sb, "\n")

	// Deriver (optional)
	if meta.Deriver != nil {
		deriverName := strings.TrimPrefix(*meta.Deriver, "/nix/store/")
		fmt.Fprintf(&sb, "Deriver: %s\n", deriverName)
	}

	// Signatures (passed as parameter from signing process)
	if len(signatures) > 0 {
		// Sort signatures for deterministic output
		sortedSigs := make([]string, len(signatures))
		copy(sortedSigs, signatures)
		sort.Strings(sortedSigs)

		for _, sig := range sortedSigs {
			fmt.Fprintf(&sb, "Sig: %s\n", sig)
		}
	}

	// CA (optional)
	if meta.CA != nil {
		fmt.Fprintf(&sb, "CA: %s\n", *meta.CA)
	}

	return sb.String()
}

// CompressNarinfo compresses narinfo content using zstd with encoder pooling.
func CompressNarinfo(content string) ([]byte, error) {
	var buf bytes.Buffer

	// Get encoder from pool and reset it to write to buffer
	encoder, ok := zstdEncoderPool.Get().(*zstd.Encoder)
	if !ok {
		return nil, errors.New("failed to get zstd encoder from pool")
	}
	defer zstdEncoderPool.Put(encoder)

	encoder.Reset(&buf)

	if _, err := encoder.Write([]byte(content)); err != nil {
		return nil, fmt.Errorf("writing content: %w", err)
	}

	if err := encoder.Close(); err != nil {
		return nil, fmt.Errorf("closing encoder: %w", err)
	}

	return buf.Bytes(), nil
}
