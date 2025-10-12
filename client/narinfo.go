package client

import (
	"fmt"
	"strings"
)

// CreateNarinfo generates a narinfo file content.
func CreateNarinfo(pathInfo *PathInfo, narFilename string, compressedSize uint64, fileHash string) string {
	var sb strings.Builder

	// StorePath
	fmt.Fprintf(&sb, "StorePath: %s\n", pathInfo.Path)

	// URL to the NAR file
	fmt.Fprintf(&sb, "URL: nar/%s\n", narFilename)

	// Compression
	fmt.Fprintf(&sb, "Compression: zstd\n")

	// NAR hash and size (uncompressed)
	// Convert NarHash from SRI format (sha256-base64) to Nix32 format (sha256:nix32)
	narHash := pathInfo.NarHash
	if convertedHash, err := ConvertHashToNix32(pathInfo.NarHash); err == nil {
		narHash = convertedHash
	}

	fmt.Fprintf(&sb, "NarHash: %s\n", narHash)
	fmt.Fprintf(&sb, "NarSize: %d\n", pathInfo.NarSize)

	// FileHash and FileSize for compressed file
	fmt.Fprintf(&sb, "FileHash: %s\n", fileHash)
	fmt.Fprintf(&sb, "FileSize: %d\n", compressedSize)

	// References (must have space after colon, even if empty)
	fmt.Fprint(&sb, "References:")

	for _, ref := range pathInfo.References {
		// Remove /nix/store/ prefix
		refName := strings.TrimPrefix(ref, "/nix/store/")
		fmt.Fprintf(&sb, " %s", refName)
	}

	// Always add a space after "References:" even if empty
	if len(pathInfo.References) == 0 {
		fmt.Fprint(&sb, " ")
	}

	fmt.Fprint(&sb, "\n")

	// Deriver (optional)
	if pathInfo.Deriver != nil {
		deriverName := strings.TrimPrefix(*pathInfo.Deriver, "/nix/store/")
		fmt.Fprintf(&sb, "Deriver: %s\n", deriverName)
	}

	// Signatures (optional)
	if len(pathInfo.Signatures) > 0 {
		for _, sig := range pathInfo.Signatures {
			fmt.Fprintf(&sb, "Sig: %s\n", sig)
		}
	}

	// CA (optional)
	if pathInfo.CA != nil {
		fmt.Fprintf(&sb, "CA: %s\n", *pathInfo.CA)
	}

	return sb.String()
}
