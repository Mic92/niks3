package signing

import (
	"errors"
	"fmt"
	"sort"
	"strconv"
	"strings"
)

// GenerateFingerprint generates a fingerprint for signing a store path
//
// The fingerprint format is:
// 1;<store-path>;<nar-hash>;<nar-size>;<comma-separated-references>
//
// Arguments:
//   - storePath: The full store path to fingerprint (e.g., "/nix/store/...")
//   - narHash: The NAR hash in format "sha256:..."
//   - narSize: The size of the NAR in bytes
//   - references: Sorted references to other store paths
func GenerateFingerprint(storePath, narHash string, narSize uint64, references []string) ([]byte, error) {
	// Validate store path
	if !strings.HasPrefix(storePath, "/nix/store") {
		return nil, errors.New("store path does not start with /nix/store")
	}

	// Validate NAR hash
	if !strings.HasPrefix(narHash, "sha256:") {
		return nil, errors.New("NAR hash must start with 'sha256:'")
	}

	if len(narHash) != 59 {
		return nil, fmt.Errorf("NAR hash has invalid length: expected 59, got %d", len(narHash))
	}

	// Validate references
	for _, ref := range references {
		if !strings.HasPrefix(ref, "/nix/store") {
			return nil, fmt.Errorf("reference path does not start with /nix/store: %s", ref)
		}
	}

	// Sort references to ensure deterministic fingerprints
	sortedRefs := make([]string, len(references))
	copy(sortedRefs, references)
	sort.Strings(sortedRefs)

	// Build the fingerprint
	var builder strings.Builder

	// Calculate capacity to minimize allocations
	capacity := 2 + // "1;"
		len(storePath) + 1 + // store path + ";"
		len(narHash) + 1 + // nar hash + ";"
		20 + 1 // nar size (max uint64 digits) + ";"

	for i, ref := range sortedRefs {
		capacity += len(ref)
		if i > 0 {
			capacity++ // comma
		}
	}

	builder.Grow(capacity)

	// Add fixed parts
	builder.WriteString("1;")
	builder.WriteString(storePath)
	builder.WriteByte(';')
	builder.WriteString(narHash)
	builder.WriteByte(';')
	builder.WriteString(strconv.FormatUint(narSize, 10))
	builder.WriteByte(';')

	// Add references (comma-separated)
	for i, ref := range sortedRefs {
		if i > 0 {
			builder.WriteByte(',')
		}

		builder.WriteString(ref)
	}

	return []byte(builder.String()), nil
}
