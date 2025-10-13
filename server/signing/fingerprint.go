package signing

import (
	"errors"
	"fmt"
	"sort"
	"strconv"
	"strings"
)

// NarInfo encapsulates the metadata needed for narinfo operations.
type NarInfo struct {
	StorePath  string
	NarHash    string
	NarSize    uint64
	References []string
}

// GenerateFingerprint generates a fingerprint for signing a store path
//
// The fingerprint format is:
// 1;<store-path>;<nar-hash>;<nar-size>;<comma-separated-references>.
func GenerateFingerprint(info *NarInfo) ([]byte, error) {
	if info == nil {
		return nil, errors.New("NarInfo cannot be nil")
	}

	// Validate store path
	if !strings.HasPrefix(info.StorePath, "/nix/store") {
		return nil, errors.New("store path does not start with /nix/store")
	}

	// Validate NAR hash
	if !strings.HasPrefix(info.NarHash, "sha256:") {
		return nil, errors.New("NAR hash must start with 'sha256:'")
	}

	if len(info.NarHash) != 59 {
		return nil, fmt.Errorf("NAR hash has invalid length: expected 59, got %d", len(info.NarHash))
	}

	// Validate references
	for _, ref := range info.References {
		if !strings.HasPrefix(ref, "/nix/store") {
			return nil, fmt.Errorf("reference path does not start with /nix/store: %s", ref)
		}
	}

	// Sort references to ensure deterministic fingerprints
	sortedRefs := make([]string, len(info.References))
	copy(sortedRefs, info.References)
	sort.Strings(sortedRefs)

	// Build the fingerprint
	var builder strings.Builder

	// Calculate capacity to minimize allocations
	capacity := 2 + // "1;"
		len(info.StorePath) + 1 + // store path + ";"
		len(info.NarHash) + 1 + // nar hash + ";"
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
	builder.WriteString(info.StorePath)
	builder.WriteByte(';')
	builder.WriteString(info.NarHash)
	builder.WriteByte(';')
	builder.WriteString(strconv.FormatUint(info.NarSize, 10))
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
