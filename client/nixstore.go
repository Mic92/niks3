package client

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
)

// GetStoreDir determines the Nix store directory path.
// It checks in order:
// 1. NIX_STORE_DIR environment variable (from nixEnv if provided)
// 2. Queries nix command
// 3. Falls back to default "/nix/store"
// Returns the store directory (e.g., "/nix/store").
func GetStoreDir(ctx context.Context, nixEnv []string) (string, error) {
	// First check NIX_STORE_DIR environment variable
	if len(nixEnv) > 0 {
		for _, env := range nixEnv {
			if after, ok := strings.CutPrefix(env, "NIX_STORE_DIR="); ok {
				return after, nil
			}
		}
	} else if storeDir := os.Getenv("NIX_STORE_DIR"); storeDir != "" {
		return storeDir, nil
	}

	// Try to query nix command
	cmd := exec.CommandContext(ctx, "nix", "--extra-experimental-features", "nix-command", "eval", "--raw", "--expr", "builtins.storeDir")
	if len(nixEnv) > 0 {
		cmd.Env = nixEnv
	}

	output, err := cmd.Output()
	if err == nil {
		storeDir := strings.TrimSpace(string(output))
		if storeDir != "" {
			return storeDir, nil
		}
	}

	// Fall back to default /nix/store
	// This is the standard location on NixOS and most Nix installations
	return "/nix/store", nil
}

// Hash represents a Nix hash value.
// It supports both the old string format (e.g., "sha256-base64hash")
// and the new structured format from Nix 2.33+.
type Hash struct {
	algorithm string
	format    string
	hash      string
}

// UnmarshalJSON implements custom JSON unmarshaling to support both
// old string format and new structured format from nix path-info.
func (h *Hash) UnmarshalJSON(data []byte) error {
	// Try to unmarshal as string (old format)
	var hashStr string
	if err := json.Unmarshal(data, &hashStr); err == nil {
		// Old format: store the string as-is
		h.hash = hashStr

		// Parse algorithm from string for consistency
		if strings.HasPrefix(hashStr, "sha256:") || strings.HasPrefix(hashStr, "sha256-") {
			h.algorithm = "sha256"
		} else if strings.HasPrefix(hashStr, "sha512:") || strings.HasPrefix(hashStr, "sha512-") {
			h.algorithm = "sha512"
		}
		return nil
	}

	// Try to unmarshal as structured object (new format)
	var hashObj struct {
		Algorithm string `json:"algorithm"`
		Format    string `json:"format"`
		Hash      string `json:"hash"`
	}
	if err := json.Unmarshal(data, &hashObj); err != nil {
		return fmt.Errorf("hash must be either a string or structured object: %w", err)
	}

	h.algorithm = hashObj.Algorithm
	h.format = hashObj.Format
	h.hash = hashObj.Hash
	return nil
}

// String returns the hash in SRI format (e.g., "sha256-base64hash").
// This format is compatible with the existing ConvertHashToNix32 function.
func (h *Hash) String() string {
	if h.hash == "" {
		return ""
	}

	// If we stored the old string format directly, return it as-is
	if strings.Contains(h.hash, ":") || strings.Contains(h.hash, "-") {
		return h.hash
	}

	// For new structured format, construct SRI format (algorithm-hash)
	// This is the standard format that ConvertHashToNix32 already handles
	if h.algorithm != "" {
		return h.algorithm + "-" + h.hash
	}

	return h.hash
}

// ContentAddress represents a Nix content address.
// It supports both the old string format (e.g., "fixed:r:sha256:abc...")
// and the new structured format from Nix 2.33+.
type ContentAddress struct {
	method string
	hash   Hash
	raw    string // Store original string if provided
}

// UnmarshalJSON implements custom JSON unmarshaling to support both
// old string format and new structured format from nix path-info.
func (ca *ContentAddress) UnmarshalJSON(data []byte) error {
	// Try to unmarshal as string (old format)
	var caStr string
	if err := json.Unmarshal(data, &caStr); err == nil {
		// Old format: store the string as-is
		ca.raw = caStr
		// Parse method from string (e.g., "fixed:r:sha256:..." -> method could be "fixed")
		// We'll just store the whole string for now
		return nil
	}

	// Try to unmarshal as structured object (new format)
	var caObj struct {
		Method string `json:"method"`
		Hash   Hash   `json:"hash"`
	}
	if err := json.Unmarshal(data, &caObj); err != nil {
		return fmt.Errorf("ca must be either a string or structured object: %w", err)
	}

	ca.method = caObj.Method
	ca.hash = caObj.Hash
	return nil
}

// String returns the content address in the old string format.
// This format is compatible with existing code that expects strings.
func (ca *ContentAddress) String() string {
	// If we stored the old string format directly, return it as-is
	if ca.raw != "" {
		return ca.raw
	}

	// For new structured format, reconstruct the old string format
	// The old format was like "fixed:r:sha256:hash" or just "text:sha256:hash"
	// For now, we'll construct a simplified version
	if ca.method != "" {
		// Construct something like "method:hash"
		// Note: The old format had variants like "fixed:r:" for recursive, but
		// we don't have enough info to reconstruct that exactly, so we'll do our best
		hashStr := ca.hash.String()
		// Remove the algorithm prefix from hash since it will be in the CA string
		// Old format example: "fixed:r:sha256:abc..." or "text:sha256:abc..."
		return ca.method + ":" + hashStr
	}

	return ca.raw
}

// PathInfo represents Nix path information.
type PathInfo struct {
	Path string `json:"-"`
	//nolint:tagliatelle // narHash and narSize are defined by Nix's JSON format
	NarHash Hash `json:"narHash"`
	//nolint:tagliatelle // narHash and narSize are defined by Nix's JSON format
	NarSize    uint64          `json:"narSize"`
	References []string        `json:"references"`
	Deriver    *string         `json:"deriver,omitempty"`
	Signatures []string        `json:"signatures,omitempty"`
	CA         *ContentAddress `json:"ca,omitempty"`
}

// RealisationInfo represents Nix realisation information for CA derivations.
type RealisationInfo struct {
	ID                    string            `json:"id"`      // "sha256:hash!outputName"
	OutPath               string            `json:"outPath"` //nolint:tagliatelle // outPath is defined by Nix's JSON format
	Signatures            []string          `json:"signatures,omitempty"`
	DependentRealisations map[string]string `json:"dependentRealisations,omitempty"` //nolint:tagliatelle
}

// GetPathInfoRecursive queries Nix for path info including all dependencies.
func GetPathInfoRecursive(ctx context.Context, storePaths []string, nixEnv []string) (map[string]*PathInfo, error) {
	args := []string{"--extra-experimental-features", "nix-command", "path-info", "--recursive", "--json", "--"}
	args = append(args, storePaths...)

	cmd := exec.CommandContext(ctx, "nix", args...)
	if len(nixEnv) > 0 {
		cmd.Env = nixEnv
	}

	output, err := cmd.Output()
	if err != nil {
		cmdStr := "nix " + strings.Join(args, " ")

		var exitErr *exec.ExitError
		if errors.As(err, &exitErr) {
			return nil, fmt.Errorf("command failed: %s\nstderr: %s\nerror: %w", cmdStr, exitErr.Stderr, err)
		}

		return nil, fmt.Errorf("command failed: %s\nerror: %w", cmdStr, err)
	}

	// Parse JSON output
	var pathInfos map[string]*PathInfo
	if err := json.Unmarshal(output, &pathInfos); err != nil {
		return nil, fmt.Errorf("parsing nix path-info output: %w", err)
	}

	// Populate path field from map keys
	for path, info := range pathInfos {
		info.Path = path
	}

	return pathInfos, nil
}

// GetStorePathHash extracts the hash from a store path.
// e.g., "/nix/store/abc123-name" -> "abc123".
func GetStorePathHash(storePath string) (string, error) {
	base := filepath.Base(storePath)

	// Require at least hash and name separated by hyphen
	parts := strings.SplitN(base, "-", 2)
	if len(parts) < 2 {
		return "", fmt.Errorf("invalid store path format (missing hyphen): %s", storePath)
	}

	hash := parts[0]

	// Validate hash length (Nix uses 32-character base32-encoded hashes)
	// This is the length of base32-encoded 160-bit (20-byte) hashes
	const expectedHashLen = 32
	if len(hash) != expectedHashLen {
		return "", fmt.Errorf("invalid hash length %d (expected %d): %s", len(hash), expectedHashLen, storePath)
	}

	// Validate hash charset (Nix base32: 0-9 and a-z except e,o,t,u)
	for i, ch := range hash {
		if !strings.ContainsRune(nixBase32Alphabet, ch) {
			return "", fmt.Errorf("invalid character %q at position %d in hash: %s", ch, i, storePath)
		}
	}

	return hash, nil
}

// QueryRealisations queries realisations from Nix's local database using `nix realisation info`.
// It only queries paths that have the CA field set, as non-CA paths don't have realisations.
// Returns a map from realisation key ("realisations/<id>.doi") to RealisationInfo.
func QueryRealisations(ctx context.Context, pathInfos map[string]*PathInfo, nixEnv []string) (map[string]*RealisationInfo, error) {
	// OPTIMIZATION: Only query paths that have CA field set
	// Non-CA paths don't have realisations, so skip them
	caPaths := make([]string, 0, len(pathInfos))
	for _, info := range pathInfos {
		if info.CA != nil && info.CA.String() != "" {
			caPaths = append(caPaths, info.Path)
		}
	}

	if len(caPaths) == 0 {
		return make(map[string]*RealisationInfo), nil
	}

	result := make(map[string]*RealisationInfo)

	// Chunk paths to avoid ARG_MAX overflow (typically 2MB on most systems)
	// Store paths are ~60 chars, so 1000 paths per chunk is safe (~300KB with overhead)
	const maxPathsPerChunk = 1000
	for i := 0; i < len(caPaths); i += maxPathsPerChunk {
		end := min(i+maxPathsPerChunk, len(caPaths))

		chunk := caPaths[i:end]

		// Batch query chunk of CA paths
		args := append([]string{"--extra-experimental-features", "nix-command ca-derivations", "realisation", "info", "--json"}, chunk...)

		cmd := exec.CommandContext(ctx, "nix", args...)
		if len(nixEnv) > 0 {
			cmd.Env = nixEnv
		}

		output, err := cmd.Output()
		if err != nil {
			// Some CA paths might not have realisations yet - this is OK
			continue
		}

		var realisations []RealisationInfo
		if err := json.Unmarshal(output, &realisations); err != nil {
			return nil, fmt.Errorf("parsing realisation info: %w", err)
		}

		// Build map: realisations/<id>.doi -> RealisationInfo
		for _, r := range realisations {
			key := "realisations/" + r.ID + ".doi"
			rCopy := r
			result[key] = &rCopy
		}
	}

	return result, nil
}
