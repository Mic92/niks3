package client

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
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

// String returns the content address in the narinfo-compatible string format.
// Nix narinfo files use these prefixes:
//   - "text:sha256:..." for text
//   - "fixed:sha256:..." for flat
//   - "fixed:r:sha256:..." for nar (recursive)
//   - "fixed:git:sha256:..." for git
//
// The hash is in nix32 format (sha256:base32hash).
func (ca *ContentAddress) String() string {
	// If we stored the old string format directly, return it as-is
	if ca.raw != "" {
		return ca.raw
	}

	// For new structured format, reconstruct the narinfo-compatible format
	// The JSON "method" field maps to narinfo prefixes as follows:
	//   "text" -> "text:"
	//   "flat" -> "fixed:"
	//   "nar"  -> "fixed:r:"
	//   "git"  -> "fixed:git:"
	if ca.method != "" {
		nix32Hash := ca.hash.Nix32()

		switch ca.method {
		case "text":
			return "text:" + nix32Hash
		case "flat":
			return "fixed:" + nix32Hash
		case string(ObjectTypeNAR):
			return "fixed:r:" + nix32Hash
		case "git":
			return "fixed:git:" + nix32Hash
		default:
			// Unknown method, use as-is (shouldn't happen)
			return ca.method + ":" + nix32Hash
		}
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
	args := make([]string, 0, 6+len(storePaths))
	args = append(args, "--extra-experimental-features", "nix-command", "path-info", "--recursive", "--json", "--")
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

	return parsePathInfoJSON(output)
}

// parsePathInfoJSON parses the JSON output of `nix path-info --json`.
// It supports both Nix format (object keyed by store path) and
// Lix format (array of objects with a "path" field).
func parsePathInfoJSON(output []byte) (map[string]*PathInfo, error) {
	// Try Nix format first: object keyed by store path
	var pathInfos map[string]*PathInfo
	if err := json.Unmarshal(output, &pathInfos); err == nil {
		for path, info := range pathInfos {
			info.Path = path
		}

		return pathInfos, nil
	}

	// Try Lix format: array of objects with "path" field
	type lixPathInfo struct {
		PathInfo

		Path string `json:"path"`
	}

	var lixInfos []lixPathInfo
	if err := json.Unmarshal(output, &lixInfos); err != nil {
		return nil, fmt.Errorf("parsing nix path-info output: %w", err)
	}

	result := make(map[string]*PathInfo, len(lixInfos))
	for i := range lixInfos {
		lixInfos[i].PathInfo.Path = lixInfos[i].Path
		result[lixInfos[i].Path] = &lixInfos[i].PathInfo
	}

	return result, nil
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

// Key is the binary-cache object key for r.
func (r *RealisationInfo) Key() string { return "realisations/" + r.ID + ".doi" }

// QueryRealisations returns realisations of CA paths keyed by full store
// path. Nix only answers for `<drv>^*`, not for store paths, and reports
// outPath as a basename.
func QueryRealisations(ctx context.Context, pathInfos map[string]*PathInfo, nixEnv []string) (map[string][]RealisationInfo, error) {
	byBase := make(map[string]string) // basename -> full store path
	drvs := make(map[string]struct{})

	for storePath, info := range pathInfos {
		if info.CA == nil || info.CA.String() == "" || info.Deriver == nil || *info.Deriver == "" {
			continue
		}

		byBase[filepath.Base(storePath)] = storePath
		drvs[*info.Deriver+"^*"] = struct{}{}
	}

	result := make(map[string][]RealisationInfo)
	if len(drvs) == 0 {
		return result, nil
	}

	installables := make([]string, 0, len(drvs))
	for d := range drvs {
		installables = append(installables, d)
	}

	const maxPerChunk = 1000
	for i := 0; i < len(installables); i += maxPerChunk {
		chunk := installables[i:min(i+maxPerChunk, len(installables))]

		args := append([]string{"--extra-experimental-features", "nix-command ca-derivations", "realisation", "info", "--json"}, chunk...)

		cmd := exec.CommandContext(ctx, "nix", args...)
		if len(nixEnv) > 0 {
			cmd.Env = nixEnv
		}

		output, err := cmd.Output()
		if err != nil {
			// e.g. deriver not present locally. Realisations are best effort.
			slog.Debug("nix realisation info failed", "error", err)

			continue
		}

		var realisations []RealisationInfo
		if err := json.Unmarshal(output, &realisations); err != nil {
			return nil, fmt.Errorf("parsing realisation info: %w", err)
		}

		for _, r := range realisations {
			if r.ID == "" {
				continue
			}

			if storePath, ok := byBase[filepath.Base(r.OutPath)]; ok {
				result[storePath] = append(result[storePath], r)
			}
		}
	}

	return result, nil
}
