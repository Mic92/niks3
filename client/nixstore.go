package client

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os/exec"
	"path/filepath"
	"strings"
)

// PathInfo represents Nix path information.
type PathInfo struct {
	Path string `json:"-"`
	//nolint:tagliatelle // narHash and narSize are defined by Nix's JSON format
	NarHash string `json:"narHash"`
	//nolint:tagliatelle // narHash and narSize are defined by Nix's JSON format
	NarSize    uint64   `json:"narSize"`
	References []string `json:"references"`
	Deriver    *string  `json:"deriver,omitempty"`
	Signatures []string `json:"signatures,omitempty"`
	CA         *string  `json:"ca,omitempty"`
}

// GetPathInfoRecursive queries Nix for path info including all dependencies.
func GetPathInfoRecursive(ctx context.Context, storePaths []string) (map[string]*PathInfo, error) {
	args := []string{"--extra-experimental-features", "nix-command", "path-info", "--recursive", "--json"}
	args = append(args, storePaths...)

	cmd := exec.CommandContext(ctx, "nix", args...)

	output, err := cmd.Output()
	if err != nil {
		var exitErr *exec.ExitError
		if errors.As(err, &exitErr) {
			return nil, fmt.Errorf("nix path-info failed: %s: %w", exitErr.Stderr, err)
		}

		return nil, fmt.Errorf("nix path-info failed: %w", err)
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

	parts := strings.SplitN(base, "-", 2)
	if len(parts) < 1 {
		return "", fmt.Errorf("invalid store path format: %s", storePath)
	}

	return parts[0], nil
}
