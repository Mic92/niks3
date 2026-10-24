// Package configdir resolves the shared niks3 CLI configuration directory.
package configdir

import (
	"fmt"
	"os"
	"path/filepath"
)

// Dir follows the CLI's XDG convention on both Linux and macOS.
func Dir() (string, error) {
	dir := os.Getenv("XDG_CONFIG_HOME")
	if dir == "" {
		home, err := os.UserHomeDir()
		if err != nil {
			return "", fmt.Errorf("finding niks3 home directory: %w", err)
		}
		dir = filepath.Join(home, ".config")
	}

	return filepath.Join(dir, "niks3"), nil
}
