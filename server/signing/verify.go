package signing

import (
	"crypto/ed25519"
	"encoding/base64"
	"fmt"
	"strings"
)

// ValidatePublicKeys checks that every key is in nix.conf format ("name:base64")
// and holds an Ed25519 public key.
func ValidatePublicKeys(keys []string) error {
	for _, key := range keys {
		name, encoded, ok := strings.Cut(key, ":")
		if !ok || name == "" {
			return fmt.Errorf("public key %q is not name:base64", key)
		}

		raw, err := base64.StdEncoding.DecodeString(encoded)
		if err != nil || len(raw) != ed25519.PublicKeySize {
			return fmt.Errorf("public key %s is not a base64 Ed25519 key", name)
		}
	}

	return nil
}
