package signing

import (
	"crypto/ed25519"
	"encoding/base64"
	"fmt"
	"strings"
)

// PublicKeys verifies narinfo signatures against a set of named Ed25519 keys.
type PublicKeys map[string]ed25519.PublicKey

// ParsePublicKeys parses keys in nix.conf format ("name:base64").
func ParsePublicKeys(keys []string) (PublicKeys, error) {
	parsed := make(PublicKeys, len(keys))

	for _, key := range keys {
		name, encoded, ok := strings.Cut(key, ":")
		if !ok || name == "" {
			return nil, fmt.Errorf("public key %q is not name:base64", key)
		}

		raw, err := base64.StdEncoding.DecodeString(encoded)
		if err != nil {
			return nil, fmt.Errorf("public key %s: %w", name, err)
		}

		if len(raw) != ed25519.PublicKeySize {
			return nil, fmt.Errorf("public key %s has %d bytes, want %d", name, len(raw), ed25519.PublicKeySize)
		}

		parsed[name] = raw
	}

	return parsed, nil
}

// Verifies reports whether any signature ("name:base64") is valid for info
// under one of the keys.
func (k PublicKeys) Verifies(info *NarInfo, signatures []string) (bool, error) {
	if len(k) == 0 || len(signatures) == 0 {
		return false, nil
	}

	fingerprint, err := GenerateFingerprint(info)
	if err != nil {
		return false, err
	}

	for _, signature := range signatures {
		name, encoded, ok := strings.Cut(signature, ":")
		if !ok {
			continue
		}

		key, ok := k[name]
		if !ok {
			continue
		}

		raw, err := base64.StdEncoding.DecodeString(encoded)
		if err != nil {
			continue
		}

		if ed25519.Verify(key, fingerprint, raw) {
			return true, nil
		}
	}

	return false, nil
}
