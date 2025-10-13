package signing

import (
	"errors"
	"fmt"
)

// SignNarinfo generates signatures for a narinfo file using multiple signing keys
//
// Arguments:
//   - keys: The signing keys to use for signing
//   - storePath: The store path being signed
//   - narHash: The NAR hash in format "sha256:..."
//   - narSize: The size of the NAR in bytes
//   - references: References to other store paths
//
// Returns an array of signature strings in the format "name:base64-signature",
// one for each provided signing key.
func SignNarinfo(keys []*Key, storePath, narHash string, narSize uint64, references []string) ([]string, error) {
	// Validate keys to prevent unsigned narinfos
	if keys == nil {
		return nil, errors.New("signing keys cannot be nil")
	}

	if len(keys) == 0 {
		return nil, errors.New("signing keys cannot be empty - at least one key is required")
	}

	// Validate that no key is nil to prevent nil pointer dereference
	for i, key := range keys {
		if key == nil {
			return nil, fmt.Errorf("signing key at index %d is nil", i)
		}
	}

	// Generate the fingerprint
	fingerprint, err := GenerateFingerprint(storePath, narHash, narSize, references)
	if err != nil {
		return nil, err
	}

	// Sign with each key (safe now - all keys validated as non-nil)
	signatures := make([]string, len(keys))
	for i, key := range keys {
		signatures[i] = key.Sign(fingerprint)
	}

	return signatures, nil
}
