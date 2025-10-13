package signing

import (
	"errors"
	"fmt"
)

// SignNarinfo generates signatures for a narinfo file using multiple signing keys
//
// Returns an array of signature strings in the format "name:base64-signature",
// one for each provided signing key.
func SignNarinfo(keys []*Key, info *NarInfo) ([]string, error) {
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
	fingerprint, err := GenerateFingerprint(info)
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
