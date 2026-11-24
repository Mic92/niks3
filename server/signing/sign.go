package signing

import (
	"context"
	"fmt"
)

// Signer provides a cache's public key and signs batches of fingerprints.
type Signer interface {
	// PublicKey returns the public key in the format "name:base64-public-key" for use in nix configuration.
	PublicKey() (string, error)

	// SignFingerprints returns one "name:base64-signature" per fingerprint, keyed like fingerprints.
	SignFingerprints(ctx context.Context, fingerprints map[string][]byte) (map[string]string, error)
}

// NarInfoFingerprints returns the fingerprint of each narinfo, keyed like infos.
func NarInfoFingerprints(infos map[string]*NarInfo) (map[string][]byte, error) {
	fingerprints := make(map[string][]byte, len(infos))
	for objectKey, info := range infos {
		fingerprint, err := GenerateFingerprint(info)
		if err != nil {
			return nil, fmt.Errorf("narinfo %q: %w", objectKey, err)
		}
		fingerprints[objectKey] = fingerprint
	}

	return fingerprints, nil
}

// SignNarInfos signs the fingerprints of infos with s, keyed like infos.
func SignNarInfos(ctx context.Context, s Signer, infos map[string]*NarInfo) (map[string]string, error) {
	fingerprints, err := NarInfoFingerprints(infos)
	if err != nil {
		return nil, err
	}

	signatures, err := s.SignFingerprints(ctx, fingerprints)
	if err != nil {
		return nil, fmt.Errorf("signing narinfos: %w", err)
	}

	return signatures, nil
}
