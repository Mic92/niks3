package signing

import "context"

// Signer provides a cache's public key and signs narinfo batches.
type Signer interface {
	// PublicKey returns the public key in the format "name:base64-public-key" for use in nix configuration.
	PublicKey() (string, error)

	// Sign returns one "name:base64-signature" per narinfo, keyed like infos.
	Sign(ctx context.Context, infos map[string]*NarInfo) (map[string]string, error)
}
