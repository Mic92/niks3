package signing

import (
	"crypto/ed25519"
	"encoding/base64"
	"errors"
	"fmt"
	"os"
	"strings"
)

// Key represents an Ed25519 signing key with a name.
type Key struct {
	Name string
	key  ed25519.PrivateKey // 64 bytes for Ed25519
}

// ParseKey parses a signing key from a string in the format "name:base64-key"
// The key can be either 32 bytes (secret key only) or 64 bytes (full keypair).
func ParseKey(s string) (*Key, error) {
	parts := strings.SplitN(s, ":", 2)
	if len(parts) != 2 {
		return nil, errors.New("sign key does not contain a ':'")
	}

	name := strings.TrimSpace(parts[0])
	keyBase64 := strings.TrimSpace(parts[1])

	if name == "" {
		return nil, errors.New("empty key name")
	}

	// Decode base64 key - try standard encoding first, then raw (no padding) if that fails
	keyBytes, err := base64.StdEncoding.DecodeString(keyBase64)
	if err != nil {
		// Try raw base64 without padding
		keyBytes, err = base64.RawStdEncoding.DecodeString(keyBase64)
		if err != nil {
			return nil, fmt.Errorf("failed to decode base64: %w", err)
		}
	}

	var privateKey ed25519.PrivateKey

	switch len(keyBytes) {
	case 32:
		// 32-byte secret key - need to generate the full keypair
		// In Ed25519, the private key is 64 bytes (32 bytes secret + 32 bytes public)
		// The seed is the first 32 bytes
		privateKey = ed25519.NewKeyFromSeed(keyBytes)
	case 64:
		// 64-byte keypair (32 bytes secret + 32 bytes public)
		privateKey = ed25519.PrivateKey(keyBytes)
	default:
		return nil, fmt.Errorf("invalid signing key length: expected 32 or 64 bytes, got %d", len(keyBytes))
	}

	// Validate the key by checking its length
	if len(privateKey) != ed25519.PrivateKeySize {
		return nil, fmt.Errorf("invalid Ed25519 key size: %d", len(privateKey))
	}

	return &Key{
		Name: name,
		key:  privateKey,
	}, nil
}

// LoadKeyFromFile loads a signing key from a file
// The file should contain a key in the format "name:base64-key".
func LoadKeyFromFile(path string) (*Key, error) {
	content, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("failed to read signing key file: %w", err)
	}

	return ParseKey(strings.TrimSpace(string(content)))
}

// Sign signs a message and returns the signature in the format "name:base64-signature".
func (k *Key) Sign(msg []byte) string {
	signature := ed25519.Sign(k.key, msg)
	signatureBase64 := base64.StdEncoding.EncodeToString(signature)

	return fmt.Sprintf("%s:%s", k.Name, signatureBase64)
}
