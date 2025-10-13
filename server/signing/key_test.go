package signing_test

import (
	"strings"
	"testing"

	"github.com/Mic92/niks3/server/signing"
)

func TestParseSigningKey(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name        string
		keyStr      string
		expectedKey string
		shouldError bool
	}{
		{
			name:        "valid 32-byte key",
			keyStr:      "test-key:AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA=",
			expectedKey: "test-key",
			shouldError: false,
		},
		{
			name:        "valid 32-byte key with different name",
			keyStr:      "test-key:zFD7RJEU40VJzJvgT7h5xQwFm8FufXKH2CJPaKvh/xo=",
			expectedKey: "test-key",
			shouldError: false,
		},
		{
			name:        "no colon",
			keyStr:      "no-colon",
			shouldError: true,
		},
		{
			name:        "empty name",
			keyStr:      ":AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA=",
			shouldError: true,
		},
		{
			name:        "invalid base64",
			keyStr:      "name:invalid-base64!!!",
			shouldError: true,
		},
		{
			name:        "wrong length",
			keyStr:      "name:aGVsbG8=", // "hello" in base64 (5 bytes)
			shouldError: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			key, err := signing.ParseKey(tt.keyStr)

			if tt.shouldError {
				if err == nil {
					t.Errorf("Expected error, got nil")
				}

				return
			}

			if err != nil {
				t.Fatalf("signing.ParseKey failed: %v", err)
			}

			if key.Name != tt.expectedKey {
				t.Errorf("Expected name '%s', got '%s'", tt.expectedKey, key.Name)
			}

			// Note: we can't check private key length from outside the package
		})
	}
}

func TestSignMessage(t *testing.T) {
	t.Parallel()

	keyStr := "test-key:AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA="

	key, err := signing.ParseKey(keyStr)
	if err != nil {
		t.Fatalf("signing.ParseKey failed: %v", err)
	}

	msg := []byte("Hello, world!")
	signature := key.Sign(msg)

	// Check format
	if !strings.HasPrefix(signature, "test-key:") {
		t.Errorf("Signature should start with 'test-key:', got: %s", signature)
	}

	parts := strings.Split(signature, ":")
	if len(parts) != 2 {
		t.Errorf("Expected signature format 'name:base64', got: %s", signature)
	}

	// Verify the signature is deterministic
	signature2 := key.Sign(msg)
	if signature != signature2 {
		t.Errorf("Signature should be deterministic")
	}
}

func TestSignNarinfo(t *testing.T) {
	t.Parallel()
	// Create multiple signing keys
	// #nosec G101 -- These are test keys with dummy values, not real credentials
	key1Str := "cache.example.com-1:AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA="
	key2Str := "cache.example.com-2:BBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBB="

	key1, err := signing.ParseKey(key1Str)
	if err != nil {
		t.Fatalf("signing.ParseKey key1 failed: %v", err)
	}

	key2, err := signing.ParseKey(key2Str)
	if err != nil {
		t.Fatalf("signing.ParseKey key2 failed: %v", err)
	}

	keys := []*signing.Key{key1, key2}

	storePath := "/nix/store/26xbg1ndr7hbcncrlf9nhx5is2b25d13-hello-2.12.1"
	narHash := "sha256:1mkvday29m2qxg1fnbv8xh9s6151bh8a2xzhh0k86j7lqhyfwibh"
	narSize := uint64(226560)
	references := []string{
		"/nix/store/sl141d1g77wvhr050ah87lcyz2czdxa3-glibc-2.40-36",
	}

	signatures, err := signing.SignNarinfo(keys, storePath, narHash, narSize, references)
	if err != nil {
		t.Fatalf("signing.SignNarinfo failed: %v", err)
	}

	if len(signatures) != 2 {
		t.Errorf("Expected 2 signatures, got %d", len(signatures))
	}

	// Verify signature format
	if !strings.HasPrefix(signatures[0], "cache.example.com-1:") {
		t.Errorf("First signature should start with 'cache.example.com-1:', got: %s", signatures[0])
	}

	if !strings.HasPrefix(signatures[1], "cache.example.com-2:") {
		t.Errorf("Second signature should start with 'cache.example.com-2:', got: %s", signatures[1])
	}

	// Signatures should be deterministic
	signatures2, err := signing.SignNarinfo(keys, storePath, narHash, narSize, references)
	if err != nil {
		t.Fatalf("signing.SignNarinfo (second call) failed: %v", err)
	}

	if signatures[0] != signatures2[0] || signatures[1] != signatures2[1] {
		t.Errorf("Signatures should be deterministic")
	}
}
