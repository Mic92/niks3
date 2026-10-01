package signing_test

import (
	"context"
	"errors"
	"maps"
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

func TestSign(t *testing.T) {
	t.Parallel()

	keyStr := "test-key:AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA="

	key, err := signing.ParseKey(keyStr)
	if err != nil {
		t.Fatalf("signing.ParseKey failed: %v", err)
	}

	info := &signing.NarInfo{
		StorePath: "/nix/store/test",
		NarHash:   "sha256:1mkvday29m2qxg1fnbv8xh9s6151bh8a2xzhh0k86j7lqhyfwibh",
		NarSize:   100,
	}
	signatures, err := key.Sign(t.Context(), map[string]*signing.NarInfo{"test.narinfo": info})
	if err != nil {
		t.Fatalf("Sign failed: %v", err)
	}
	if len(signatures) != 1 {
		t.Fatalf("Expected 1 signature, got %d", len(signatures))
	}
	signature := signatures["test.narinfo"]

	// Check format
	if !strings.HasPrefix(signature, "test-key:") {
		t.Errorf("Signature should start with 'test-key:', got: %s", signature)
	}

	parts := strings.Split(signature, ":")
	if len(parts) != 2 {
		t.Errorf("Expected signature format 'name:base64', got: %s", signature)
	}

	// Verify the signature is deterministic
	signatures2, err := key.Sign(t.Context(), map[string]*signing.NarInfo{"test.narinfo": info})
	if err != nil {
		t.Fatalf("Sign (second call) failed: %v", err)
	}
	if !maps.Equal(signatures, signatures2) {
		t.Errorf("Signature should be deterministic")
	}
}

func TestSignBatch(t *testing.T) {
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

	keys := []signing.Signer{key1, key2}

	narInfo := &signing.NarInfo{
		StorePath: "/nix/store/26xbg1ndr7hbcncrlf9nhx5is2b25d13-hello-2.12.1",
		NarHash:   "sha256:1mkvday29m2qxg1fnbv8xh9s6151bh8a2xzhh0k86j7lqhyfwibh",
		NarSize:   uint64(226560),
		References: []string{
			"/nix/store/sl141d1g77wvhr050ah87lcyz2czdxa3-glibc-2.40-36",
		},
	}

	second := *narInfo
	second.StorePath = "/nix/store/second"
	infos := map[string]*signing.NarInfo{"first.narinfo": narInfo, "second.narinfo": &second}

	for i, key := range keys {
		signatures, err := key.Sign(t.Context(), infos)
		if err != nil {
			t.Fatalf("Sign failed: %v", err)
		}
		if len(signatures) != len(infos) {
			t.Fatalf("Expected 2 signatures, got %d", len(signatures))
		}

		prefix := []string{"cache.example.com-1:", "cache.example.com-2:"}[i]
		for objectKey, info := range infos {
			if !strings.HasPrefix(signatures[objectKey], prefix) {
				t.Errorf("Signature should start with %q, got: %s", prefix, signatures[objectKey])
			}

			single, err := key.Sign(t.Context(), map[string]*signing.NarInfo{"test.narinfo": info})
			if err != nil {
				t.Fatalf("Sign (single narinfo) failed: %v", err)
			}
			if len(single) != 1 || signatures[objectKey] != single["test.narinfo"] {
				t.Errorf("Batch signature %q does not match signing its narinfo individually", objectKey)
			}
		}

		// Signatures should be deterministic
		signatures2, err := key.Sign(t.Context(), infos)
		if err != nil {
			t.Fatalf("Sign (second call) failed: %v", err)
		}
		if !maps.Equal(signatures, signatures2) {
			t.Errorf("Signatures should be deterministic")
		}
	}
}

func TestNilKeyPublicKey(t *testing.T) {
	t.Parallel()

	var key *signing.Key
	if _, err := key.PublicKey(); err == nil {
		t.Fatal("expected nil key to return an error")
	}
}

func TestSignErrors(t *testing.T) {
	t.Parallel()

	key, err := signing.ParseKey("test-key:AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA=")
	if err != nil {
		t.Fatal(err)
	}

	for _, tc := range []struct {
		name     string
		key      *signing.Key
		info     *signing.NarInfo
		canceled bool
	}{
		{name: "nil key"},
		{name: "nil narinfo", key: key},
		{name: "invalid narinfo", key: key, info: &signing.NarInfo{}},
		{name: "canceled", key: key, canceled: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			ctx := t.Context()
			if tc.canceled {
				var cancel context.CancelFunc
				ctx, cancel = context.WithCancel(ctx)
				cancel()
			}

			signatures, err := tc.key.Sign(ctx, map[string]*signing.NarInfo{"test.narinfo": tc.info})
			if err == nil || len(signatures) != 0 {
				t.Fatalf("got %v, %v; want no signatures and an error", signatures, err)
			}
			if tc.canceled && !errors.Is(err, context.Canceled) {
				t.Errorf("got %v, want context.Canceled", err)
			}
		})
	}
}
