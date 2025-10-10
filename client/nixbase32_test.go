package client_test

import (
	"crypto/sha256"
	"encoding/hex"
	"testing"

	"github.com/Mic92/niks3/client"
)

func TestEncodeNixBase32(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		input    string // hex-encoded input
		expected string
	}{
		{
			name:     "test string hash",
			input:    "9f86d081884c7d659a2feaa0c55ad015a3bf4f1b2b0b822cd15d6c15b0f00a08",
			expected: "020ay2q1av2xs4n842rb3d7vz8qms1dcb87a5yd6azaci20x11lz",
		},
		{
			name:     "empty input",
			input:    "",
			expected: "",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			var input []byte
			if tt.input != "" {
				var err error
				input, err = hex.DecodeString(tt.input)
				if err != nil {
					t.Fatalf("Failed to decode hex input: %v", err)
				}
			}

			result := client.EncodeNixBase32(input)
			if result != tt.expected {
				t.Errorf("client.EncodeNixBase32() = %q, want %q", result, tt.expected)
			}
		})
	}
}

func TestEncodeNixBase32WithRealHash(t *testing.T) {
	t.Parallel()

	// Test with actual SHA256 hash of "test"
	input := []byte("test")
	hash := sha256.Sum256(input)

	result := client.EncodeNixBase32(hash[:])
	expected := "020ay2q1av2xs4n842rb3d7vz8qms1dcb87a5yd6azaci20x11lz"

	if result != expected {
		t.Errorf("client.EncodeNixBase32(sha256(test)) = %q, want %q", result, expected)
	}
}

func TestConvertHashToNix32(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		input    string
		expected string
		wantErr  bool
	}{
		{
			name:     "SRI format to Nix32",
			input:    "sha256-n4bQgYhMfWWaL+qgxVrQFaO/TxsrC4Is0V1sFbDwCgg=",
			expected: "sha256:020ay2q1av2xs4n842rb3d7vz8qms1dcb87a5yd6azaci20x11lz",
			wantErr:  false,
		},
		{
			name:     "already Nix32 format",
			input:    "sha256:020ay2q1av2xs4n842rb3d7vz8qms1dcb87a5yd6azaci20x11lz",
			expected: "sha256:020ay2q1av2xs4n842rb3d7vz8qms1dcb87a5yd6azaci20x11lz",
			wantErr:  false,
		},
		{
			name:     "invalid format",
			input:    "md5:abc123",
			expected: "",
			wantErr:  true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			result, err := client.ConvertHashToNix32(tt.input)
			if (err != nil) != tt.wantErr {
				t.Errorf("ConvertHashToNix32() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if result != tt.expected {
				t.Errorf("client.ConvertHashToNix32() = %q, want %q", result, tt.expected)
			}
		})
	}
}
