package client_test

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
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

func TestHashEncodings(t *testing.T) {
	t.Parallel()

	const (
		sri   = "sha256-n4bQgYhMfWWaL+qgxVrQFaO/TxsrC4Is0V1sFbDwCgg="
		nix32 = "sha256:020ay2q1av2xs4n842rb3d7vz8qms1dcb87a5yd6azaci20x11lz"
		hex16 = "9f86d081884c7d659a2feaa0c55ad015a3bf4f1b2b0b822cd15d6c15b0f00a08"
	)

	inputs := []string{
		`"` + sri + `"`,
		`"` + nix32 + `"`,
		`"sha256:` + hex16 + `"`,
		`"sha256:n4bQgYhMfWWaL+qgxVrQFaO/TxsrC4Is0V1sFbDwCgg="`,
		`{"algorithm":"sha256","format":"base64","hash":"n4bQgYhMfWWaL+qgxVrQFaO/TxsrC4Is0V1sFbDwCgg="}`,
		`{"algorithm":"sha256","format":"nix32","hash":"020ay2q1av2xs4n842rb3d7vz8qms1dcb87a5yd6azaci20x11lz"}`,
		`{"algorithm":"sha256","format":"base16","hash":"` + hex16 + `"}`,
	}

	for _, in := range inputs {
		var h client.Hash
		if err := json.Unmarshal([]byte(in), &h); err != nil {
			t.Errorf("%s: %v", in, err)

			continue
		}

		if h.SRI() != sri || h.Nix32() != nix32 {
			t.Errorf("%s: got SRI=%s Nix32=%s", in, h.SRI(), h.Nix32())
		}
	}

	for _, bad := range []string{`"md5:abc123"`, `"sha256-AAAA"`, `{"algorithm":"sha256","format":"base16","hash":"zz"}`} {
		var h client.Hash
		if err := json.Unmarshal([]byte(bad), &h); err == nil {
			t.Errorf("%s: expected error", bad)
		}
	}
}
