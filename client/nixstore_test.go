package client_test

import (
	"testing"

	"github.com/Mic92/niks3/client"
)

func TestGetStorePathHash(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		storePath string
		wantHash  string
		wantErr   bool
	}{
		{
			name:      "valid store path",
			storePath: "/nix/store/8ha1dhmx807czjczmwy078s4r9s254il-hello-2.12.2",
			wantHash:  "8ha1dhmx807czjczmwy078s4r9s254il",
			wantErr:   false,
		},
		{
			name:      "basename without hyphen should error",
			storePath: "/nix/store/badhash",
			wantHash:  "",
			wantErr:   true,
		},
		{
			name:      "hash with invalid characters should error",
			storePath: "/nix/store/INVALID!!!xyz0123456789abcdfghij-package",
			wantHash:  "",
			wantErr:   true,
		},
		{
			name:      "hash with wrong length should error",
			storePath: "/nix/store/tooshort-package",
			wantHash:  "",
			wantErr:   true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			hash, err := client.GetStorePathHash(tt.storePath)
			if (err != nil) != tt.wantErr {
				t.Errorf("GetStorePathHash() error = %v, wantErr %v", err, tt.wantErr)

				return
			}

			if hash != tt.wantHash {
				t.Errorf("GetStorePathHash() = %q, want %q", hash, tt.wantHash)
			}
		})
	}
}
