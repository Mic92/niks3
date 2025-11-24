package client_test

import (
	"encoding/json"
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

func TestPathInfoHashCompatibility(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name           string
		jsonInput      string
		expectedString string
		wantErr        bool
	}{
		{
			name:           "old string format with colon",
			jsonInput:      `{"narHash":"sha256:FePFYIlMuycIXPZbWi7LGEiMmZSX9FMbaQenWBzm1Sc=","narSize":1000,"references":[]}`,
			expectedString: "sha256:FePFYIlMuycIXPZbWi7LGEiMmZSX9FMbaQenWBzm1Sc=",
			wantErr:        false,
		},
		{
			name:           "old string format with dash (SRI)",
			jsonInput:      `{"narHash":"sha256-FePFYIlMuycIXPZbWi7LGEiMmZSX9FMbaQenWBzm1Sc=","narSize":1000,"references":[]}`,
			expectedString: "sha256-FePFYIlMuycIXPZbWi7LGEiMmZSX9FMbaQenWBzm1Sc=",
			wantErr:        false,
		},
		{
			name:           "old string format with nix32",
			jsonInput:      `{"narHash":"sha256:1abc2def3ghi4jkl5mno6pqr7stu8vwx","narSize":1000,"references":[]}`,
			expectedString: "sha256:1abc2def3ghi4jkl5mno6pqr7stu8vwx",
			wantErr:        false,
		},
		{
			name:           "new structured format",
			jsonInput:      `{"narHash":{"algorithm":"sha256","format":"base64","hash":"FePFYIlMuycIXPZbWi7LGEiMmZSX9FMbaQenWBzm1Sc="},"narSize":1000,"references":[]}`,
			expectedString: "sha256:FePFYIlMuycIXPZbWi7LGEiMmZSX9FMbaQenWBzm1Sc=",
			wantErr:        false,
		},
		{
			name:           "new structured format with nix32",
			jsonInput:      `{"narHash":{"algorithm":"sha256","format":"nix32","hash":"1abc2def3ghi4jkl5mno6pqr7stu8vwx"},"narSize":1000,"references":[]}`,
			expectedString: "sha256:1abc2def3ghi4jkl5mno6pqr7stu8vwx",
			wantErr:        false,
		},
		{
			name:           "new structured format with sha512",
			jsonInput:      `{"narHash":{"algorithm":"sha512","format":"base64","hash":"abcdef123456"},"narSize":1000,"references":[]}`,
			expectedString: "sha512:abcdef123456",
			wantErr:        false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			var pathInfo client.PathInfo
			err := json.Unmarshal([]byte(tt.jsonInput), &pathInfo)
			if (err != nil) != tt.wantErr {
				t.Errorf("json.Unmarshal() error = %v, wantErr %v", err, tt.wantErr)

				return
			}

			if !tt.wantErr {
				hashStr := pathInfo.NarHash.String()
				if hashStr != tt.expectedString {
					t.Errorf("NarHash.String() = %q, want %q", hashStr, tt.expectedString)
				}
			}
		})
	}
}
