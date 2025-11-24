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
		expectedString string // Expected output from Hash.String()
		wantErr        bool
	}{
		{
			name:           "old string format with dash (SRI)",
			jsonInput:      `{"narHash":"sha256-FePFYIlMuycIXPZbWi7LGEiMmZSX9FMbaQenWBzm1Sc=","narSize":1000,"references":[]}`,
			expectedString: "sha256-FePFYIlMuycIXPZbWi7LGEiMmZSX9FMbaQenWBzm1Sc=",
			wantErr:        false,
		},
		{
			name:           "old string format with colon",
			jsonInput:      `{"narHash":"sha256:FePFYIlMuycIXPZbWi7LGEiMmZSX9FMbaQenWBzm1Sc=","narSize":1000,"references":[]}`,
			expectedString: "sha256:FePFYIlMuycIXPZbWi7LGEiMmZSX9FMbaQenWBzm1Sc=",
			wantErr:        false,
		},
		{
			name:           "new structured format - converts to SRI",
			jsonInput:      `{"narHash":{"algorithm":"sha256","format":"base64","hash":"FePFYIlMuycIXPZbWi7LGEiMmZSX9FMbaQenWBzm1Sc="},"narSize":1000,"references":[]}`,
			expectedString: "sha256-FePFYIlMuycIXPZbWi7LGEiMmZSX9FMbaQenWBzm1Sc=", // Note: dash, not colon!
			wantErr:        false,
		},
		{
			name:           "new structured format with sha512",
			jsonInput:      `{"narHash":{"algorithm":"sha512","format":"base64","hash":"abcdef123456"},"narSize":1000,"references":[]}`,
			expectedString: "sha512-abcdef123456",
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

func TestPathInfoCACompatibility(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name          string
		jsonInput     string
		expectedCAStr string // Expected output from CA.String() if not nil
		expectNil     bool
		wantErr       bool
	}{
		{
			name:      "null ca field",
			jsonInput: `{"narHash":"sha256-FePFYIlMuycIXPZbWi7LGEiMmZSX9FMbaQenWBzm1Sc=","narSize":1000,"references":[],"ca":null}`,
			expectNil: true,
			wantErr:   false,
		},
		{
			name:          "old string format - text",
			jsonInput:     `{"narHash":"sha256-FePFYIlMuycIXPZbWi7LGEiMmZSX9FMbaQenWBzm1Sc=","narSize":1000,"references":[],"ca":"text:sha256:1abc2def3ghi"}`,
			expectedCAStr: "text:sha256:1abc2def3ghi",
			expectNil:     false,
			wantErr:       false,
		},
		{
			name:          "old string format - fixed recursive",
			jsonInput:     `{"narHash":"sha256-FePFYIlMuycIXPZbWi7LGEiMmZSX9FMbaQenWBzm1Sc=","narSize":1000,"references":[],"ca":"fixed:r:sha256:1abc2def"}`,
			expectedCAStr: "fixed:r:sha256:1abc2def",
			expectNil:     false,
			wantErr:       false,
		},
		{
			name:          "new structured format - text",
			jsonInput:     `{"narHash":{"algorithm":"sha256","format":"base64","hash":"FePFYIlM"},"narSize":1000,"references":[],"ca":{"method":"text","hash":{"algorithm":"sha256","format":"base64","hash":"h1JyyIYA"}}}`,
			expectedCAStr: "text:sha256-h1JyyIYA",
			expectNil:     false,
			wantErr:       false,
		},
		{
			name:          "new structured format - nar method",
			jsonInput:     `{"narHash":{"algorithm":"sha256","format":"base64","hash":"FePF"},"narSize":1000,"references":[],"ca":{"method":"nar","hash":{"algorithm":"sha256","format":"base64","hash":"abcd1234"}}}`,
			expectedCAStr: "nar:sha256-abcd1234",
			expectNil:     false,
			wantErr:       false,
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
				if tt.expectNil {
					if pathInfo.CA != nil {
						t.Errorf("Expected CA to be nil, but got: %v", pathInfo.CA)
					}
				} else {
					if pathInfo.CA == nil {
						t.Errorf("Expected CA to be non-nil")

						return
					}

					caStr := pathInfo.CA.String()
					if caStr != tt.expectedCAStr {
						t.Errorf("CA.String() = %q, want %q", caStr, tt.expectedCAStr)
					}
				}
			}
		})
	}
}
