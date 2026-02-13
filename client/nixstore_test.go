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

func TestParsePathInfoJSON(t *testing.T) {
	t.Parallel()

	nixJSON := `{
		"/nix/store/8ha1dhmx807czjczmwy078s4r9s254il-hello-2.12.2": {
			"narHash": "sha256-FePFYIlMuycIXPZbWi7LGEiMmZSX9FMbaQenWBzm1Sc=",
			"narSize": 226560,
			"references": [
				"/nix/store/3n58xw4373jp0ljirf06d8077j15pc4j-glibc-2.37-8",
				"/nix/store/8ha1dhmx807czjczmwy078s4r9s254il-hello-2.12.2"
			],
			"deriver": "/nix/store/abc-hello.drv",
			"signatures": ["cache.nixos.org-1:sig"]
		}
	}`

	lixJSON := `[
		{
			"path": "/nix/store/8ha1dhmx807czjczmwy078s4r9s254il-hello-2.12.2",
			"narHash": "sha256-FePFYIlMuycIXPZbWi7LGEiMmZSX9FMbaQenWBzm1Sc=",
			"narSize": 226560,
			"references": [
				"/nix/store/3n58xw4373jp0ljirf06d8077j15pc4j-glibc-2.37-8",
				"/nix/store/8ha1dhmx807czjczmwy078s4r9s254il-hello-2.12.2"
			],
			"deriver": "/nix/store/abc-hello.drv",
			"signatures": ["cache.nixos.org-1:sig"]
		}
	]`

	tests := []struct {
		name    string
		input   string
		wantErr bool
	}{
		{name: "Nix format", input: nixJSON, wantErr: false},
		{name: "Lix format", input: lixJSON, wantErr: false},
		{name: "empty input", input: "", wantErr: true},
		{name: "whitespace only", input: "   \n\t  ", wantErr: true},
		{name: "invalid JSON", input: "not json", wantErr: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			result, err := client.ParsePathInfoJSON([]byte(tt.input))
			if (err != nil) != tt.wantErr {
				t.Fatalf("ParsePathInfoJSON() error = %v, wantErr %v", err, tt.wantErr)
			}

			if tt.wantErr {
				return
			}

			storePath := "/nix/store/8ha1dhmx807czjczmwy078s4r9s254il-hello-2.12.2"

			info, ok := result[storePath]
			if !ok {
				t.Fatalf("expected key %q in result, got keys: %v", storePath, keys(result))
			}

			if info.Path != storePath {
				t.Errorf("Path = %q, want %q", info.Path, storePath)
			}

			if info.NarHash.String() != "sha256-FePFYIlMuycIXPZbWi7LGEiMmZSX9FMbaQenWBzm1Sc=" {
				t.Errorf("NarHash = %q, want %q", info.NarHash.String(), "sha256-FePFYIlMuycIXPZbWi7LGEiMmZSX9FMbaQenWBzm1Sc=")
			}

			if info.NarSize != 226560 {
				t.Errorf("NarSize = %d, want %d", info.NarSize, 226560)
			}

			if len(info.References) != 2 {
				t.Errorf("References count = %d, want %d", len(info.References), 2)
			}

			if info.Deriver == nil || *info.Deriver != "/nix/store/abc-hello.drv" {
				t.Errorf("Deriver = %v, want %q", info.Deriver, "/nix/store/abc-hello.drv")
			}

			if len(info.Signatures) != 1 || info.Signatures[0] != "cache.nixos.org-1:sig" {
				t.Errorf("Signatures = %v, want %v", info.Signatures, []string{"cache.nixos.org-1:sig"})
			}
		})
	}
}

func keys(m map[string]*client.PathInfo) []string {
	result := make([]string, 0, len(m))
	for k := range m {
		result = append(result, k)
	}

	return result
}

func TestParsePathInfoJSONMultiplePaths(t *testing.T) {
	t.Parallel()

	nixJSON := `{
		"/nix/store/aaaa-foo": {"narHash": "sha256-abc=", "narSize": 100, "references": []},
		"/nix/store/bbbb-bar": {"narHash": "sha256-def=", "narSize": 200, "references": []}
	}`

	lixJSON := `[
		{"path": "/nix/store/aaaa-foo", "narHash": "sha256-abc=", "narSize": 100, "references": []},
		{"path": "/nix/store/bbbb-bar", "narHash": "sha256-def=", "narSize": 200, "references": []}
	]`

	for _, tt := range []struct {
		name  string
		input string
	}{
		{name: "Nix multiple paths", input: nixJSON},
		{name: "Lix multiple paths", input: lixJSON},
	} {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			result, err := client.ParsePathInfoJSON([]byte(tt.input))
			if err != nil {
				t.Fatalf("ParsePathInfoJSON() error = %v", err)
			}

			if len(result) != 2 {
				t.Fatalf("expected 2 entries, got %d", len(result))
			}

			foo := result["/nix/store/aaaa-foo"]
			if foo == nil {
				t.Fatal("missing /nix/store/aaaa-foo")
			}

			if foo.Path != "/nix/store/aaaa-foo" {
				t.Errorf("foo.Path = %q, want %q", foo.Path, "/nix/store/aaaa-foo")
			}

			if foo.NarSize != 100 {
				t.Errorf("foo.NarSize = %d, want 100", foo.NarSize)
			}

			bar := result["/nix/store/bbbb-bar"]
			if bar == nil {
				t.Fatal("missing /nix/store/bbbb-bar")
			}

			if bar.Path != "/nix/store/bbbb-bar" {
				t.Errorf("bar.Path = %q, want %q", bar.Path, "/nix/store/bbbb-bar")
			}

			if bar.NarSize != 200 {
				t.Errorf("bar.NarSize = %d, want 200", bar.NarSize)
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
			expectedCAStr: "text:sha256:00hv474ll7",
			expectNil:     false,
			wantErr:       false,
		},
		{
			name:          "new structured format - nar method",
			jsonInput:     `{"narHash":{"algorithm":"sha256","format":"base64","hash":"FePF"},"narSize":1000,"references":[],"ca":{"method":"nar","hash":{"algorithm":"sha256","format":"base64","hash":"abcd1234"}}}`,
			expectedCAStr: "fixed:r:sha256:7qdpbivdv9",
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
