package signing_test

import (
	"strings"
	"testing"

	"github.com/Mic92/niks3/server/signing"
)

func TestGenerateFingerprint(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name        string
		storePath   string
		narHash     string
		narSize     uint64
		references  []string
		expected    string
		shouldError bool
		errorMsg    string
	}{
		{
			name:      "basic with references",
			storePath: "/nix/store/syd87l2rxw8cbsxmxl853h0r6pdwhwjr-curl-7.82.0-bin",
			narHash:   "sha256:1b4sb93wp679q4zx9k1ignby1yna3z7c4c2ri3wphylbc2dwsys0",
			narSize:   196040,
			references: []string{
				"/nix/store/0jqd0rlxzra1rs38rdxl43yh6rxchgc6-curl-7.82.0",
				"/nix/store/5dq2jj6d7k197p6fzqn8l5n0jfmhxmcg-glibc-2.33-59",
			},
			expected:    "1;/nix/store/syd87l2rxw8cbsxmxl853h0r6pdwhwjr-curl-7.82.0-bin;sha256:1b4sb93wp679q4zx9k1ignby1yna3z7c4c2ri3wphylbc2dwsys0;196040;/nix/store/0jqd0rlxzra1rs38rdxl43yh6rxchgc6-curl-7.82.0,/nix/store/5dq2jj6d7k197p6fzqn8l5n0jfmhxmcg-glibc-2.33-59",
			shouldError: false,
		},
		{
			name:        "no references",
			storePath:   "/nix/store/26xbg1ndr7hbcncrlf9nhx5is2b25d13-hello-2.12.1",
			narHash:     "sha256:1mkvday29m2qxg1fnbv8xh9s6151bh8a2xzhh0k86j7lqhyfwibh",
			narSize:     226560,
			references:  []string{},
			expected:    "1;/nix/store/26xbg1ndr7hbcncrlf9nhx5is2b25d13-hello-2.12.1;sha256:1mkvday29m2qxg1fnbv8xh9s6151bh8a2xzhh0k86j7lqhyfwibh;226560;",
			shouldError: false,
		},
		{
			name:      "unsorted references get sorted",
			storePath: "/nix/store/test",
			narHash:   "sha256:1mkvday29m2qxg1fnbv8xh9s6151bh8a2xzhh0k86j7lqhyfwibh",
			narSize:   100,
			references: []string{
				"/nix/store/zzz-package",
				"/nix/store/aaa-package",
				"/nix/store/mmm-package",
			},
			expected:    "1;/nix/store/test;sha256:1mkvday29m2qxg1fnbv8xh9s6151bh8a2xzhh0k86j7lqhyfwibh;100;/nix/store/aaa-package,/nix/store/mmm-package,/nix/store/zzz-package",
			shouldError: false,
		},
		{
			name:        "invalid nar hash prefix",
			storePath:   "/nix/store/test",
			narHash:     "sha512:abc",
			narSize:     100,
			references:  []string{},
			shouldError: true,
			errorMsg:    "must start with 'sha256:'",
		},
		{
			name:        "invalid nar hash length",
			storePath:   "/nix/store/test",
			narHash:     "sha256:tooshort",
			narSize:     100,
			references:  []string{},
			shouldError: true,
			errorMsg:    "invalid length",
		},
		{
			name:        "invalid store path prefix",
			storePath:   "/usr/local/test",
			narHash:     "sha256:1mkvday29m2qxg1fnbv8xh9s6151bh8a2xzhh0k86j7lqhyfwibh",
			narSize:     100,
			references:  []string{},
			shouldError: true,
			errorMsg:    "does not start with /nix/store",
		},
		{
			name:        "invalid reference prefix",
			storePath:   "/nix/store/test",
			narHash:     "sha256:1mkvday29m2qxg1fnbv8xh9s6151bh8a2xzhh0k86j7lqhyfwibh",
			narSize:     100,
			references:  []string{"/usr/local/invalid"},
			shouldError: true,
			errorMsg:    "reference path does not start with /nix/store",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			fingerprint, err := signing.GenerateFingerprint(tt.storePath, tt.narHash, tt.narSize, tt.references)

			if tt.shouldError {
				if err == nil {
					t.Fatalf("Expected error containing '%s', got nil", tt.errorMsg)
				}

				if !strings.Contains(err.Error(), tt.errorMsg) {
					t.Errorf("Expected error containing '%s', got: %v", tt.errorMsg, err)
				}

				return
			}

			if err != nil {
				t.Fatalf("GenerateFingerprint failed: %v", err)
			}

			if string(fingerprint) != tt.expected {
				t.Errorf("Fingerprint mismatch.\nGot:      %s\nExpected: %s", string(fingerprint), tt.expected)
			}
		})
	}
}
