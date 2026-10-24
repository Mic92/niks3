package main

import (
	"bytes"
	"testing"
)

func TestAuthHelpAndValidation(t *testing.T) {
	t.Setenv("NIKS3_SERVER_URL", "")
	for _, tc := range []struct {
		args    []string
		valid   bool
		wantErr string
	}{
		{args: []string{"--help"}, valid: true},
		{args: []string{"login", "--help"}, valid: true},
		{args: []string{"status", "--help"}, valid: true},
		{args: []string{"logout", "--help"}, valid: true},
		{args: nil},
		{args: []string{"unknown"}},
		{args: []string{"login"}, wantErr: "server URL is required (use --server-url or NIKS3_SERVER_URL env var)"},
		{args: []string{"status"}, wantErr: "server URL is required (use --server-url or NIKS3_SERVER_URL env var)"},
		{args: []string{"logout"}, wantErr: "server URL is required (use --server-url or NIKS3_SERVER_URL env var)"},
		{args: []string{"login", "--server-url", "https://cache.example.com"}, wantErr: "login requires --device, --oidc-issuer and --oidc-client-id"},
		{args: []string{"status", "unexpected"}},
		{args: []string{"logout", "--device"}},
	} {
		var out, instructions bytes.Buffer
		err := runAuth(t.Context(), tc.args, &out, &instructions)
		if (err == nil) != tc.valid {
			t.Fatalf("auth %v: %v", tc.args, err)
		}
		if tc.wantErr != "" && err.Error() != tc.wantErr {
			t.Fatalf("auth %v: got error %q, want %q", tc.args, err, tc.wantErr)
		}
		if out.Len() != 0 {
			t.Fatal("help or validation wrote to stdout")
		}
	}
}
