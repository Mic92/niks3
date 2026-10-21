package cmdutil_test

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"flag"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/Mic92/niks3/client/oidclogin"
	"github.com/Mic92/niks3/cmdutil"
)

func TestOIDCResolution(t *testing.T) {
	dir := t.TempDir()
	t.Setenv("XDG_CONFIG_HOME", dir)
	t.Setenv("NIKS3_AUTH_TOKEN_FILE", "")
	t.Setenv("DBUS_SESSION_BUS_ADDRESS", "")
	server := "https://cache.example.com"
	profileDir := filepath.Join(dir, "niks3", "oidc")
	if err := os.MkdirAll(profileDir, 0o700); err != nil {
		t.Fatal(err)
	}
	data, err := json.Marshal(oidclogin.Profile{Server: server, Config: oidclogin.Config{Issuer: "https://auth.example.com", ClientID: "cli"}})
	if err != nil {
		t.Fatal(err)
	}
	sum := sha256.Sum256([]byte(server))
	if err := os.WriteFile(filepath.Join(profileDir, hex.EncodeToString(sum[:])+".json"), data, 0o600); err != nil {
		t.Fatal(err)
	}
	tokenFile := filepath.Join(dir, "token")
	if err := os.WriteFile(tokenFile, []byte("file-token"), 0o600); err != nil {
		t.Fatal(err)
	}
	for _, tc := range []struct {
		name            string
		flags           []string
		envFile         string
		mtls            bool
		oidc            bool
		want            string
		resolutionError bool
		keyringError    bool
	}{
		{name: "saved-fallback", oidc: true, keyringError: true},
		{name: "explicit-token", oidc: true, flags: []string{"--auth-token", "literal"}, want: "literal"},
		{name: "explicit-file", oidc: true, flags: []string{"--auth-token-path", tokenFile}, want: "file-token"},
		{name: "env-file", oidc: true, envFile: tokenFile, want: "file-token"},
		{name: "mtls", oidc: true, mtls: true},
		{name: "explicit-profile", oidc: true, mtls: true, envFile: tokenFile, flags: []string{"--oidc-profile"}, keyringError: true},
		{name: "explicit-token-over-profile", oidc: true, flags: []string{"--oidc-profile", "--auth-token", "literal"}, want: "literal"},
		{name: "hook-does-not-select-profile", resolutionError: true},
		{name: "hook-env-file", envFile: tokenFile, want: "file-token"},
		{name: "device-conflict", oidc: true, flags: []string{"--oidc-device", "--auth-token", "literal"}, resolutionError: true},
		{name: "device-profile-conflict", oidc: true, flags: []string{"--oidc-device", "--oidc-profile"}, resolutionError: true},
		{name: "issuer-without-device", oidc: true, flags: []string{"--oidc-issuer", "https://auth.example.com"}, resolutionError: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Setenv("NIKS3_AUTH_TOKEN_FILE", tc.envFile)
			fs := flag.NewFlagSet("test", flag.ContinueOnError)
			cf := cmdutil.AddCommonFlags(fs)
			tf := cmdutil.AddTLSFlags(fs)
			if tc.oidc {
				cf = cf.AddOIDCFlags(fs)
			}
			args := append([]string{"--server-url", server}, tc.flags...)
			if tc.mtls {
				args = append(args, "--client-cert", "test-cert")
			}
			if err := fs.Parse(args); err != nil {
				t.Fatal(err)
			}
			source, err := cf.TokenSource(fs, tf)
			if tc.resolutionError {
				if err == nil {
					t.Fatal("expected resolution failure")
				}

				return
			}
			if err != nil {
				t.Fatal(err)
			}
			token, err := source(t.Context())
			if tc.keyringError {
				if err == nil || !strings.Contains(err.Error(), "Secret Service") {
					t.Fatalf("saved login not selected: %v", err)
				}

				return
			}
			if err != nil || token != tc.want {
				t.Fatalf("resolved token = %q, error = %v", token, err)
			}
		})
	}
}
