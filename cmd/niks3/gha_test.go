package main

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestGhaAppendFile(t *testing.T) {
	path := filepath.Join(t.TempDir(), "env")

	if err := os.WriteFile(path, nil, 0o600); err != nil {
		t.Fatal(err)
	}

	t.Setenv("GITHUB_ENV", path)

	if err := ghaSetEnv("FOO", "bar"); err != nil {
		t.Fatalf("ghaSetEnv: %v", err)
	}

	if err := ghaSetEnv("MULTI", "line1\nline2"); err != nil {
		t.Fatalf("ghaSetEnv multiline: %v", err)
	}

	got, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}

	s := string(got)

	if !strings.Contains(s, "FOO=bar\n") {
		t.Errorf("missing single-line entry; got:\n%s", s)
	}

	// Multiline: MULTI<<EOF_xxx\nline1\nline2\nEOF_xxx\n
	if !strings.Contains(s, "MULTI<<EOF_") || !strings.Contains(s, "\nline1\nline2\nEOF_") {
		t.Errorf("bad multiline entry; got:\n%s", s)
	}
}

func TestGhaAppendFile_NoEnv(t *testing.T) {
	t.Setenv("GITHUB_ENV", "")

	// Should silently do nothing.
	if err := ghaSetEnv("FOO", "bar"); err != nil {
		t.Errorf("ghaSetEnv with no GITHUB_ENV: %v", err)
	}
}

func TestGhaGetInput(t *testing.T) {
	t.Setenv("INPUT_SERVER_URL", "  https://example.com  ")

	if got := ghaGetInput("server-url"); got != "https://example.com" {
		t.Errorf("ghaGetInput(server-url) = %q, want %q", got, "https://example.com")
	}

	if got := ghaGetInput("unset"); got != "" {
		t.Errorf("ghaGetInput(unset) = %q, want empty", got)
	}
}

func TestEscapeGHAData(t *testing.T) {
	t.Parallel()

	tests := []struct {
		in, want string
	}{
		{"plain", "plain"},
		{"100%", "100%25"},
		{"a\nb", "a%0Ab"},
		{"a\r\nb", "a%0D%0Ab"},
	}

	for _, tt := range tests {
		if got := escapeGHAData(tt.in); got != tt.want {
			t.Errorf("escapeGHAData(%q) = %q, want %q", tt.in, got, tt.want)
		}
	}
}
