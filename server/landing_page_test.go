package server

import (
	"strings"
	"testing"

	"github.com/Mic92/niks3/server/signing"
)

func TestGenerateLandingPage(t *testing.T) {
	// Create a test signing key
	key, err := signing.ParseKey("test-cache:YWJjZGVmZ2hpamtsbW5vcHFyc3R1dnd4eXoxMjM0NTY=")
	if err != nil {
		t.Fatalf("Failed to parse test key: %v", err)
	}

	service := &Service{
		SigningKeys: []*signing.Key{key},
		CacheURL:    "https://cache.example.com",
	}

	html, err := service.GenerateLandingPage("https://cache.example.com")
	if err != nil {
		t.Fatalf("Failed to generate landing page: %v", err)
	}

	// Verify the HTML contains expected elements
	expectedStrings := []string{
		"<!DOCTYPE html>",
		"Nix Binary Cache",
		"https://cache.example.com",
		"test-cache:",
		"extra-substituters",
		"extra-trusted-public-keys",
		"nixConfig",
		"nix.settings",
	}

	for _, expected := range expectedStrings {
		if !strings.Contains(html, expected) {
			t.Errorf("Landing page missing expected string: %q", expected)
		}
	}

	// Verify it's valid HTML
	if !strings.HasPrefix(html, "<!DOCTYPE html>") {
		t.Error("Landing page does not start with DOCTYPE")
	}

	if !strings.Contains(html, "</html>") {
		t.Error("Landing page does not end with closing html tag")
	}
}
