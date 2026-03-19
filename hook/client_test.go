package hook_test

import (
	"testing"

	"github.com/Mic92/niks3/hook"
)

func TestSendPathsEmpty(t *testing.T) {
	t.Parallel()

	// Empty paths should return nil immediately without connecting.
	if err := hook.SendPaths("/nonexistent/socket", nil); err != nil {
		t.Fatalf("SendPaths failed for empty paths: %v", err)
	}

	if err := hook.SendPaths("/nonexistent/socket", []string{}); err != nil {
		t.Fatalf("SendPaths failed for empty slice: %v", err)
	}
}
