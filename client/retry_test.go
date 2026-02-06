package client_test

import (
	"bytes"
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Mic92/niks3/client"
)

// TestDoWithRetry_BodyReplayedViaGetBody verifies that request bodies are
// replayed via GetBody on retries rather than copied into a heap buffer.
// This is critical for mmap'd uploads where copying defeats the purpose.
func TestDoWithRetry_BodyReplayedViaGetBody(t *testing.T) {
	t.Parallel()

	var attempts atomic.Int32

	payload := []byte("hello")

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, err := io.ReadAll(r.Body)
		if err != nil {
			t.Errorf("reading body: %v", err)
			http.Error(w, "bad", http.StatusInternalServerError)

			return
		}

		if !bytes.Equal(body, payload) {
			t.Errorf("attempt %d: unexpected body %q, want %q", attempts.Load()+1, body, payload)
			http.Error(w, "bad body", http.StatusInternalServerError)

			return
		}

		n := attempts.Add(1)
		if n < 3 {
			w.WriteHeader(http.StatusServiceUnavailable)

			return
		}

		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	c := client.NewTestClient(srv.Client(), client.RetryConfig{
		MaxRetries:     5,
		InitialBackoff: 1 * time.Millisecond,
		MaxBackoff:     10 * time.Millisecond,
		Multiplier:     1.0,
		Jitter:         0,
	})

	req, err := http.NewRequestWithContext(context.Background(), http.MethodPut, srv.URL, bytes.NewReader(payload))
	if err != nil {
		t.Fatal(err)
	}

	resp, err := c.DoWithRetry(context.Background(), req)
	if err != nil {
		t.Fatalf("DoWithRetry failed: %v", err)
	}
	defer func() {
		if err := resp.Body.Close(); err != nil {
			t.Errorf("closing response body: %v", err)
		}
	}()

	if resp.StatusCode != http.StatusOK {
		t.Fatalf("expected 200, got %d", resp.StatusCode)
	}

	if got := int(attempts.Load()); got != 3 {
		t.Fatalf("expected 3 attempts, got %d", got)
	}
}
