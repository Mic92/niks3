package client_test

import (
	"bytes"
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Mic92/niks3/client"
)

// TestDoWithRetry_BodyReplayedViaGetBody verifies that request bodies are
// replayed via GetBody on retries rather than copied into a heap buffer.
// The first attempt sends the caller's body and later ones fresh ones from
// GetBody, and the transport closes every one of them: a file-backed body
// replaced before the first attempt was left open with nobody to close it.
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

	var (
		bodiesMu sync.Mutex
		bodies   []*closeTrackingBody
	)

	newBody := func() *closeTrackingBody {
		b := &closeTrackingBody{Reader: bytes.NewReader(payload)}

		bodiesMu.Lock()
		bodies = append(bodies, b)
		bodiesMu.Unlock()

		return b
	}

	req, err := http.NewRequestWithContext(context.Background(), http.MethodPut, srv.URL, newBody())
	if err != nil {
		t.Fatal(err)
	}

	req.ContentLength = int64(len(payload))
	req.GetBody = func() (io.ReadCloser, error) { return newBody(), nil }

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

	bodiesMu.Lock()
	defer bodiesMu.Unlock()

	// The caller's body and one from GetBody per retry.
	if len(bodies) != 3 {
		t.Errorf("%d request bodies made, want 3: the caller's and one per retry", len(bodies))
	}

	for i, b := range bodies {
		if !b.closed.Load() {
			t.Errorf("request body %d was never closed", i)
		}
	}
}

// closeTrackingBody is a request body that records whether it was closed.
type closeTrackingBody struct {
	*bytes.Reader

	closed atomic.Bool
}

func (b *closeTrackingBody) Close() error {
	b.closed.Store(true)

	return nil
}

// The last retryable response is returned to the caller with its body intact,
// so error messages carry the server's explanation.
func TestDoWithRetry_FinalResponseBodyReadable(t *testing.T) {
	t.Parallel()

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		http.Error(w, "slow down", http.StatusServiceUnavailable)
	}))
	defer srv.Close()

	c := client.NewTestClient(srv.Client(), client.RetryConfig{
		MaxRetries:     1,
		InitialBackoff: 1 * time.Millisecond,
		MaxBackoff:     10 * time.Millisecond,
		Multiplier:     1.0,
		Jitter:         0,
	})

	req, err := http.NewRequestWithContext(context.Background(), http.MethodGet, srv.URL, nil)
	if err != nil {
		t.Fatal(err)
	}

	resp, err := c.DoWithRetry(context.Background(), req)
	if err != nil {
		t.Fatalf("DoWithRetry failed: %v", err)
	}

	defer func() { _ = resp.Body.Close() }()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("reading final response body: %v", err)
	}

	if got := string(bytes.TrimSpace(body)); got != "slow down" {
		t.Fatalf("final response body = %q, want the server's message", got)
	}
}
