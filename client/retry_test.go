package client_test

import (
	"bytes"
	"context"
	"fmt"
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

// A request that never reaches the transport still hands its body over:
// when the rate limiter's wait fails, before the first attempt, the caller's
// body must be closed as Do would have, with retries on and off. A build log
// body is a file whose name is already removed, so a leak holds its disk
// space as well as the descriptor.
func TestDoWithRetry_ClosesBodyWhenNoAttemptIsMade(t *testing.T) {
	t.Parallel()

	for _, retries := range []int{0, 5} {
		t.Run(fmt.Sprintf("retries=%d", retries), func(t *testing.T) {
			t.Parallel()

			srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
				t.Error("a request reached the server although the limiter wait failed")
				w.WriteHeader(http.StatusOK)
			}))
			defer srv.Close()

			c := client.NewTestClient(srv.Client(), client.RetryConfig{MaxRetries: retries, InitialBackoff: time.Millisecond})
			c.S3RateLimiter.RecordThrottle()

			ctx, cancel := context.WithCancel(context.Background())
			cancel()

			body := &closeTrackingBody{Reader: bytes.NewReader([]byte("hello"))}

			req, err := http.NewRequestWithContext(ctx, http.MethodPut, srv.URL, body)
			if err != nil {
				t.Fatal(err)
			}

			req.ContentLength = 5
			req.GetBody = func() (io.ReadCloser, error) {
				return &closeTrackingBody{Reader: bytes.NewReader([]byte("hello"))}, nil
			}

			if resp, err := c.DoS3Request(ctx, req); err == nil {
				_ = resp.Body.Close()

				t.Fatal("DoS3Request succeeded although the limiter wait failed")
			}

			if !body.closed.Load() {
				t.Error("the caller's body was never closed")
			}
		})
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

// A server restart takes longer than a handful of attempts.
func TestRetryTimeoutOutlastsMaxRetries(t *testing.T) {
	t.Parallel()

	var attempts atomic.Int32

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		if attempts.Add(1) <= 20 {
			w.WriteHeader(http.StatusBadGateway)
		}
	}))
	defer srv.Close()

	retry := client.RetryConfig{MaxRetries: 2, InitialBackoff: time.Millisecond, MaxBackoff: time.Millisecond, Multiplier: 1}

	for _, tc := range []struct {
		timeout time.Duration
		status  int
	}{{0, http.StatusBadGateway}, {time.Minute, http.StatusOK}} {
		retry.Timeout = tc.timeout

		req, _ := http.NewRequestWithContext(t.Context(), http.MethodGet, srv.URL, nil)

		resp, err := client.NewTestClient(srv.Client(), retry).DoServerRequest(t.Context(), req)
		if err != nil {
			t.Fatal(err)
		}

		_ = resp.Body.Close()

		if resp.StatusCode != tc.status {
			t.Errorf("timeout %v: status %d, want %d", tc.timeout, resp.StatusCode, tc.status)
		}
	}
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
